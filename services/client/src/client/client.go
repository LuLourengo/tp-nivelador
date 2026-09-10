package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const (
	MsgBet     byte = 1
	MsgEnd     byte = 2
	MsgWinners byte = 3
	MsgAck     byte = 4
)

const CONNECTION_ATTEMPTS_MAX = 60
const CONNECTION_ATTEMPS_DELAY_MS = 500

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	if config.BatchSize <= 0 {
		config.BatchSize = 10
	}
	
	return &Client{
		config: config,
	}, nil
}

func (client *Client) Run(ctx context.Context) error {
	connectionSocket, err := connectToServer(ctx, client.config.ServerHost, client.config.ServerPort)
	if err != nil {
		logger.Error("connect-to-server", logger.Fail, "err", err)
		return err
	}
	
	client.conn = connectionSocket
	defer client.conn.Close()

	stopWatchChannel := make(chan struct{})
	defer close(stopWatchChannel)
	
	go func() {
		select {
		case <-ctx.Done():
			_ = client.conn.Close()
		case <-stopWatchChannel:
		}
	}()

	inputFileHandle, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "file", client.config.InputFile, "err", err)
		return err
	}
	defer inputFileHandle.Close()

	fileScanner := bufio.NewScanner(inputFileHandle)
	var batchBuffer bytes.Buffer
	itemsInCurrentBatch := 0

	for fileScanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		currentLineText := strings.TrimSpace(fileScanner.Text())
		if currentLineText == "" {
			continue
		}

		if itemsInCurrentBatch > 0 {
			batchBuffer.WriteByte('\n')
		}
		
		batchBuffer.WriteString(client.config.AgencyId)
		batchBuffer.WriteByte(',')
		batchBuffer.WriteString(currentLineText)
		
		itemsInCurrentBatch++

		if itemsInCurrentBatch >= client.config.BatchSize {
			batchPayloadBytes := batchBuffer.Bytes()
			
			if err := client.sendBatchPayload(batchPayloadBytes); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return err
			}
			
			batchBuffer.Reset()
			itemsInCurrentBatch = 0
		}
	}

	if scannerError := fileScanner.Err(); scannerError != nil {
		logger.Error("read-input-file", logger.Fail, "err", scannerError)
		return scannerError
	}

	if itemsInCurrentBatch > 0 {
		remainingPayloadBytes := batchBuffer.Bytes()
		
		if err := client.sendBatchPayload(remainingPayloadBytes); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		
		batchBuffer.Reset()
	}

	if err := sendMsg(client.conn, MsgEnd, nil); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		logger.Error("send-end", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	headerBuffer, err := safe_socket.RecvAll(client.conn, 5)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		logger.Error("recv-winners-header", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	payloadLength := binary.BigEndian.Uint32(headerBuffer[0:4])
	incomingMessageType := headerBuffer[4]

	if incomingMessageType != MsgWinners {
		unexpectedMessageError := errors.New("unexpected message type received from server")
		logger.Error("recv-winners", logger.Fail, "agency-id", client.config.AgencyId, "err", unexpectedMessageError)
		return unexpectedMessageError
	}

	outputFileHandle, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("create-output-file", logger.Fail, "file", client.config.OutputFile, "err", err)
		return err
	}
	defer outputFileHandle.Close()

	if payloadLength > 0 {
		const chunkLimitSize = 8192
		bytesRemainingToRead := int(payloadLength)
		
		for bytesRemainingToRead > 0 {
			bytesToReadNow := chunkLimitSize
			if bytesRemainingToRead < bytesToReadNow {
				bytesToReadNow = bytesRemainingToRead
			}

			chunkBytes, err := safe_socket.RecvAll(client.conn, bytesToReadNow)
			if err != nil {
				logger.Error("stream-winners-chunk", logger.Fail, "err", err)
				return err
			}

			if _, err := outputFileHandle.Write(chunkBytes); err != nil {
				logger.Error("write-winners-chunk", logger.Fail, "err", err)
				return err
			}

			bytesRemainingToRead = bytesRemainingToRead - len(chunkBytes)
		}
	}

	debug.FreeOSMemory()

	logger.Info("process-file", logger.Success, "agency-id", client.config.AgencyId)
	return nil
}

func (client *Client) sendBatchPayload(payloadBytes []byte) error {
	if err := sendMsg(client.conn, MsgBet, payloadBytes); err != nil {
		logger.Error("send-batch", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	responseMessageType, _, err := recvMsg(client.conn)
	if err != nil {
		logger.Error("recv-batch-ack", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}
	
	if responseMessageType != MsgAck {
		ackError := errors.New("expected ack from server")
		logger.Error("recv-batch-ack", logger.Fail, "agency-id", client.config.AgencyId, "err", ackError)
		return ackError
	}
	
	return nil
}

func connectToServer(ctx context.Context, host string, port string) (net.Conn, error) {
	const actionDescription = "connect-to-server"
	var connectionError error
	var establishedConnection net.Conn
	var networkDialer net.Dialer

	logger.Info(actionDescription, logger.InProgress)
	
	for attemptIndex := range CONNECTION_ATTEMPTS_MAX {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		targetAddress := host + ":" + port
		establishedConnection, connectionError = networkDialer.DialContext(ctx, "tcp", targetAddress)
		
		if connectionError != nil {
			logger.Warn(actionDescription, logger.Fail, "attempt", attemptIndex)
			
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond):
			}
			continue
			
		}

		logger.Info(actionDescription, logger.Success)
		break
	}

	return establishedConnection, connectionError
}

func sendMsg(conn net.Conn, msgType byte, payload []byte) error {
	headerBuffer := make([]byte, 5)
	binary.BigEndian.PutUint32(headerBuffer[0:4], uint32(len(payload)))
	headerBuffer[4] = msgType

	completePacket := append(headerBuffer, payload...)
	return safe_socket.SendAll(conn, completePacket)
}

func recvMsg(conn net.Conn) (byte, []byte, error) {
	headerBuffer, err := safe_socket.RecvAll(conn, 5)
	if err != nil {
		return 0, nil, err
	}

	payloadLength := binary.BigEndian.Uint32(headerBuffer[0:4])
	messageType := headerBuffer[4]

	if payloadLength == 0 {
		return messageType, []byte{}, nil
	}

	payloadBytes, err := safe_socket.RecvAll(conn, int(payloadLength))
	if err != nil {
		return 0, nil, err
	}

	return messageType, payloadBytes, nil
}