package client

import (
	"bufio"
	"encoding/binary"
	"errors"
	"net"
	"os"
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
	return &Client{config: config}, nil
}

func (client *Client) Run() error {
	conn, err := connectToServer(client.config.ServerHost, client.config.ServerPort)
	if err != nil {
		logger.Error("connect-to-server", logger.Fail, "err", err)
		return err
	}
	client.conn = conn
	defer client.conn.Close()

	inFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "file", client.config.InputFile, "err", err)
		return err
	}
	defer inFile.Close()

	scanner := bufio.NewScanner(inFile)
	batch := make([]string, 0, client.config.BatchSize)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		betWithAgency := client.config.AgencyId + "," + line
		batch = append(batch, betWithAgency)

		if len(batch) >= client.config.BatchSize {
			if err := client.sendBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("read-input-file", logger.Fail, "err", err)
		return err
	}

	if len(batch) > 0 {
		if err := client.sendBatch(batch); err != nil {
			return err
		}
	}

	if err := sendMsg(client.conn, MsgEnd, nil); err != nil {
		logger.Error("send-end", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	msgType, winnersPayload, err := recvMsg(client.conn)
	if err != nil {
		logger.Error("recv-winners", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	if msgType != MsgWinners {
		err := errors.New("unexpected message type received from server")
		logger.Error("recv-winners", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	outFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("create-output-file", logger.Fail, "file", client.config.OutputFile, "err", err)
		return err
	}
	defer outFile.Close()

	if len(winnersPayload) > 0 {
		outWriter := bufio.NewWriter(outFile)
		if _, err := outWriter.Write(winnersPayload); err != nil {
			logger.Error("write-output-file", logger.Fail, "err", err)
			return err
		}
		if err := outWriter.Flush(); err != nil {
			logger.Error("flush-output-file", logger.Fail, "err", err)
			return err
		}
	}

	logger.Info("process-file", logger.Success, "agency-id", client.config.AgencyId)
	return nil
}

func (client *Client) sendBatch(batch []string) error {
	payload := []byte(strings.Join(batch, "\n"))
	if err := sendMsg(client.conn, MsgBet, payload); err != nil {
		logger.Error("send-batch", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}

	msgType, _, err := recvMsg(client.conn)
	if err != nil {
		logger.Error("recv-batch-ack", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}
	if msgType != MsgAck {
		err := errors.New("expected ack from server")
		logger.Error("recv-batch-ack", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
		return err
	}
	return nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func sendMsg(conn net.Conn, msgType byte, payload []byte) error {
	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
	header[4] = msgType

	packet := append(header, payload...)
	return safe_socket.SendAll(conn, packet)
}

func recvMsg(conn net.Conn) (byte, []byte, error) {
	header, err := safe_socket.RecvAll(conn, 5)
	if err != nil {
		return 0, nil, err
	}

	payloadLen := binary.BigEndian.Uint32(header[0:4])
	msgType := header[4]

	if payloadLen == 0 {
		return msgType, []byte{}, nil
	}

	payload, err := safe_socket.RecvAll(conn, int(payloadLen))
	if err != nil {
		return 0, nil, err
	}

	return msgType, payload, nil
}