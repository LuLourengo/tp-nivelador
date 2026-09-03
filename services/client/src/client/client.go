package client

import (
	"net"
	"time"

	"bufio"
	"os"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 100

const ECHO_CLIENT_BUFFER_SIZE = 512


const ECHO_CLIENT_MESSAGE_AMOUNT = 3
const ECHO_CLIENT_MESSAGE_DELAY_MS = 1000

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile string
	OutputFile string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
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


func (client *Client) Run() error {
	defer client.conn.Close()

	inFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "file", client.config.InputFile, "err", err)
		return err
	}
	defer inFile.Close()

	outFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("create-output-file", logger.Fail, "file", client.config.OutputFile, "err", err)
		return err
	}
	defer outFile.Close()

	outWriter := bufio.NewWriter(outFile)
	defer outWriter.Flush()

	scanner := bufio.NewScanner(inFile)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		if err := safe_socket.SendAll(client.conn, []byte(line)); err != nil {
			logger.Error("send-bet", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
			return err
		}

		responseBuffer, err := safe_socket.RecvAll(client.conn, ECHO_CLIENT_BUFFER_SIZE)
		if err != nil {
			logger.Error("recv-response", logger.Fail, "agency-id", client.config.AgencyId, "err", err)
			return err
		}

		if _, err := outWriter.Write(responseBuffer); err != nil {
			logger.Error("write-output-file", logger.Fail, "err", err)
			return err
		}
		if _, err := outWriter.WriteString("\r\n"); err != nil {
		    logger.Error("write-output-file", logger.Fail, "err", err)
		    return err
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("read-input-file", logger.Fail, "err", err)
		return err
	}

	logger.Info("process-file", logger.Success, "agency-id", client.config.AgencyId)
	return nil
}
