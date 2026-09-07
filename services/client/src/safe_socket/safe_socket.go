
package safe_socket

import (
	"errors"
	"io"
)

func SendAll(socket io.Writer, bytes []byte) error {
	totalSent := 0
	totalToSend := len(bytes)

	for totalSent < totalToSend {
		bytesWritten, err := socket.Write(bytes[totalSent:])
		if err != nil {
			return err
		}
		if bytesWritten == 0 {
			return errors.New("0 bytes sent: socket disconnected")
		}
		totalSent += bytesWritten
	}

	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buffer := make([]byte, size)
	totalRead := 0

	for totalRead < size {
		bytesRead, err := socket.Read(buffer[totalRead:])
		if bytesRead > 0 {
			totalRead += bytesRead
		}
		if err != nil {
			return buffer[:totalRead], err
		}
	}

	return buffer, nil
}