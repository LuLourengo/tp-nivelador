package safe_socket

import (
	"io"
)

func SendAll(writer io.Writer, data []byte) error {
	bytesSent := 0
	totalDataLength := len(data)

	for bytesSent < totalDataLength {
		chunkToSend := data[bytesSent:]
		
		writtenCount, err := writer.Write(chunkToSend)
		
		if err != nil {
			return err
		}

		bytesSent = bytesSent + writtenCount
	}

	return nil
}

func RecvAll(reader io.Reader, amountToRead int) ([]byte, error) {
	if amountToRead == 0 {
		return []byte{}, nil
	}

	buffer := make([]byte, amountToRead)
	bytesReadSoFar := 0

	for bytesReadSoFar < amountToRead {
		sliceToReadInto := buffer[bytesReadSoFar:]
		
		readCount, err := reader.Read(sliceToReadInto)

		if readCount > 0 {
			bytesReadSoFar = bytesReadSoFar + readCount
		}

		if err != nil {
			
			if err == io.EOF {
				if bytesReadSoFar == amountToRead {
					return buffer, nil
				}
			}
			
			return nil, err
		}
	}
	return buffer, nil
}