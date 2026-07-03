package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/Harichandra-Prasath/Delare/internal/protocol"
)

func getLastIndexOffset(file *os.File) (uint64, error) {
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}

	if stat.Size() == 0 {
		return 0, nil
	}

	if stat.Size()%16 != 0 {
		return 0, fmt.Errorf("index file alignment corruption: size is not a multiple of 16 bytes")
	}

	// Last 8 bytes will contain the last indexed offset
	_, err = file.Seek(-8, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	var buf [8]byte
	if _, err := io.ReadFull(file, buf[:]); err != nil {
		return 0, err
	}

	lastOffset := binary.BigEndian.Uint64(buf[:])
	return lastOffset, nil
}

func getLastLogTimestamp(logFile *os.File, lastIndexOffset uint64) (uint64, error) {
	if _, err := logFile.Seek(int64(lastIndexOffset), io.SeekStart); err != nil {
		return 0, err
	}

	var lastTimeStamp uint64 = 0
	headerBuf := make([]byte, protocol.HEADER_SIZE)

	for {
		_, err := io.ReadFull(logFile, headerBuf)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return 0, err
		}

		if headerBuf[0] != 0xDE || headerBuf[1] != 0xAD {
			return 0, fmt.Errorf("corruption detected during tail scan: invalid magic bytes")
		}

		frameLength := binary.BigEndian.Uint32(headerBuf[2:6])
		timestamp := binary.BigEndian.Uint64(headerBuf[6:14])

		if timestamp > lastTimeStamp {
			lastTimeStamp = timestamp
		}

		payloadLength := int64(frameLength) - 20
		if payloadLength > 0 {
			if _, err := logFile.Seek(payloadLength, io.SeekCurrent); err != nil {
				break
			}
		}
	}

	return lastTimeStamp, nil
}
