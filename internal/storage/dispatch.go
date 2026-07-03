package storage

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/Harichandra-Prasath/Delare/internal/arena"
	"github.com/Harichandra-Prasath/Delare/internal/logging"
)

const (
	SEGMENT_BUFFER_SIZE = 64 * 1024
	INDEX_BUFFER_SIZE   = 16 * 1024
	INDEX_SIZE          = 4 * 1024
	FLUSH_DURATION      = 1000 // In Milliseconds
)

func Dispatch(errChan chan error) {
	batchBuffer := make([]byte, 0, SEGMENT_BUFFER_SIZE)
	indexBuffer := make([]byte, 0, INDEX_BUFFER_SIZE)
	ticker := time.NewTicker(FLUSH_DURATION * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			if len(batchBuffer) > 0 {
				logging.Logger.Debug("reached flush duration. flushing current buffer")
				if err := GlobalDiskWriter.flushtoDisk(batchBuffer, indexBuffer); err != nil {
					errChan <- fmt.Errorf("flushing to disk: %s", err.Error())
					return
				}
				batchBuffer = batchBuffer[:0]
				indexBuffer = indexBuffer[:0]
			}
		default:
			if payload := GlobalRingBuffer.Pop(); payload != nil {

				virtualOffset := GlobalDiskWriter.segmentBytesWritten + uint64(len(batchBuffer))
				if virtualOffset-GlobalDiskWriter.lastIndexOffset >= INDEX_SIZE {
					timestamp := binary.BigEndian.Uint64((*payload)[6:14])

					var indexEntry [16]byte
					binary.BigEndian.PutUint64(indexEntry[0:8], timestamp)
					binary.BigEndian.PutUint64(indexEntry[8:16], virtualOffset)

					indexBuffer = append(indexBuffer, indexEntry[:]...)

					GlobalDiskWriter.lastIndexOffset = virtualOffset
				}

				batchBuffer = append(batchBuffer, *payload...)
				arena.BufferPool.Put(payload)

				if len(batchBuffer) >= SEGMENT_BUFFER_SIZE {
					logging.Logger.Debug("reached max buffer size. flushing current buffer")
					if err := GlobalDiskWriter.flushtoDisk(batchBuffer, indexBuffer); err != nil {
						errChan <- fmt.Errorf("flushing to disk: %s", err.Error())
						return
					}
					batchBuffer = batchBuffer[:0]
					indexBuffer = indexBuffer[:0]
					ticker.Reset(FLUSH_DURATION * time.Millisecond)
				}
			}
		}
	}
}
