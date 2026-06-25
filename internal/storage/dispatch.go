package storage

import (
	"fmt"
	"time"

	"github.com/Harichandra-Prasath/Delare/internal/arena"
	"github.com/Harichandra-Prasath/Delare/internal/logging"
)

const (
	BUFFER_SIZE    = 64 * 1024
	FLUSH_DURATION = 1000 // In Milliseconds
)

func Dispatch(errChan chan error) {
	batchBuffer := make([]byte, 0, BUFFER_SIZE)
	ticker := time.NewTicker(FLUSH_DURATION * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			if len(batchBuffer) > 0 {
				logging.Logger.Debug("reached flush duration. flushing current buffer")
				if err := GlobalLSWriter.flushtoDisk(batchBuffer); err != nil {
					errChan <- fmt.Errorf("flushing to disk: %s", err.Error())
					return
				}
				batchBuffer = batchBuffer[:0]
			}
		default:
			if payload := GlobalRingBuffer.Pop(); payload != nil {
				batchBuffer = append(batchBuffer, *payload...)
				arena.BufferPool.Put(payload)

				if len(batchBuffer) >= BUFFER_SIZE {
					logging.Logger.Debug("reached max buffer size. flushing current buffer")
					if err := GlobalLSWriter.flushtoDisk(batchBuffer); err != nil {
						errChan <- fmt.Errorf("flushing to disk: %s", err.Error())
						return
					}
					batchBuffer = batchBuffer[:0]
					ticker.Reset(FLUSH_DURATION * time.Millisecond)
				}
			}
		}
	}
}
