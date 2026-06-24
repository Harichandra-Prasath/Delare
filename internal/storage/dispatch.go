package storage

import (
	"fmt"
	"time"

	"github.com/Harichandra-Prasath/Delare/internal/arena"
)

const (
	BUFFER_SIZE    = 64 * 1024
	FLUSH_DURATION = 500 // In Milliseconds
)

func Dispatch() error {
	batchBuffer := make([]byte, 0, BUFFER_SIZE)
	ticker := time.NewTicker(FLUSH_DURATION * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			if len(batchBuffer) > 0 {
				if err := GlobalLSWriter.flushtoDisk(&batchBuffer); err != nil {
					return fmt.Errorf("flushing to disk: %s", err.Error())
				}
				batchBuffer = batchBuffer[:0]
			}
		default:
			if payload := GlobalRingBuffer.Pop(); payload != nil {
				batchBuffer = append(batchBuffer, *payload...)
				arena.BufferPool.Put(payload)

				if len(batchBuffer) >= BUFFER_SIZE {
					if err := GlobalLSWriter.flushtoDisk(&batchBuffer); err != nil {
						return fmt.Errorf("flushing to disk: %s", err.Error())
					}
					batchBuffer = batchBuffer[:0]
					ticker.Reset(FLUSH_DURATION * time.Millisecond)
				}
			}
		}
	}
}
