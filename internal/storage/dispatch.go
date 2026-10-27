package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"time"

	"github.com/Harichandra-Prasath/Delare/internal/arena"
)

const (
	SEGMENT_BUFFER_SIZE = 64 * 1024
	INDEX_BUFFER_SIZE   = 16 * 1024
	INDEX_SIZE          = 4 * 1024
	FLUSH_DURATION      = 1000 // In Milliseconds
)

type Dispatcher struct {
	batchBuffer []byte
	indexBuffer []byte
	ticker      *time.Ticker
}

func GetNewDispatcher() *Dispatcher {
	return &Dispatcher{
		batchBuffer: make([]byte, 0, SEGMENT_BUFFER_SIZE),
		indexBuffer: make([]byte, 0, INDEX_BUFFER_SIZE),
		ticker:      time.NewTicker(FLUSH_DURATION * time.Millisecond),
	}
}

func (D *Dispatcher) Dispatch(gCtx context.Context) error {
	D.ticker.Reset(FLUSH_DURATION * time.Millisecond)
	for {
		select {
		case <-gCtx.Done():
			slog.Info("Dispatch Exit Recieved.")
			return nil
		case <-D.ticker.C:
			if len(D.batchBuffer) > 0 {
				slog.Debug("reached flush duration. flushing current buffer")
				if err := GlobalDiskWriter.flushtoDisk(D.batchBuffer, D.indexBuffer); err != nil {
					return fmt.Errorf("flushing to disk: %w", err)
				}
				D.batchBuffer = D.batchBuffer[:0]
				D.indexBuffer = D.indexBuffer[:0]
			}
		default:
			if payload := GlobalRingBuffer.Pop(); payload != nil {
				D.populate(payload)
				if len(D.batchBuffer) >= SEGMENT_BUFFER_SIZE {
					slog.Debug("reached max buffer size. flushing current buffer")
					if err := GlobalDiskWriter.flushtoDisk(D.batchBuffer, D.indexBuffer); err != nil {
						return fmt.Errorf("flushing to disk: %w", err)
					}
					D.batchBuffer = D.batchBuffer[:0]
					D.indexBuffer = D.indexBuffer[:0]

					D.ticker.Reset(FLUSH_DURATION * time.Millisecond)
				}
			}
		}
	}
}

func (D *Dispatcher) populate(payload *[]byte) {
	virtualOffset := GlobalDiskWriter.segmentBytesWritten + uint64(len(D.batchBuffer))
	if virtualOffset-GlobalDiskWriter.lastIndexOffset >= INDEX_SIZE {
		timestamp := binary.BigEndian.Uint64((*payload)[6:14])

		var indexEntry [16]byte
		binary.BigEndian.PutUint64(indexEntry[0:8], timestamp)
		binary.BigEndian.PutUint64(indexEntry[8:16], virtualOffset)

		D.indexBuffer = append(D.indexBuffer, indexEntry[:]...)

		GlobalDiskWriter.lastIndexOffset = virtualOffset
	}

	D.batchBuffer = append(D.batchBuffer, *payload...)
	arena.BufferPool.Put(payload)
}

func (D *Dispatcher) Close() error {
	defer D.ticker.Stop()

	for {
		if payload := GlobalRingBuffer.Pop(); payload != nil {
			D.populate(payload)
			continue
		}
		break
	}

	if len(D.batchBuffer) > 0 {
		if err := GlobalDiskWriter.flushtoDisk(D.batchBuffer, D.indexBuffer); err != nil {
			return fmt.Errorf("flushing to disk: %w", err)
		}

		D.batchBuffer = D.batchBuffer[:0]
		D.indexBuffer = D.indexBuffer[:0]

	}
	return nil
}
