package ingestion

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Harichandra-Prasath/Delare/internal/arena"
	"github.com/Harichandra-Prasath/Delare/internal/logging"
	"github.com/Harichandra-Prasath/Delare/internal/protocol"
	"github.com/Harichandra-Prasath/Delare/internal/storage"
)

func StreamLogs(ctx context.Context, client *http.Client, name string) {
	url := fmt.Sprintf("http://localhost/v1.45/containers/%s/logs?stdout=true&stderr=true&follow=true&timestamps=true", name)

	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := client.Do(req)
	if err != nil {
		logging.Logger.Error("error in http streaming call", "container", name, "error", err.Error())
		logging.Logger.Info("closing streaming of logs", "container", name)
		return
	}

	if resp.StatusCode == http.StatusNotFound {
		logging.Logger.Warn("container not found", "container", name)
		return
	} else if resp.StatusCode == http.StatusInternalServerError {
		logging.Logger.Warn("internal server error from docker", "container", name)
		return
	} else {
		logging.Logger.Info("container found", "name", name)
	}

	logging.Logger.Info("starting ingestion loop", "container", name)
	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(resp.Body, header); err != nil {
			if err == io.EOF {
				logging.Logger.Warn("container stream closed", "container", name)
				return
			}
			logging.Logger.Error("error in reading header", "container", name, "error", err.Error())
			continue
		}

		// Last 4 bytes indicates the following payload size
		payloadSize := binary.BigEndian.Uint32(header[4:8])

		totalFrameSize := payloadSize + protocol.HEADER_SIZE

		bufPtr := arena.BufferPool.Get().(*[]byte)

		if uint32(cap(*bufPtr)) < totalFrameSize {
			newBuf := make([]byte, totalFrameSize)
			bufPtr = &newBuf
		}

		*bufPtr = (*bufPtr)[:totalFrameSize]
		buf := *bufPtr

		if _, err := io.ReadFull(resp.Body, buf[protocol.HEADER_SIZE:]); err != nil {
			logging.Logger.Error("error in reading payload", "container", name, "error", err.Error())
			arena.BufferPool.Put(bufPtr)
			continue
		}

		ts := string(buf[protocol.HEADER_SIZE : protocol.HEADER_SIZE+30])
		t, _ := time.Parse(time.RFC3339Nano, ts)
		ut := uint64(t.UnixMicro())
		protocol.EncodeLog(buf, ut, 1)
		if ok := storage.GlobalRingBuffer.Push(bufPtr); !ok {
			logging.Logger.Debug("dropping logs due to high ingestion rate. consider increasing the ring buffer slots")
			arena.BufferPool.Put(bufPtr)
			continue
		}
	}
}
