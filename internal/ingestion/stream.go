package ingestion

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/Harichandra-Prasath/Delare/internal/arena"
	"github.com/Harichandra-Prasath/Delare/internal/protocol"
	"github.com/Harichandra-Prasath/Delare/internal/storage"
)

func StreamLogs(ctx context.Context, client *http.Client, name string, containerId uint16, checkpoint string, lastTimestamp uint64) {
	url := fmt.Sprintf("http://localhost/v1.45/containers/%s/logs?stdout=true&stderr=true&follow=true&timestamps=true&since=%s", name, checkpoint)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		slog.Error("error building the log stream request", "err", err, "container", name)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		slog.Error("error in the http streaming call", "err", err, "container", name)
		slog.Info("closing streaming of logs", "container", name)
		return
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		slog.Info("container found", "container", name)
	case http.StatusNotFound:
		slog.Warn("container not found", "container", name)
		return
	case http.StatusInternalServerError:
		slog.Warn("internal server error from docker", "container", name)
		return
	default:
		slog.Error("unexpected status code from docker", "status", resp.StatusCode, "container", name)
		return
	}

	slog.Info("starting ingestion loop", "container", name)
	slog.Info("ingesting logs after the last checkpoint", "checkpoint", checkpoint, "timestamp", lastTimestamp, "container", name)
	header := make([]byte, 8)
	for {
		if _, err := io.ReadFull(resp.Body, header); err != nil {
			if err == io.EOF {
				slog.Warn("container stream closed", "container", name)
				return
			}
			if ctx.Err() != nil {
				return
			}
			slog.Error("error in reading header", "err", err, "container", name)
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
			slog.Error("error in reading payload", "err", err, "container", name)
			arena.BufferPool.Put(bufPtr)
			continue
		}
		ts := string(buf[protocol.HEADER_SIZE : protocol.HEADER_SIZE+30])
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			slog.Warn("error parsing the log timestamp", "err", err, "container", name)
			arena.BufferPool.Put(bufPtr)
			continue
		}
		ut := uint64(t.UnixMicro())
		if ut <= lastTimestamp {
			slog.Debug("recieved logs newer than last checkpoint but older than last timestamp", "container", name)
			arena.BufferPool.Put(bufPtr)
			continue
		}
		protocol.EncodeLog(buf, ut, containerId)
		storage.CPManager.Update(containerId, ut)
		if ok := storage.GlobalRingBuffer.Push(bufPtr); !ok {
			slog.Debug("dropping logs due to high ingestion rate. consider increasing the ring buffer slots", "container", name)
			arena.BufferPool.Put(bufPtr)
			continue
		}
	}
}
