package ingestion

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/http"

	"github.com/Harichandra-Prasath/Delare/internal/arena"
	"github.com/Harichandra-Prasath/Delare/internal/logging"
	"github.com/Harichandra-Prasath/Delare/internal/storage"
)

func StreamLogs(client *http.Client, name string) {
	url := fmt.Sprintf("http://localhost/v1.45/containers/%s/logs?stdout=true&stderr=true&follow=true&timestamps=true", name)

	resp, err := client.Get(url)
	if err != nil {
		logging.Logger.Error("error in http streaming call", "container", name, "error", err.Error())
		logging.Logger.Info("closing streaming of logs", "container", name)
		return
	}

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
		bufPtr := arena.BufferPool.Get().(*[]byte)
		buf := *bufPtr

		if uint32(cap(buf)) < payloadSize {
			buf = make([]byte, payloadSize)
		} else {
			buf = buf[:payloadSize]
		}

		if _, err := io.ReadFull(resp.Body, buf); err != nil {
			logging.Logger.Error("error in reading payload", "container", name, "error", err.Error())
			continue
		}
		if ok := storage.GlobalRingBuffer.Push(&buf); !ok {
			logging.Logger.Warn("dropping logs due to high ingestion rate. consider increasing the ring buffer slots")
			continue
		}
	}
}
