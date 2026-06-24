package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Harichandra-Prasath/Delare/internal/logging"
)

const (
	MAX_SIZE = 10 * 1024 * 1024 // 10MB
)

var DELARE_DIRECTORY = fmt.Sprintf("%s/.delared/", os.Getenv("HOME"))

type LogSegmentWriter struct {
	file         *os.File
	bytesWritten uint64
	maxSize      uint64
	rotate       bool // Flag to check whether files needs to be rotated before next write
}

var GlobalLSWriter *LogSegmentWriter

func InitialiseLSWriter() error {
	dir, err := os.Open(DELARE_DIRECTORY)
	if err != nil {
		return fmt.Errorf("error opening delared directory: %s", err.Error())
	}
	defer dir.Close()

	GlobalLSWriter = &LogSegmentWriter{maxSize: MAX_SIZE}

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return fmt.Errorf("error reading file names: %s", err.Error())
	}
	var latest string
	for _, name := range names {
		if strings.HasSuffix(name, ".log") {
			if name > latest {
				latest = name
			}
		}
	}
	if latest == "" {
		GlobalLSWriter.rotate = true // create a new file on first write
	} else {
		fp := filepath.Join(DELARE_DIRECTORY, latest)
		file, err := os.OpenFile(fp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("error opening the recent file: %s", err.Error())
		}
		GlobalLSWriter.file = file

		stat, err := file.Stat()
		if err != nil {
			file.Close()
			return fmt.Errorf("error on checking stat file: %s", err.Error())
		}
		GlobalLSWriter.bytesWritten = uint64(stat.Size())
	}

	return nil
}

func (L *LogSegmentWriter) flushtoDisk(data *[]byte) error {
	if L.rotate {
		data := *data
		ts := string(data[:30])
		t, _ := time.Parse(time.RFC3339Nano, ts)
		name := t.UnixMicro()
		fp := filepath.Join(DELARE_DIRECTORY, fmt.Sprintf("%020d.log", name))
		file, err := os.OpenFile(fp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("error rotating new file: %s", err.Error())
		}
		L.file = file
		logging.Logger.Info("log segment writer with new segment", "file", fp)
		L.rotate = false
		L.bytesWritten = 0
	}

	n, err := L.file.Write(*data)
	if err != nil {
		return fmt.Errorf("writing to disk: %s", err.Error())
	}
	L.bytesWritten += uint64(n)
	if L.bytesWritten >= L.maxSize {
		logging.Logger.Info("current segment exceeded the max size and will be rotated", "current", L.bytesWritten, "max", L.maxSize)
		L.rotate = true
	}
	logging.Logger.Info("new chunk written to the segment")
	return nil
}
