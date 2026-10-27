package storage

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

const (
	MAX_SIZE = 100 * 1024 * 1024 // 100MB
)

var DELARE_DIRECTORY = fmt.Sprintf("%s/.delared/", os.Getenv("HOME"))

type DiskWriter struct {
	segmentFile             *os.File
	indexFile               *os.File
	segmentBytesWritten     uint64
	lastIndexOffset         uint64
	LastContainerTimestamps map[uint16]uint64
	maxSegmentSize          uint64
	rotate                  bool
}

var GlobalDiskWriter *DiskWriter

func InitialiseDiskWriter() error {
	dir, err := os.Open(DELARE_DIRECTORY)
	if err != nil {
		return fmt.Errorf("error opening delared directory: %w", err)
	}
	defer dir.Close()

	dw := &DiskWriter{maxSegmentSize: MAX_SIZE, LastContainerTimestamps: make(map[uint16]uint64)}

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return fmt.Errorf("error reading file names: %w", err)
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
		dw.rotate = true // create a new file on first write
	} else {

		// SegmentFile Init
		segmentfp := filepath.Join(DELARE_DIRECTORY, latest)
		segmentFile, err := os.OpenFile(segmentfp, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return fmt.Errorf("error opening the recent segment file: %w", err)
		}
		dw.segmentFile = segmentFile
		stat, err := segmentFile.Stat()
		if err != nil {
			segmentFile.Close()
			return fmt.Errorf("error on checking stat file: %w", err)
		}
		dw.segmentBytesWritten = uint64(stat.Size())

		// IndexFile Init
		indexfp := strings.Replace(segmentfp, "log", "index", 1)
		indexFile, err := os.OpenFile(indexfp, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return fmt.Errorf("error opening the recent index file: %w", err)
		}
		dw.indexFile = indexFile
		dw.lastIndexOffset, err = getLastIndexOffset(indexFile)
		if err != nil {
			return fmt.Errorf("error getting the last index offset: %w", err)
		}
		dw.LastContainerTimestamps, err = getLastLogTimestamp(segmentFile, dw.lastIndexOffset)
		if err != nil {
			return fmt.Errorf("error getting the last log timestamp: %w", err)
		}
	}

	GlobalDiskWriter = dw

	return nil
}

func (D *DiskWriter) flushtoDisk(segmentData []byte, indexData []byte) error {
	if D.rotate {
		ts := binary.BigEndian.Uint64(segmentData[6:14])
		segmentFp := filepath.Join(DELARE_DIRECTORY, fmt.Sprintf("%d.log", ts))
		indexFp := filepath.Join(DELARE_DIRECTORY, fmt.Sprintf("%d.index", ts))

		segmentFile, err := os.OpenFile(segmentFp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("error rotating new file: %w", err)
		}
		indexFile, err := os.OpenFile(indexFp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("error rotating new file: %w", err)
		}

		if D.segmentFile != nil {
			D.segmentFile.Close()
		}
		if D.indexFile != nil {
			D.indexFile.Close()
		}

		D.segmentFile = segmentFile
		D.indexFile = indexFile

		slog.Info("log segment writer with new segment", "file", segmentFp)
		D.rotate = false
		D.segmentBytesWritten = 0
	}

	n, err := D.segmentFile.Write(segmentData)
	if err != nil {
		return fmt.Errorf("writing segment to disk: %w", err)
	}
	D.segmentBytesWritten += uint64(n)

	_, err = D.indexFile.Write(indexData)
	if err != nil {
		return fmt.Errorf("writing index to disk: %w", err)
	}
	if D.segmentBytesWritten >= D.maxSegmentSize {
		slog.Info("current segment exceeded the max size and will be rotated", "current", D.segmentBytesWritten, "max", D.maxSegmentSize)
		D.rotate = true
	}
	slog.Info("new chunk written to the segment")
	return nil
}

func (D *DiskWriter) Close() {
	D.indexFile.Close()
	D.segmentFile.Close()
}
