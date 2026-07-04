package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Harichandra-Prasath/Delare/internal/logging"
)

type CheckPointManger struct {
	lock        sync.RWMutex
	checkpoints map[uint16]uint64
	filepath    string
}

var CPManager *CheckPointManger

const CHECKPOINT_DURATION = 5 * time.Second

func InitialiseCheckPointManager() error {
	m := &CheckPointManger{
		checkpoints: make(map[uint16]uint64),
		filepath:    filepath.Join(DELARE_DIRECTORY, "checkpoints.json"),
	}
	data, err := os.ReadFile(m.filepath)
	if err == nil {
		err = json.Unmarshal(data, &m.checkpoints)
		if err != nil {
			return fmt.Errorf("error loading the checkpoints: %s", err)
		}
		logging.Logger.Info("checkpoints loaded")
	} else if os.IsNotExist(err) {
		logging.Logger.Warn("no checkpoins found. all logs since start will be pulled")
	} else {
		return fmt.Errorf("error loading the checkpoints: %s", err)
	}
	CPManager = m
	return nil
}

func (M *CheckPointManger) Update(containerId uint16, timestamp uint64) {
	M.lock.Lock()
	defer M.lock.Unlock()

	if last, ok := M.checkpoints[containerId]; !ok || timestamp > last {
		M.checkpoints[containerId] = timestamp
	}
}

func (M *CheckPointManger) Get(containerId uint16) uint64 {
	M.lock.RLock()
	defer M.lock.RUnlock()
	if ts, ok := M.checkpoints[containerId]; ok {
		return ts
	}
	return 0
}

func (M *CheckPointManger) Start(errChan chan error) {
	ticker := time.NewTicker(CHECKPOINT_DURATION)

	for {
		select {
		case <-ticker.C:
			err := M.flush()
			if err != nil {
				errChan <- fmt.Errorf("error in flushing the checkpoints: %s", err.Error())
			}
		}
	}
}

func (M *CheckPointManger) flush() error {
	M.lock.RLock()
	defer M.lock.RUnlock()

	data, err := json.Marshal(M.checkpoints)
	if err != nil {
		return fmt.Errorf("error marshaling the checkpoints: %s", err.Error())
	}

	tmpFile := M.filepath + ".tmp"

	err = os.WriteFile(tmpFile, data, 0o644)
	if err != nil {
		return fmt.Errorf("error writing to checkpoints: %s", err.Error())
	}

	if err = os.Rename(tmpFile, M.filepath); err != nil {
		return fmt.Errorf("error writing to checkpoints: %s", err.Error())
	}
	logging.Logger.Debug("checkpoints flushed to disk")
	return nil
}
