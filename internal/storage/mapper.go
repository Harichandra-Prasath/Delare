package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Harichandra-Prasath/Delare/internal/logging"
)

type Mapper struct {
	lock     sync.RWMutex
	filePath string
	NameMap  map[string]uint16 `json:"name_map"`
	nextID   uint16
}

var GlobalMapper *Mapper

func InitialiseMapper() error {
	path := filepath.Join(DELARE_DIRECTORY, "mapper.json")
	m := &Mapper{
		filePath: path,
		NameMap:  make(map[string]uint16),
		nextID:   1,
	}

	data, err := os.ReadFile(path)
	if err == nil {
		if err := json.Unmarshal(data, &m.NameMap); err != nil {
			return fmt.Errorf("error unmarshaling the mapper: %s", err)
		}
		for _, id := range m.NameMap {
			if id >= m.nextID {
				m.nextID = id + 1
			}
		}
		logging.Logger.Info("Mappings found and initialised..")

	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("error reading the mappings: %s", err)
	}
	GlobalMapper = m
	return nil
}

func (m *Mapper) GetOrAdd(containerName string) (uint16, error) {
	m.lock.RLock()
	id, exists := m.NameMap[containerName]
	m.lock.RUnlock()
	if exists {
		return id, nil
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	if id, exists := m.NameMap[containerName]; exists {
		return id, nil
	}

	newID := m.nextID
	m.NameMap[containerName] = newID
	m.nextID++

	if err := m.flush(); err != nil {
		delete(m.NameMap, containerName)
		m.nextID--
		return 0, fmt.Errorf("flushing mapper to disk: %s", err)
	}

	return newID, nil
}

func (m *Mapper) flush() error {
	data, err := json.MarshalIndent(m.NameMap, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := m.filePath + ".tmp"

	// Write to temporary file
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, m.filePath)
}
