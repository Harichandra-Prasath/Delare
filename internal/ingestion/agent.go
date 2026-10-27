package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"

	"github.com/Harichandra-Prasath/Delare/internal/storage"
)

type Agent struct {
	client     *http.Client
	active     map[string]context.CancelFunc
	activeLock sync.RWMutex
	waitGroup  sync.WaitGroup
}

type event struct {
	Status string         `json:"Action"`
	Actor  map[string]any `json:"Actor"`
}

func GetAgent() *Agent {
	return &Agent{client: getDockerClient(), active: map[string]context.CancelFunc{}}
}

func (A *Agent) RunControlPanel(ctx context.Context, containers []string) error {
	// Initial bootup
	for _, cntr := range containers {
		A.startStream(ctx, cntr)
	}

	v := map[string][]string{"container": containers, "event": {"start", "die"}, "type": {"container"}}
	js, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshaling the event filters: %w", err)
	}

	url := fmt.Sprintf("http://localhost/v1.54/events?filters=%s", string(js))
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("getting events: %w", err)
	}
	resp, err := A.client.Do(req)
	if err != nil {
		return fmt.Errorf("getting events: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("recieved an non ok status code on events. code: %d", resp.StatusCode)
	}

	slog.Info("control panel started. listening for events")

	decoder := json.NewDecoder(resp.Body)
	for {
		ev := event{}
		if err := decoder.Decode(&ev); err != nil {
			if ctx.Err() != nil {
				slog.Info("Control Panel Exit Recieved...")
				return nil
			}

			if errors.Is(err, io.EOF) {
				return fmt.Errorf("docker event stream closed")
			}
			return fmt.Errorf("decoding docker event: %w", err)
		}

		attributes, ok := ev.Actor["Attributes"].(map[string]any)
		if !ok {
			slog.Warn("event has no attributes, skipping", "action", ev.Status)
			continue
		}
		name, ok := attributes["name"].(string)
		if !ok {
			slog.Warn("event attributes have no container name, skipping", "action", ev.Status)
			continue
		}

		switch ev.Status {
		case "start":
			slog.Info("start event recieved", "container", name)
			A.startStream(ctx, name)
		case "die":
			slog.Info("die event recieved", "container", name)
			A.removeStream(name)
		default:
			slog.Debug("unhandled container event", "action", ev.Status, "container", name)
		}
	}
}

func (A *Agent) startStream(parentCtx context.Context, name string) {
	A.activeLock.Lock()
	defer A.activeLock.Unlock()

	ctx, cancel := context.WithCancel(parentCtx)
	A.active[name] = cancel

	containerId, err := storage.GlobalMapper.GetOrAdd(name)
	if err != nil {
		slog.Error("stream not started for container", "err", err, "container", name)
		return
	}

	A.waitGroup.Add(1)
	timestamp := storage.CPManager.Get(containerId)
	// Convert to unix seconds from micro
	seconds := timestamp / 1000000
	microSeconds := timestamp % 1000000

	timeString := fmt.Sprintf("%d.%d", seconds, microSeconds)
	go func() {
		defer cancel()
		defer A.waitGroup.Done()
		StreamLogs(ctx, A.client, name, containerId, timeString, storage.GlobalDiskWriter.LastContainerTimestamps[containerId])
	}()
}

func (A *Agent) removeStream(name string) {
	A.activeLock.Lock()
	defer A.activeLock.Unlock()

	if cancel, exists := A.active[name]; exists {
		cancel()
		delete(A.active, name)
	}
}

func (A *Agent) Close() {
	for name, cancel := range A.active {
		cancel()
		delete(A.active, name)
	}
	A.waitGroup.Wait()
}
