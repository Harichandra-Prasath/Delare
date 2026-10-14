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
}

type event struct {
	Status string         `json:"Action"`
	Actor  map[string]any `json:"Actor"`
}

func GetAgent() *Agent {
	return &Agent{client: getDockerClient(), active: map[string]context.CancelFunc{}}
}

func (A *Agent) StartControlPanel(ctx context.Context, containers []string, errChan chan error) {
	// Initial bootup

	for _, cntr := range containers {
		A.startStream(ctx, cntr)
	}

	v := map[string][]string{"container": containers, "event": {"start", "die"}, "type": {"container"}}
	js, err := json.Marshal(v)
	if err != nil {
		errChan <- fmt.Errorf("marshaling the event filters: %s", err.Error())
		return
	}

	url := fmt.Sprintf("http://localhost/v1.54/events?filters=%s", string(js))
	resp, err := A.client.Get(url)
	if err != nil {
		errChan <- fmt.Errorf("error in getting events: %s", err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errChan <- fmt.Errorf("recieved an non ok status code on events. code: %d", resp.StatusCode)
		return
	}

	slog.Info("control panel started. listening for events")
	decoder := json.NewDecoder(resp.Body)
	for {
		ev := event{}
		if err := decoder.Decode(&ev); err != nil {
			if errors.Is(err, io.EOF) {
				slog.Info("docker event stream closed")
				return
			}
			errChan <- fmt.Errorf("decoding docker event: %s", err.Error())
			return
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

	timestamp := storage.CPManager.Get(containerId)
	// Convert to unix seconds from micro
	seconds := timestamp / 1000000
	microSeconds := timestamp % 1000000

	timeString := fmt.Sprintf("%d.%d", seconds, microSeconds)
	go func() {
		defer cancel()
		StreamLogs(ctx, A.client, name, containerId, timeString, storage.GlobalDiskWriter.LastContainerTimestamps[containerId])
	}()
}

func (a *Agent) removeStream(name string) {
	a.activeLock.Lock()
	defer a.activeLock.Unlock()

	if cancel, exists := a.active[name]; exists {
		cancel()
		delete(a.active, name)
	}
}
