package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/Harichandra-Prasath/Delare/internal/logging"
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
	js, _ := json.Marshal(v)

	url := fmt.Sprintf("http://localhost/v1.54/events?filters=%s", string(js))
	resp, err := A.client.Get(url)
	if err != nil {
		errChan <- fmt.Errorf("error in getting events: %s", err.Error())
		return

	}

	if resp.StatusCode != http.StatusOK {
		errChan <- fmt.Errorf("recieved an non ok status code on events. code: %d", resp.StatusCode)
		return
	}
	defer resp.Body.Close()
	logging.Logger.Info("control panel started. listening for events")
	decoder := json.NewDecoder(resp.Body)
	for {
		ev := event{}
		if err := decoder.Decode(&ev); err != nil {
		}
		attributes := ev.Actor["Attributes"].(map[string]any)
		name := attributes["name"].(string)

		switch ev.Status {
		case "start":
			logging.Logger.Info("start event recieved", "container", name)
			A.startStream(ctx, name)
		case "die":
			logging.Logger.Info("die event recieved", "container", name)
			A.removeStream(name)
		}
	}
}

func (A *Agent) startStream(parentCtx context.Context, name string) {
	A.activeLock.Lock()
	defer A.activeLock.Unlock()

	ctx, cancel := context.WithCancel(parentCtx)
	A.active[name] = cancel

	go func() {
		defer cancel()
		StreamLogs(ctx, A.client, name)
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
