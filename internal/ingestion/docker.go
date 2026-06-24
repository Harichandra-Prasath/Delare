package ingestion

import (
	"context"
	"net"
	"net/http"
	"time"
)

const DOCKER_SOCK_PATH = "/var/run/docker.sock"

func getDockerClient() *http.Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return net.Dial("unix", DOCKER_SOCK_PATH)
		},
		DisableKeepAlives:     true,
		MaxConnsPerHost:       0,
		ResponseHeaderTimeout: 5 * time.Second,
	}

	client := http.Client{
		Transport: transport,
	}

	return &client
}
