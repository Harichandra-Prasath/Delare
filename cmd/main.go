package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Harichandra-Prasath/Delare/internal/ingestion"
	"github.com/Harichandra-Prasath/Delare/internal/logging"
	"github.com/Harichandra-Prasath/Delare/internal/storage"
)

type Config struct {
	LogLevel   string // Log level for delared
	Containers string // Comma Seperated Container Names
}

func ParseConfig(args []string) (*Config, error) {
	fs := flag.NewFlagSet("delared", flag.ContinueOnError)
	cfg := &Config{}

	fs.StringVar(&cfg.Containers, "containers", "", "Comma Seperated list of containers")
	fs.StringVar(&cfg.LogLevel, "log-level", "info", "Log Level for Delare")

	err := fs.Parse(args)
	if err != nil {
		return nil, err
	}

	if cfg.Containers == "" {
		return nil, fmt.Errorf("delared atleast needs one container..")
	}

	return cfg, nil
}

func main() {
	cfg, err := ParseConfig(os.Args[1:])
	if err != nil {
		panic(err)
	}

	logging.InitialiseLogger(cfg.LogLevel)
	err = storage.InitialiseLSWriter()
	if err != nil {
		panic(err)
	}

	storage.InitialiseRingBuffer()

	agent := ingestion.GetAgent()
	containers := strings.Split(cfg.Containers, ",")

	errChan := make(chan error)
	logging.Logger.Info("delared starting!!")
	go agent.StartControlPanel(context.Background(), containers, errChan)
	go storage.Dispatch(errChan)
	for {
		select {
		case err := <-errChan:
			panic(err)
		}
	}
}
