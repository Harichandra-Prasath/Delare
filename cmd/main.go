package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Harichandra-Prasath/Delare/internal/ingestion"
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
		return nil, errors.New("delared requires at least one container")
	}

	return cfg, nil
}

func parseLogLevel(levelStr string) slog.Level {
	switch strings.ToLower(levelStr) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func initLogger(logLevel string) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(logLevel),
	})))
}

func main() {
	cfg, err := ParseConfig(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		// The logger is not configured yet, so this falls back to the default
		// slog handler which writes to stderr.
		slog.Error("failed to parse configuration", "err", err)
		os.Exit(1)
	}

	initLogger(cfg.LogLevel)

	storage.InitialiseRingBuffer()

	if err := storage.InitialiseMapper(); err != nil {
		slog.Error("failed to initialise the mapper", "err", err)
		os.Exit(1)
	}

	if err := storage.InitialiseCheckPointManager(); err != nil {
		slog.Error("failed to initialise the checkpoint manager", "err", err)
		os.Exit(1)
	}

	if err := storage.InitialiseDiskWriter(); err != nil {
		slog.Error("failed to initialise the disk writer", "err", err)
		os.Exit(1)
	}

	agent := ingestion.GetAgent()
	containers := strings.Split(cfg.Containers, ",")

	errChan := make(chan error)

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	slog.Info("delared starting", "containers", containers)

	go storage.Dispatch(errChan)
	go storage.CPManager.Start(errChan)
	go agent.StartControlPanel(context.Background(), containers, errChan)

	for {
		select {
		case err := <-errChan:
			slog.Error("fatal error", "err", err)
			os.Exit(1)
		case <-stopChan:
			slog.Info("shutdown signal received, cleaning up resources")
			// TODO: Cleanup to be written
			os.Exit(0)
		}
	}
}
