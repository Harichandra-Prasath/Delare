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
	"golang.org/x/sync/errgroup"
)

type config struct {
	LogLevel   string // Log level for delared
	Containers string // Comma Seperated Container Names
}

func ParseConfig(args []string) (*config, error) {
	fs := flag.NewFlagSet("delared", flag.ContinueOnError)
	cfg := &config{}

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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		slog.Error("delared terminated with error", "err", err)
		os.Exit(1)
	}
	slog.Info("delared shutdown complete")
}

func run(ctx context.Context, cfg *config) error {
	// Core components (Fail fast)
	storage.InitialiseRingBuffer()
	if err := storage.InitialiseMapper(); err != nil {
		return err
	}

	if err := storage.InitialiseCheckPointManager(); err != nil {
		return err
	}

	if err := storage.InitialiseDiskWriter(); err != nil {
		return err
	}

	g, gCtx := errgroup.WithContext(ctx)
	agent := ingestion.GetAgent()
	dispatcher := storage.GetNewDispatcher()
	containers := strings.Split(cfg.Containers, ",")

	slog.Info("delared starting", "containers", containers)
	g.Go(func() error {
		return dispatcher.Dispatch(gCtx)
	})

	g.Go(func() error {
		return storage.CPManager.Start(gCtx)
	})

	g.Go(func() error {
		return agent.RunControlPanel(gCtx, containers)
	})

	gErr := g.Wait()
	slog.Info("shutting down, trying to cleanup...")

	agent.Close()
	CErr := storage.CPManager.Close()
	DErr := dispatcher.Close()

	storage.GlobalDiskWriter.Close()

	return errors.Join(gErr, CErr, DErr)
}
