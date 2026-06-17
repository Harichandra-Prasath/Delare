package logging

import (
	"log/slog"
	"os"
	"strings"
)

var Logger *slog.Logger

func getLogLevel(levelStr string) slog.Level {
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


func InitialiseLogger(logLevel string) {
	Logger = slog.New(slog.NewJSONHandler(os.Stdout,&slog.HandlerOptions{
	Level: getLogLevel(logLevel),
	}))
}
