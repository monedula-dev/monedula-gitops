package cli

import (
	"io"
	"log/slog"

	"github.com/go-logr/logr"

	"github.com/monedula-dev/monedula-gitops/internal/cluster"
)

// logger is the CLI-wide diagnostic logger (spec §31). It writes to STDERR so
// stdout stays clean for command output. Until setupLogging runs it discards
// everything, so code paths that log before flag parsing stay silent.
var logger = slog.New(slog.NewTextHandler(io.Discard, nil))

// setupLogging configures the package logger from the --log-level flag value,
// writing to w (the command's stderr). Levels: error (quiet even on warnings),
// warn (default: role-name typos and RBAC-coarsening warnings are visible
// without flags), info (command start/end + counts), debug (per-stage
// pipeline logs).
//
// At debug it additionally arms cluster.KafkaLogWriter so the real franz-go
// client attaches kgo's BasicLogger; at any other level the hook is cleared.
// Secrets never pass through this logger: the resolver values are handed
// directly to the kgo/SR clients and are not logged anywhere in the CLI.
func setupLogging(w io.Writer, level string) error {
	var l slog.Level
	switch level {
	case "error":
		l = slog.LevelError
	case "warn":
		l = slog.LevelWarn
	case "info":
		l = slog.LevelInfo
	case "debug":
		l = slog.LevelDebug
	default:
		return &ExitError{Code: 2, Msg: "invalid --log-level " + level + " (want error, warn, info, or debug)"}
	}
	logger = slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: l}))
	if l == slog.LevelDebug {
		cluster.KafkaLogWriter = w
	} else {
		cluster.KafkaLogWriter = nil
	}
	return nil
}

// controllerRuntimeLogger returns a logr.Logger that writes through the CLI's
// current slog handler, so controller-runtime output (the manager internals
// and every reconciler's log.FromContext logger) obeys --log-level and lands
// on the same stderr stream as the rest of the CLI's logs. The operator
// command hands it to ctrl.SetLogger exactly once, after setupLogging has run
// (PersistentPreRunE) and before the manager is constructed — without that
// call, controller-runtime v0.24's log.Log discards everything.
//
// Level mapping (logr has no warn level): logr Info -> slog INFO (visible at
// --log-level=info and below), logr Error -> slog ERROR (always visible),
// logr V(n) -> below INFO (visible at --log-level=debug for n <= 4).
func controllerRuntimeLogger() logr.Logger {
	return logr.FromSlogHandler(logger.Handler())
}
