package cli

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSetupLoggingLevelParsing pins the --log-level -> slog.Level mapping,
// including the warn level added between error and info (review fix: role-name
// typos and RBAC-coarsening warnings must be visible without flags at the new
// default).
func TestSetupLoggingLevelParsing(t *testing.T) {
	cases := []struct {
		level     string
		wantLevel slog.Level
		wantErr   bool
	}{
		{level: "error", wantLevel: slog.LevelError},
		{level: "warn", wantLevel: slog.LevelWarn},
		{level: "info", wantLevel: slog.LevelInfo},
		{level: "debug", wantLevel: slog.LevelDebug},
		{level: "verbose", wantErr: true},
		{level: "", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.level, func(t *testing.T) {
			var buf bytes.Buffer
			err := setupLogging(&buf, tc.level)
			if tc.wantErr {
				require.Error(t, err)
				var ee *ExitError
				require.ErrorAs(t, err, &ee)
				require.Equal(t, 2, ee.Code)
				return
			}
			require.NoError(t, err)
			require.True(t, logger.Enabled(context.TODO(), tc.wantLevel), "level %s should enable %s", tc.level, tc.wantLevel)
			if tc.wantLevel > slog.LevelDebug {
				require.False(t, logger.Enabled(context.TODO(), tc.wantLevel-1), "level %s should not enable one level below", tc.level)
			}
		})
	}
}

// TestSetupLoggingWarnIsBetweenErrorAndInfo asserts the level ordering
// invariant directly: warn-level logs are suppressed at --log-level=error and
// emitted at --log-level=warn (and above).
func TestSetupLoggingWarnIsBetweenErrorAndInfo(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, setupLogging(&buf, "error"))
	logger.Warn("should not appear")
	require.Empty(t, buf.String())

	buf.Reset()
	require.NoError(t, setupLogging(&buf, "warn"))
	logger.Warn("should appear")
	require.Contains(t, buf.String(), "should appear")
}

// TestControllerRuntimeLoggerBridgesSlogHandler pins the logr<->slog bridge the
// operator command hands to ctrl.SetLogger: logr output must write through the
// CLI's configured handler and obey --log-level (logr Info maps to slog INFO,
// logr Error to slog ERROR), so controller-runtime logs are governed by the
// same flag as everything else instead of being silently discarded.
func TestControllerRuntimeLoggerBridgesSlogHandler(t *testing.T) {
	var buf bytes.Buffer

	// At --log-level=info, logr Info-level output is emitted with its key/value
	// pairs.
	require.NoError(t, setupLogging(&buf, "info"))
	l := controllerRuntimeLogger()
	l.Info("bridged info line", "requested", 4)
	require.Contains(t, buf.String(), "bridged info line")
	require.Contains(t, buf.String(), "requested=4")

	// At the default --log-level=warn, logr Info is suppressed but logr Error
	// still surfaces.
	buf.Reset()
	require.NoError(t, setupLogging(&buf, "warn"))
	l = controllerRuntimeLogger()
	l.Info("suppressed info line")
	require.Empty(t, buf.String())
	l.Error(nil, "bridged error line")
	require.Contains(t, buf.String(), "bridged error line")
}
