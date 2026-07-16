package cli

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/operator/manager"
)

// TestOperatorCmdFlagDefaults asserts the operator command exposes the expected
// flags with their documented defaults. It builds the command only; it never
// runs RunE, so no manager is started.
func TestOperatorCmdFlagDefaults(t *testing.T) {
	cmd := newOperatorCmd()
	require.Equal(t, "operator", cmd.Name())

	cases := map[string]string{
		"metrics-bind-address":      ":8080",
		"metrics-secure":            "false",
		"health-probe-bind-address": ":8081",
		"leader-elect":              "false",
		"cluster-namespace":         "",
		"enable-webhooks":           "false",
		"webhook-cert-dir":          "",
		"resync-interval":           "5m0s",
		"max-concurrent-reconciles": "1",
	}
	for name, want := range cases {
		f := cmd.Flags().Lookup(name)
		require.NotNilf(t, f, "flag %q should be registered", name)
		require.Equalf(t, want, f.DefValue, "flag %q default", name)
	}
}

// TestOperatorCmdRegistered confirms the operator subcommand is wired into the
// root command tree.
func TestOperatorCmdRegistered(t *testing.T) {
	root := NewRootCmd()
	var found bool
	for _, c := range root.Commands() {
		if c.Name() == "operator" {
			found = true
			break
		}
	}
	require.True(t, found, "operator command should be registered on the root command")
}

// TestOperatorCmdResyncIntervalBelowMinimumRejected pins the --resync-interval
// floor: RunE must reject a too-short interval BEFORE calling manager.Run (which
// would otherwise try to build a real client and block), so this must return
// promptly with an ExitError rather than hang or panic.
func TestOperatorCmdResyncIntervalBelowMinimumRejected(t *testing.T) {
	_, err := run(t, "operator", "--resync-interval=1s")
	requireExitCode(t, err, 2)
	var ee *ExitError
	require.ErrorAs(t, err, &ee)
	require.Contains(t, ee.Msg, "--resync-interval must be at least")
}

// TestOperatorCmdResyncIntervalZeroRejected: an explicit 0 (e.g. a Helm
// misrender) is rejected the same way as a too-short duration, not silently
// reinterpreted as "use the default".
func TestOperatorCmdResyncIntervalZeroRejected(t *testing.T) {
	_, err := run(t, "operator", "--resync-interval=0s")
	requireExitCode(t, err, 2)
}

// TestOperatorCmdConcurrencyWithoutLeaderElectRejected pins the v0.37
// single-active-replica guard: --max-concurrent-reconciles >1 without
// --leader-elect must be refused at startup (exit 2, before manager.Run) —
// the in-process locking that makes >1 safe cannot protect against a second
// ACTIVE replica, so leader election is a hard precondition, not a warning.
func TestOperatorCmdConcurrencyWithoutLeaderElectRejected(t *testing.T) {
	_, err := run(t, "operator", "--max-concurrent-reconciles=2")
	requireExitCode(t, err, 2)
	var ee *ExitError
	require.ErrorAs(t, err, &ee)
	require.Contains(t, ee.Msg, "--leader-elect")
	require.Contains(t, ee.Msg, "--max-concurrent-reconciles=2")
}

// TestValidateOperatorOptionsConcurrencyMatrix covers the accept side of the
// guard via the extracted validation helper (RunE would start a real manager,
// so the passing combinations cannot be exercised through run()): >1 WITH
// leader election passes validation, as do 1/0 without it.
func TestValidateOperatorOptionsConcurrencyMatrix(t *testing.T) {
	base := manager.Options{ResyncInterval: 5 * time.Minute}

	withLE := base
	withLE.MaxConcurrentReconciles = 4
	withLE.LeaderElect = true
	require.NoError(t, validateOperatorOptions(withLE), ">1 with --leader-elect must pass validation")

	serial := base
	serial.MaxConcurrentReconciles = 1
	require.NoError(t, validateOperatorOptions(serial), "1 without --leader-elect must stay valid")

	zero := base
	require.NoError(t, validateOperatorOptions(zero), "unset concurrency must stay valid")

	rejected := base
	rejected.MaxConcurrentReconciles = 4
	err := validateOperatorOptions(rejected)
	var ee *ExitError
	require.ErrorAs(t, err, &ee)
	require.Equal(t, 2, ee.Code)
	require.Contains(t, ee.Msg, "--leader-elect")
}
