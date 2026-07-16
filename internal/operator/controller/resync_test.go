package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestResyncInterval pins resync.go's zero-value-means-default contract: a
// reconciler struct literal that never sets ResyncInterval (every existing
// unit test in this package) must keep resolving to the pre-v0.36 5-minute
// cadence, while a configured non-zero value always wins.
func TestResyncInterval(t *testing.T) {
	require.Equal(t, DefaultResyncInterval, resyncInterval(0))
	require.Equal(t, 5*time.Minute, resyncInterval(0))
	require.Equal(t, 30*time.Second, resyncInterval(30*time.Second))
	require.Equal(t, time.Hour, resyncInterval(time.Hour))
}

// TestReconcilerOptions pins the MaxConcurrentReconciles zero-value-means-
// default contract mirrored from resyncInterval, the guard against a negative
// value slipping through to controller.Options, and — since v0.37 lifted the
// clamp — that values >1 pass through unchanged to every kind's WithOptions.
func TestReconcilerOptions(t *testing.T) {
	require.Equal(t, DefaultMaxConcurrentReconciles, reconcilerOptions(0).MaxConcurrentReconciles)
	require.Equal(t, 1, reconcilerOptions(1).MaxConcurrentReconciles)
	require.Equal(t, DefaultMaxConcurrentReconciles, reconcilerOptions(-1).MaxConcurrentReconciles)
	require.Equal(t, 4, reconcilerOptions(4).MaxConcurrentReconciles)
	require.Equal(t, 100, reconcilerOptions(100).MaxConcurrentReconciles)
}
