package franz

// Hermetic unit tests for the pure SCRAM mechanism-mapping logic. The kadm
// round-trip itself (DescribeUserSCRAMs / AlterUserSCRAMs against a real
// broker) needs a live cluster and is covered separately; parseScramMechanism
// is extracted so its "unknown mechanism is a hard error, never silent" and
// "canonical string in, kadm enum out" behavior can be pinned without one.

import (
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestParseScramMechanism(t *testing.T) {
	tests := []struct {
		name      string
		mechanism string
		want      kadm.ScramMechanism
		wantErr   bool
	}{
		{"SCRAM-SHA-256 maps to kadm.ScramSha256", "SCRAM-SHA-256", kadm.ScramSha256, false},
		{"SCRAM-SHA-512 maps to kadm.ScramSha512", "SCRAM-SHA-512", kadm.ScramSha512, false},
		{"lowercase is rejected (canonical form only)", "scram-sha-256", 0, true},
		{"unknown mechanism is a hard error", "PLAIN", 0, true},
		{"empty string is a hard error", "", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseScramMechanism(tt.mechanism)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseScramMechanism(%q) = nil error, want an error", tt.mechanism)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseScramMechanism(%q) unexpected error: %v", tt.mechanism, err)
			}
			if got != tt.want {
				t.Fatalf("parseScramMechanism(%q) = %v, want %v", tt.mechanism, got, tt.want)
			}
		})
	}
}

// TestDefaultScramIterationsIsWithinBrokerBounds pins that the adapter's
// zero-iterations fallback stays within kadm's documented UpsertSCRAM.Iterations
// bound (must be between 4096 and 16384) -- if this constant ever drifts
// outside that range, upserts with Iterations unset would start failing.
func TestDefaultScramIterationsIsWithinBrokerBounds(t *testing.T) {
	const (
		minIterations = 4096
		maxIterations = 16384
	)
	if defaultScramIterations < minIterations || defaultScramIterations > maxIterations {
		t.Fatalf("defaultScramIterations = %d, want within [%d, %d]",
			defaultScramIterations, minIterations, maxIterations)
	}
}

// TestIsResourceNotFound is a hermetic regression test for a live-broker
// finding (confirmed against Confluent Local 7.6.1/KRaft): when
// ListScramCredentials explicitly names a user that currently has zero SCRAM
// credentials (e.g. immediately after its last mechanism was deleted),
// DescribeUserSCRAMCredentials returns RESOURCE_NOT_FOUND as that user's
// per-user Err -- not merely an absence from the response map. Without
// filtering it, that turned "list credentials for a user with none left"
// into a hard error, which would break KafkaUser reconciliation the moment a
// credential is fully removed. The kadm round-trip itself needs a live broker
// (covered by TestIntegration_ScramCredentialRoundTrip); the filtering logic
// is pinned here without one.
func TestIsResourceNotFound(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is not ResourceNotFound", nil, false},
		{"kerr.ResourceNotFound matches", kerr.ResourceNotFound, true},
		{"wrapped kerr.ResourceNotFound matches",
			fmt.Errorf("describing user: %w", kerr.ResourceNotFound), true},
		{"a different kerr does not match", kerr.UnknownTopicOrPartition, false},
		{"a plain error does not match", errors.New("boom"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isResourceNotFound(tt.err); got != tt.want {
				t.Fatalf("isResourceNotFound(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
