package franz

// Hermetic regression test for review I7: DeleteTopic must be IDEMPOTENT.
// kadm surfaces UNKNOWN_TOPIC_OR_PARTITION as the per-topic response error
// when deleting an absent topic; the adapter must treat that as success
// (mirroring the mock), otherwise a deletionPolicy=Delete CR whose topic was
// already removed out-of-band can never finalize (every delete attempt errors
// and the finalizer blocks forever). The kadm response path itself needs a
// real broker (covered by the integration test), so the error filtering is
// extracted into deleteTopicErr and unit-tested here.

import (
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
)

func TestDeleteTopicErr(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantNil bool
	}{
		{"nil passes through", nil, true},
		{"unknown topic is idempotent success", kerr.UnknownTopicOrPartition, true},
		{"wrapped unknown topic is idempotent success",
			fmt.Errorf("per-topic: %w", kerr.UnknownTopicOrPartition), true},
		{"other kerr propagates", kerr.TopicAuthorizationFailed, false},
		{"plain error propagates", errors.New("boom"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deleteTopicErr(tt.err)
			if tt.wantNil && got != nil {
				t.Fatalf("deleteTopicErr(%v) = %v, want nil (idempotent delete)", tt.err, got)
			}
			if !tt.wantNil && got == nil {
				t.Fatalf("deleteTopicErr(%v) = nil, want the error propagated", tt.err)
			}
		})
	}
}
