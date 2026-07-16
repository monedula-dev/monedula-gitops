package executor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
)

// deleteAclOp builds a DeleteAcl prune candidate against topic "t".
func deleteAclOp() operations.Operation {
	op := operations.New(operations.DeleteAcl)
	op.Target = "User:x Read topic:t Allow"
	op.ACL = &kafka.ACLState{
		Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: "Read", Permission: "Allow",
	}
	return op
}

func pruneMock() *mock.Client {
	return mock.New(nil, []kafka.ACLState{{
		Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: "Read", Permission: "Allow",
	}})
}

func TestApplyDeleteAclWithoutConsentIsPruneDisabled(t *testing.T) {
	client := pruneMock()
	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{deleteAclOp()}, Approvals{})

	require.Len(t, res.Results, 1)
	require.Equal(t, PruneDisabled, res.Results[0].Status)
	require.Empty(t, res.Results[0].Err)
	require.Empty(t, client.Calls(), "an unconsented prune must never reach the client")
	require.True(t, res.OK(), "PruneDisabled is informational, not a failure")
}

func TestApplyDeleteAclDestructiveApprovalDoesNotPrune(t *testing.T) {
	// DeleteAcl is no longer behind the destructive gate (spec §10.3): the
	// --allow-destructive approval must NOT enable pruning.
	client := pruneMock()
	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{deleteAclOp()}, Approvals{Destructive: true, Delete: true})

	require.Equal(t, PruneDisabled, res.Results[0].Status)
	require.Empty(t, client.Calls())
}

func TestApplyDeleteAclWithRunWideConsent(t *testing.T) {
	// CLI path: apply --prune sets the run-wide Approvals.Prune.
	client := pruneMock()
	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{deleteAclOp()}, Approvals{Prune: true})

	require.Equal(t, Succeeded, res.Results[0].Status)
	require.Contains(t, client.Calls(), "DeleteACLs 1")
}

func TestApplyDeleteAclWithOpLevelConsent(t *testing.T) {
	// Operator path: the diff stamps PruneAllowed from the covering scope's
	// spec.prune consent; no run-wide approval is needed.
	client := pruneMock()
	op := deleteAclOp()
	op.PruneAllowed = true
	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{})

	require.Equal(t, Succeeded, res.Results[0].Status)
	require.Contains(t, client.Calls(), "DeleteACLs 1")
}

func TestApplyDeleteAclDetectOnlyStaysReportOnlyDespiteConsent(t *testing.T) {
	// Mode wins over prune consent (spec §16): a DetectOnly resource's prune
	// candidate is reported, never executed, even with --prune AND op consent.
	client := pruneMock()
	op := deleteAclOp()
	op.Mode = operations.ModeDetectOnly
	op.PruneAllowed = true
	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{Prune: true})

	require.Equal(t, ReportOnly, res.Results[0].Status)
	require.Empty(t, client.Calls())
	require.True(t, res.OK())
}
