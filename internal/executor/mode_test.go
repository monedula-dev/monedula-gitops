package executor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
)

func TestApplyReportsNonEnforceOpsWithoutExecuting(t *testing.T) {
	client := mock.New(nil, nil)

	detect := createTopicOp("detect", 1, nil)
	detect.Mode = operations.ModeDetectOnly
	observe := createTopicOp("observe", 1, nil)
	observe.Mode = operations.ModeObserveOnly

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{detect, observe}, Approvals{})

	require.Len(t, res.Results, 2)
	for _, r := range res.Results {
		require.Equal(t, ReportOnly, r.Status)
		require.Empty(t, r.Err)
	}
	require.Empty(t, client.Calls(), "non-Enforce ops must never reach the client")
	require.True(t, res.OK(), "ReportOnly is not a failure")
}

func TestApplyExecutesEnforceAndEmptyMode(t *testing.T) {
	client := mock.New(nil, nil)

	enforce := createTopicOp("enforce", 1, nil)
	enforce.Mode = operations.ModeEnforce
	unattributed := createTopicOp("legacy", 1, nil) // empty Mode: operator path

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{enforce, unattributed}, Approvals{})

	require.Len(t, res.Results, 2)
	require.Equal(t, Succeeded, res.Results[0].Status)
	require.Equal(t, Succeeded, res.Results[1].Status)
	require.Contains(t, client.Calls(), "CreateTopic enforce")
	require.Contains(t, client.Calls(), "CreateTopic legacy")
}

func TestApplyModeGatePrecedesRiskGatesAndRejected(t *testing.T) {
	client := mock.New([]kafka.TopicState{{Name: "t", Partitions: 3}}, nil)

	// A gated destructive op on a DetectOnly resource is ReportOnly, not
	// Blocked: in a non-Enforce mode nothing would execute even with approval.
	gated := operations.New(operations.IncreasePartitions)
	gated.Target = "t"
	gated.Partitions = 6
	gated.Mode = operations.ModeDetectOnly

	// A Rejected op on an ObserveOnly resource is ReportOnly, not Rejected, so
	// it cannot fail the run.
	rejected := operations.New(operations.Rejected)
	rejected.Target = "t"
	rejected.Mode = operations.ModeObserveOnly

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{gated, rejected}, Approvals{})

	require.Len(t, res.Results, 2)
	require.Equal(t, ReportOnly, res.Results[0].Status)
	require.Equal(t, ReportOnly, res.Results[1].Status)
	require.Empty(t, client.Calls())
	require.True(t, res.OK())
}

func TestResultOKMixedSucceededAndReportOnly(t *testing.T) {
	res := Result{Results: []OpResult{
		{Status: Succeeded},
		{Status: ReportOnly},
	}}
	require.True(t, res.OK())

	res.Results = append(res.Results, OpResult{Status: Blocked})
	require.False(t, res.OK())
}
