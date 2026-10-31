package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/stretchr/testify/require"
)

type fakeMirrors struct {
	created, altered []operations.Operation
	err              error
}

func (f *fakeMirrors) CreateMirror(_ context.Context, op operations.Operation) error {
	f.created = append(f.created, op)
	return f.err
}

func (f *fakeMirrors) AlterMirrorConfig(_ context.Context, op operations.Operation) error {
	f.altered = append(f.altered, op)
	return f.err
}

func mirrorOp(a operations.Action, msg string) operations.Operation {
	op := operations.New(a)
	op.Target = "orders"
	op.Message = msg
	return op
}

func TestExecutorMirrorStatuses(t *testing.T) {
	cases := []struct {
		action operations.Action
		want   Status
		err    string
	}{
		{operations.MirrorTopicProgressing, ReportOnly, ""},
		{operations.MirrorTopicUnhealthy, Unsupported, "msg"},
		{operations.MirrorTopicStopping, Unsupported, "msg"},
		{operations.MirrorTopicPromoted, Unsupported, "msg"},
		{operations.UnsupportedReplicationFactorChange, Unsupported, "msg"},
		{operations.StateReadError, Failed, "msg"},
	}
	for _, c := range cases {
		t.Run(string(c.action), func(t *testing.T) {
			f := &fakeMirrors{}
			k := mock.New(nil, nil)
			res := Apply(context.Background(), Clients{Kafka: k, Mirrors: f},
				[]operations.Operation{mirrorOp(c.action, "msg")}, Approvals{Delete: true, Destructive: true, Prune: true})
			require.Len(t, res.Results, 1)
			require.Equal(t, c.want, res.Results[0].Status)
			require.Equal(t, c.err, res.Results[0].Err)
			require.Equal(t, c.want == ReportOnly, res.OK())
			require.Empty(t, f.created)
			require.Empty(t, f.altered)
			require.Empty(t, k.Calls())
		})
	}
}

func TestExecutorMirrorModeGateFirst(t *testing.T) {
	op := mirrorOp(operations.StateReadError, "msg")
	op.Mode = "DetectOnly"
	res := Apply(context.Background(), Clients{}, []operations.Operation{op}, Approvals{})
	require.Equal(t, ReportOnly, res.Results[0].Status)
}

func TestExecutorCreateMirrorDispatch(t *testing.T) {
	f := &fakeMirrors{}
	create := mirrorOp(operations.CreateMirrorTopic, "")
	alter := mirrorOp(operations.UpdateMirrorTopicConfig, "")
	alter.Field, alter.To = "retention.ms", "1"
	res := Apply(context.Background(), Clients{Kafka: mock.New(nil, nil), Mirrors: f},
		[]operations.Operation{create, alter}, Approvals{})
	require.True(t, res.OK())
	require.Len(t, f.created, 1)
	require.Equal(t, "orders", f.created[0].Target)
	require.Len(t, f.altered, 1)
	require.Equal(t, "retention.ms", f.altered[0].Field)
}

func TestExecutorMirrorNilClient(t *testing.T) {
	for _, a := range []operations.Action{operations.CreateMirrorTopic, operations.UpdateMirrorTopicConfig} {
		res := Apply(context.Background(), Clients{Kafka: mock.New(nil, nil)},
			[]operations.Operation{mirrorOp(a, "")}, Approvals{})
		require.Equal(t, Failed, res.Results[0].Status)
		require.Equal(t, "Kafka REST not configured", res.Results[0].Err)
	}
}

func TestExecutorFailedCreateMirrorSkipsACL(t *testing.T) {
	f := &fakeMirrors{err: errors.New("outcome unknown")}
	acl := operations.New(operations.CreateAcl)
	acl.Target = "orders"
	acl.ACL = &kafka.ACLState{ResourceType: "topic", ResourceName: "orders"}
	res := Apply(context.Background(), Clients{Kafka: mock.New(nil, nil), Mirrors: f},
		[]operations.Operation{acl, mirrorOp(operations.CreateMirrorTopic, "")}, Approvals{})
	require.Equal(t, Failed, res.Results[0].Status)
	require.Equal(t, "outcome unknown", res.Results[0].Err)
	require.Equal(t, Skipped, res.Results[1].Status)
}
