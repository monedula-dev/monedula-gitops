package diff

import (
	"errors"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
	"github.com/monedula-dev/monedula-gitops/internal/mirror"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/stretchr/testify/require"
)

const (
	mtLink   = "dr-link"
	mtSource = "payments.orders"
)

func baseDesiredMirror() DesiredMirror {
	return DesiredMirror{
		Kind: "KafkaMirrorTopic", Namespace: "payments", Name: "orders-mirror",
		LinkName: mtLink, SourceTopicName: mtSource, TopicName: mtSource,
		Mode: operations.ModeEnforce,
	}
}

func activeLink(configs map[string]string) map[string]mirror.LinkInfo {
	if configs == nil {
		configs = map[string]string{"topic.config.sync.include": "cleanup.policy,compression.type"}
	}
	return map[string]mirror.LinkInfo{mtLink: {Link: kafkarest.Link{Name: mtLink, State: "ACTIVE"}, Configs: configs}}
}

func liveMirrorTopic(name string, rf int, cfg map[string]string) map[string]kafka.TopicState {
	return map[string]kafka.TopicState{name: {Name: name, Partitions: 6, ReplicationFactor: rf, Config: cfg}}
}

func liveMirror(link, source, name, status string) map[string]kafkarest.Mirror {
	return map[string]kafkarest.Mirror{name: {LinkName: link, SourceTopicName: source, MirrorTopicName: name, Status: status, NumPartitions: 6}}
}

func snapPtr(s mirror.Snapshot) *mirror.Snapshot { return &s }

func mirrorActions(ops []operations.Operation) []operations.Action {
	out := make([]operations.Action, 0, len(ops))
	for _, op := range ops {
		out = append(out, op.Action)
	}
	return out
}

func TestComputeMirrorOpsDecisionTable(t *testing.T) {
	type want struct {
		action operations.Action
		msg    string // substring of Message ("" = only require non-empty for non-create ops)
	}
	cases := []struct {
		name  string
		dm    func(*DesiredMirror)
		snap  *mirror.Snapshot
		want  []want
		check func(t *testing.T, ops []operations.Operation)
	}{
		{
			name: "nil snapshot",
			snap: nil,
			want: []want{{operations.StateReadError, "Kafka REST not configured"}},
		},
		{
			name: "incomplete snapshot",
			snap: snapPtr(mirror.NewSnapshot("c1", "c2", nil, nil, nil)),
			want: []want{{operations.StateReadError, "ClusterIdentityMismatch"}},
		},
		{
			name: "incomplete snapshot with read error",
			snap: func() *mirror.Snapshot {
				s := mirror.NewSnapshot("c1", "c1", nil, nil, activeLink(nil))
				s.Err = errors.New("list mirrors: 503")
				return &s
			}(),
			want: []want{{operations.StateReadError, "list mirrors: 503"}},
		},
		{
			name: "link missing",
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", nil, nil, nil)),
			want: []want{{operations.Rejected, `link "dr-link" not found on the destination cluster`}},
		},
		{
			name: "link config read error",
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", nil, nil, map[string]mirror.LinkInfo{
				mtLink: {Link: kafkarest.Link{Name: mtLink, State: "ACTIVE"}, ConfigErr: errors.New("boom")},
			})),
			want: []want{{operations.StateReadError, `cannot read link "dr-link" configs: boom`}},
		},
		{
			name: "prefix check fails (no prefix, name differs)",
			dm:   func(dm *DesiredMirror) { dm.TopicName, dm.TopicNameExplicit = "orders.copy", true },
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", nil, nil, activeLink(nil))),
			want: []want{{operations.Rejected, `topicName "orders.copy" differs from sourceTopicName "payments.orders" and link "dr-link" has no prefix`}},
		},
		{
			name: "absent and link not active",
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", nil, nil, map[string]mirror.LinkInfo{
				mtLink: {Link: kafkarest.Link{Name: mtLink, State: "PAUSED"}, Configs: map[string]string{"topic.config.sync.include": ""}},
			})),
			want: []want{{operations.Rejected, `link "dr-link" is PAUSED; not creating mirror`}},
		},
		{
			name: "absent and link not active with link error",
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", nil, nil, map[string]mirror.LinkInfo{
				mtLink: {Link: kafkarest.Link{Name: mtLink, State: "FAILED", Error: "AUTHENTICATION_ERROR"}, Configs: map[string]string{}},
			})),
			want: []want{{operations.Rejected, `link "dr-link" is FAILED; not creating mirror: AUTHENTICATION_ERROR`}},
		},
		{
			name: "absent and link not active with NO_ERROR",
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", nil, nil, map[string]mirror.LinkInfo{
				mtLink: {Link: kafkarest.Link{Name: mtLink, State: "PAUSED", Error: "NO_ERROR"}, Configs: map[string]string{}},
			})),
			check: func(t *testing.T, ops []operations.Operation) {
				require.Equal(t, `link "dr-link" is PAUSED; not creating mirror`, ops[0].Message)
			},
			want: []want{{operations.Rejected, `not creating mirror`}},
		},
		{
			name: "absent and declared key link-managed",
			dm: func(dm *DesiredMirror) {
				dm.Config = map[string]string{"cleanup.policy": "compact", "retention.ms": "1000"}
			},
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", nil, nil, activeLink(nil))),
			want: []want{{operations.Rejected, "cleanup.policy"}},
			check: func(t *testing.T, ops []operations.Operation) {
				require.Equal(t, "cleanup.policy", ops[0].Field)
			},
		},
		{
			name: "absent creates mirror",
			dm: func(dm *DesiredMirror) {
				dm.ReplicationFactor = 3
				dm.Config = map[string]string{"retention.ms": "1000"}
			},
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", nil, nil, activeLink(nil))),
			want: []want{{operations.CreateMirrorTopic, ""}},
			check: func(t *testing.T, ops []operations.Operation) {
				op := ops[0]
				require.Equal(t, mtLink, op.MirrorLink)
				require.Equal(t, mtSource, op.MirrorSource)
				require.False(t, op.MirrorNameExplicit)
				require.Equal(t, 3, op.ReplicationFactor)
				require.Equal(t, map[string]string{"retention.ms": "1000"}, op.Config)
				require.Equal(t, mtSource, op.Target)
				require.Equal(t, "KafkaMirrorTopic", op.Kind)
				require.Equal(t, "payments", op.Namespace)
				require.Equal(t, "orders-mirror", op.Name)
				require.Equal(t, operations.ModeEnforce, op.Mode)
			},
		},
		{
			name: "normal topic exists",
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", liveMirrorTopic(mtSource, 3, nil), nil, activeLink(nil))),
			want: []want{{operations.Rejected, "topic exists and is not a mirror"}},
		},
		{
			name: "stopped former mirror",
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", liveMirrorTopic(mtSource, 3, nil),
				liveMirror(mtLink, mtSource, mtSource, "STOPPED"), activeLink(nil))),
			want: []want{{operations.Rejected, "topic exists and is not a mirror"}, {operations.MirrorTopicPromoted, ""}},
		},
		{
			name: "unknown classification",
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", nil, liveMirror(mtLink, mtSource, mtSource, "ACTIVE"), activeLink(nil))),
			want: []want{{operations.StateReadError, ""}},
		},
		{
			name: "live mirror on different link",
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", liveMirrorTopic(mtSource, 3, nil),
				liveMirror("other-link", mtSource, mtSource, "ACTIVE"), activeLink(nil))),
			want: []want{{operations.Rejected, "mirror of payments.orders on link other-link; manifest declares payments.orders on dr-link"}},
		},
		{
			name: "stopping mirror",
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", liveMirrorTopic(mtSource, 3, nil),
				liveMirror(mtLink, mtSource, mtSource, "PENDING_STOPPED"), activeLink(nil))),
			want: []want{{operations.MirrorTopicStopping, ""}},
		},
		{
			name: "unhealthy mirror",
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", liveMirrorTopic(mtSource, 3, nil),
				liveMirror(mtLink, mtSource, mtSource, "LINK_FAILED"), activeLink(nil))),
			want: []want{{operations.MirrorTopicUnhealthy, "LINK_FAILED"}},
		},
		{
			name: "progressing mirror",
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", liveMirrorTopic(mtSource, 3, nil),
				liveMirror(mtLink, mtSource, mtSource, "PENDING_MIRROR"), activeLink(nil))),
			want: []want{{operations.MirrorTopicProgressing, ""}},
		},
		{
			name: "replication factor change",
			dm:   func(dm *DesiredMirror) { dm.ReplicationFactor = 4 },
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", liveMirrorTopic(mtSource, 3, nil),
				liveMirror(mtLink, mtSource, mtSource, "ACTIVE"), activeLink(nil))),
			want: []want{{operations.UnsupportedReplicationFactorChange, ""}},
			check: func(t *testing.T, ops []operations.Operation) {
				require.Equal(t, "3", ops[0].From)
				require.Equal(t, "4", ops[0].To)
			},
		},
		{
			name: "config drift on unmanaged key",
			dm:   func(dm *DesiredMirror) { dm.Config = map[string]string{"retention.ms": "1000"} },
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", liveMirrorTopic(mtSource, 3, map[string]string{"retention.ms": "500"}),
				liveMirror(mtLink, mtSource, mtSource, "ACTIVE"), activeLink(nil))),
			want: []want{{operations.UpdateMirrorTopicConfig, ""}},
			check: func(t *testing.T, ops []operations.Operation) {
				require.Equal(t, "retention.ms", ops[0].Field)
				require.Equal(t, "500", ops[0].From)
				require.Equal(t, "1000", ops[0].To)
				require.Equal(t, mtLink, ops[0].MirrorLink)
				require.Equal(t, mtSource, ops[0].MirrorSource)
				require.False(t, ops[0].MirrorNameExplicit)
			},
		},
		{
			name: "config drift on link-managed key",
			dm:   func(dm *DesiredMirror) { dm.Config = map[string]string{"cleanup.policy": "compact"} },
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", liveMirrorTopic(mtSource, 3, map[string]string{"cleanup.policy": "delete"}),
				liveMirror(mtLink, mtSource, mtSource, "ACTIVE"), activeLink(nil))),
			want: []want{{operations.Rejected, "cleanup.policy"}},
		},
		{
			name: "link-managed key matching live is still rejected",
			dm: func(dm *DesiredMirror) {
				dm.ReplicationFactor = 3
				dm.Config = map[string]string{"retention.ms": "1000", "cleanup.policy": "delete"}
			},
			snap: snapPtr(mirror.NewSnapshot("c1", "c1",
				liveMirrorTopic(mtSource, 3, map[string]string{"retention.ms": "1000", "cleanup.policy": "delete"}),
				liveMirror(mtLink, mtSource, mtSource, "ACTIVE"), activeLink(nil))),
			want: []want{{operations.Rejected, "cleanup.policy"}},
			check: func(t *testing.T, ops []operations.Operation) {
				require.Equal(t, "cleanup.policy", ops[0].Field)
			},
		},
		{
			name: "undeterminable key matching live is still rejected",
			dm:   func(dm *DesiredMirror) { dm.Config = map[string]string{"retention.ms": "1000"} },
			snap: snapPtr(mirror.NewSnapshot("c1", "c1",
				liveMirrorTopic(mtSource, 3, map[string]string{"retention.ms": "1000"}),
				liveMirror(mtLink, mtSource, mtSource, "ACTIVE"), activeLink(map[string]string{}))),
			want: []want{{operations.Rejected, "cannot determine whether retention.ms is link-managed"}},
		},
		{
			name: "healthy mirror in sync with local keys only",
			dm: func(dm *DesiredMirror) {
				dm.ReplicationFactor = 3
				dm.Config = map[string]string{"retention.ms": "1000"}
			},
			snap: snapPtr(mirror.NewSnapshot("c1", "c1",
				liveMirrorTopic(mtSource, 3, map[string]string{"retention.ms": "1000", "cleanup.policy": "delete"}),
				liveMirror(mtLink, mtSource, mtSource, "ACTIVE"), activeLink(nil))),
			want: nil,
		},
		{
			name: "zero-value snapshot",
			snap: &mirror.Snapshot{},
			want: []want{{operations.StateReadError, ""}},
		},
		{
			name: "unrecognized status with topic present",
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", liveMirrorTopic(mtSource, 3, nil),
				liveMirror(mtLink, mtSource, mtSource, "PENDING_SOMETHING_NEW"), activeLink(nil))),
			want: []want{{operations.StateReadError, "PENDING_SOMETHING_NEW"}},
		},
		{
			name: "unhealthy mirror with RF and config drift",
			dm: func(dm *DesiredMirror) {
				dm.ReplicationFactor = 4
				dm.Config = map[string]string{"retention.ms": "1000"}
			},
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", liveMirrorTopic(mtSource, 3, map[string]string{"retention.ms": "500"}),
				liveMirror(mtLink, mtSource, mtSource, "PAUSED"), activeLink(nil))),
			want: []want{
				{operations.MirrorTopicUnhealthy, "PAUSED"},
				{operations.UnsupportedReplicationFactorChange, ""},
				{operations.UpdateMirrorTopicConfig, ""},
			},
		},
		{
			name: "stopping mirror with config drift",
			dm:   func(dm *DesiredMirror) { dm.Config = map[string]string{"retention.ms": "1000"} },
			snap: snapPtr(mirror.NewSnapshot("c1", "c1", liveMirrorTopic(mtSource, 3, map[string]string{"retention.ms": "500"}),
				liveMirror(mtLink, mtSource, mtSource, "PENDING_STOPPED"), activeLink(nil))),
			want: []want{{operations.MirrorTopicStopping, ""}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dm := baseDesiredMirror()
			if tc.dm != nil {
				tc.dm(&dm)
			}
			ops := computeMirrorOps(Desired{Mirrors: []DesiredMirror{dm}}, Live{Mirrors: tc.snap})
			wantActions := make([]operations.Action, 0, len(tc.want))
			for _, w := range tc.want {
				wantActions = append(wantActions, w.action)
			}
			require.Equal(t, wantActions, mirrorActions(ops))
			for i, w := range tc.want {
				op := ops[i]
				require.Equal(t, dm.Kind, op.Kind)
				require.Equal(t, dm.Namespace, op.Namespace)
				require.Equal(t, dm.Name, op.Name)
				require.Equal(t, dm.TopicName, op.Target)
				require.Equal(t, dm.Mode, op.Mode)
				if op.Action != operations.CreateMirrorTopic {
					require.NotEmpty(t, op.Message, op.Action)
				}
				if w.msg != "" {
					require.Contains(t, op.Message, w.msg)
				}
			}
			if tc.check != nil {
				tc.check(t, ops)
			}
		})
	}
}

func TestMirrorOpsIgnoreUndeclaredLiveMirrors(t *testing.T) {
	topics := map[string]kafka.TopicState{
		"a": {Name: "a", ReplicationFactor: 3}, "b": {Name: "b", ReplicationFactor: 3}, "c": {Name: "c", ReplicationFactor: 3},
	}
	mirrors := map[string]kafkarest.Mirror{
		"a": {LinkName: "l1", SourceTopicName: "a", MirrorTopicName: "a", Status: "ACTIVE"},
		"b": {LinkName: "l1", SourceTopicName: "b", MirrorTopicName: "b", Status: "FAILED"},
		"c": {LinkName: "l2", SourceTopicName: "c", MirrorTopicName: "c", Status: "PENDING_STOPPED"},
	}
	s := mirror.NewSnapshot("c1", "c1", topics, mirrors, nil)
	require.Empty(t, computeMirrorOps(Desired{}, Live{Mirrors: &s}))
	require.Empty(t, Compute(Desired{}, Live{Mirrors: &s}))
	// No desired mirrors and no snapshot: nothing to report either.
	require.Empty(t, Compute(Desired{}, Live{}))
}

func TestMirrorPrefixEmptyStringIsNoPrefix(t *testing.T) {
	s := mirror.NewSnapshot("c1", "c1", nil, nil, activeLink(map[string]string{
		"cluster.link.prefix": "", "topic.config.sync.include": "cleanup.policy",
	}))
	ops := computeMirrorOps(Desired{Mirrors: []DesiredMirror{baseDesiredMirror()}}, Live{Mirrors: &s})
	require.Equal(t, []operations.Action{operations.CreateMirrorTopic}, mirrorActions(ops))
}

func TestMirrorPrefixedLinkRequiresExplicitName(t *testing.T) {
	s := mirror.NewSnapshot("c1", "c1", nil, nil, activeLink(map[string]string{
		"cluster.link.prefix": "dr.", "topic.config.sync.include": "cleanup.policy",
	}))
	ops := computeMirrorOps(Desired{Mirrors: []DesiredMirror{baseDesiredMirror()}}, Live{Mirrors: &s})
	require.Equal(t, []operations.Action{operations.Rejected}, mirrorActions(ops))
	require.Contains(t, ops[0].Message, `"dr.payments.orders"`)

	// Explicit and correct prefixed name is accepted.
	dm := baseDesiredMirror()
	dm.TopicName, dm.TopicNameExplicit = "dr.payments.orders", true
	ops = computeMirrorOps(Desired{Mirrors: []DesiredMirror{dm}}, Live{Mirrors: &s})
	require.Equal(t, []operations.Action{operations.CreateMirrorTopic}, mirrorActions(ops))
	require.True(t, ops[0].MirrorNameExplicit)
	require.Equal(t, "dr.payments.orders", ops[0].Target)
}

func TestMirrorSyncIncludeWildcard(t *testing.T) {
	dm := baseDesiredMirror()
	dm.Config = map[string]string{"min.insync.replicas": "2", "retention.ms": "1000"}
	s := mirror.NewSnapshot("c1", "c1", liveMirrorTopic(mtSource, 3, map[string]string{"min.insync.replicas": "1", "retention.ms": "500"}),
		liveMirror(mtLink, mtSource, mtSource, "ACTIVE"),
		activeLink(map[string]string{"topic.config.sync.include": " cleanup.policy , min.insync.* "}))
	ops := computeMirrorOps(Desired{Mirrors: []DesiredMirror{dm}}, Live{Mirrors: &s})
	require.Equal(t, []operations.Action{operations.Rejected, operations.UpdateMirrorTopicConfig}, mirrorActions(ops))
	require.Equal(t, "min.insync.replicas", ops[0].Field)
	require.Contains(t, ops[0].Message, "min.insync.replicas")
	require.Equal(t, "retention.ms", ops[1].Field)
}

func TestMirrorConfigUndeterminable(t *testing.T) {
	for name, cfg := range map[string]map[string]string{
		"absent":     {},
		"empty":      {"topic.config.sync.include": ""},
		"whitespace": {"topic.config.sync.include": "  "},
	} {
		t.Run(name, func(t *testing.T) {
			dm := baseDesiredMirror()
			dm.Config = map[string]string{"retention.ms": "1000"}
			s := mirror.NewSnapshot("c1", "c1", nil, nil, activeLink(cfg))
			ops := computeMirrorOps(Desired{Mirrors: []DesiredMirror{dm}}, Live{Mirrors: &s})
			require.Equal(t, []operations.Action{operations.Rejected}, mirrorActions(ops))
			require.Contains(t, ops[0].Message, "cannot determine whether retention.ms is link-managed")
		})
	}
}

func TestCreateMirrorTopicCopiesConfig(t *testing.T) {
	dm := baseDesiredMirror()
	dm.Config = map[string]string{"retention.ms": "1000"}
	s := mirror.NewSnapshot("c1", "c1", nil, nil, activeLink(nil))
	ops := computeMirrorOps(Desired{Mirrors: []DesiredMirror{dm}}, Live{Mirrors: &s})
	require.Equal(t, []operations.Action{operations.CreateMirrorTopic}, mirrorActions(ops))
	ops[0].Config["retention.ms"] = "2"
	require.Equal(t, "1000", dm.Config["retention.ms"])
}

func TestLinkActive(t *testing.T) {
	require.True(t, mirror.LinkActive(mirror.LinkInfo{Link: kafkarest.Link{State: "ACTIVE"}}))
	require.False(t, mirror.LinkActive(mirror.LinkInfo{Link: kafkarest.Link{State: "PAUSED"}}))
	require.False(t, mirror.LinkActive(mirror.LinkInfo{}))
}

func TestMirrorOpsCarryMode(t *testing.T) {
	dm := baseDesiredMirror()
	dm.Mode = operations.ModeDetectOnly
	s := mirror.NewSnapshot("c1", "c1", nil, nil, activeLink(nil))
	ops := computeMirrorOps(Desired{Mirrors: []DesiredMirror{dm}}, Live{Mirrors: &s})
	require.Len(t, ops, 1)
	require.Equal(t, operations.ModeDetectOnly, ops[0].Mode)
	require.True(t, ops[0].ReportOnly())
}

func TestMirrorOpsDeterministicOrder(t *testing.T) {
	a := baseDesiredMirror()
	a.Name, a.SourceTopicName, a.TopicName = "b-res", "b.topic", "b.topic"
	a.Config = map[string]string{"z.key": "1", "a.key": "1"}
	b := baseDesiredMirror()
	b.Name, b.SourceTopicName, b.TopicName = "a-res", "a.topic", "a.topic"
	s := mirror.NewSnapshot("c1", "c1", nil, nil, activeLink(map[string]string{"topic.config.sync.include": "a.key,z.key"}))
	ops1 := Compute(Desired{Mirrors: []DesiredMirror{a, b}}, Live{Mirrors: &s})
	ops2 := Compute(Desired{Mirrors: []DesiredMirror{b, a}}, Live{Mirrors: &s})
	require.Equal(t, ops1, ops2)
	raw := computeMirrorOps(Desired{Mirrors: []DesiredMirror{a, b}}, Live{Mirrors: &s})
	require.Equal(t, "a.topic", raw[0].Target)
	require.Equal(t, "a.key", raw[1].Field)
	require.Equal(t, "z.key", raw[2].Field)
}
