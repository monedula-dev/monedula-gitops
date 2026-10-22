package importer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
	restmock "github.com/monedula-dev/monedula-gitops/internal/kafkarest/mock"
)

const mirrorTestClusterID = "dr-cluster-id"

// mirrorFixture builds a destination cluster with the normal topic "unrelated"
// and the topic "payments.orders" (RF 3) that REST reports as a mirror of itself
// over link prod-to-dr with the given status and link configs.
func mirrorFixture(t *testing.T, status string, topicConfig, linkConfigs map[string]string, acls []kafka.ACLState) (*mock.Client, *restmock.Client) {
	t.Helper()
	k := mock.New([]kafka.TopicState{
		{Name: "payments.orders", Partitions: 3, ReplicationFactor: 3, Config: topicConfig},
		{Name: "unrelated", Partitions: 1, ReplicationFactor: 3},
	}, acls)
	k.SetClusterID(mirrorTestClusterID)
	r := restmock.New(mirrorTestClusterID)
	r.AddLink(kafkarest.Link{Name: "prod-to-dr", State: "ACTIVE"}, linkConfigs)
	r.AddMirror(kafkarest.Mirror{LinkName: "prod-to-dr", MirrorTopicName: "payments.orders",
		SourceTopicName: "payments.orders", Status: status, NumPartitions: 3})
	return k, r
}

func importWithREST(t *testing.T, k *mock.Client, r *restmock.Client) Result {
	t.Helper()
	snap, err := ReadSnapshot(context.Background(), k, nil, nil, nil,
		SnapshotOptions{SkipUsers: true, SkipQuotas: true, REST: r})
	require.NoError(t, err)
	return Build(snap, "dr", []string{"acl"}, nil)
}

func hasWarningPrefix(ws []string, prefix string) bool {
	for _, w := range ws {
		if strings.HasPrefix(w, prefix) {
			return true
		}
	}
	return false
}

func TestImportMirrorNotAlsoTopic(t *testing.T) {
	k, r := mirrorFixture(t, "ACTIVE", nil, map[string]string{"acl.sync.enable": "false"}, nil)
	res := importWithREST(t, k, r)

	require.Len(t, res.Mirrors, 1)
	for _, tp := range res.Topics {
		require.NotEqual(t, "payments.orders", tp.Spec.TopicName, "a mirror must not also be imported as KafkaTopic")
	}
	require.Len(t, res.Topics, 1)

	m := res.Mirrors[0]
	require.Equal(t, "KafkaMirrorTopic", m.Kind)
	require.Equal(t, "payments.orders", m.Name)
	require.Equal(t, "dr", m.Spec.ClusterRef.Name)
	require.Equal(t, "prod-to-dr", m.Spec.LinkName)
	require.Equal(t, "payments.orders", m.Spec.SourceTopicName)
	require.Equal(t, "payments.orders", m.Spec.TopicName)
	require.NotNil(t, m.Spec.ReplicationFactor)
	require.Equal(t, 3, *m.Spec.ReplicationFactor)
	require.Nil(t, m.Spec.Config)
	require.Empty(t, m.Spec.DeletionPolicy)
	require.Equal(t, "dr", m.Annotations[importedFromAnnotation])
	require.False(t, res.Partial)
}

func TestImportStoppedBecomesTopic(t *testing.T) {
	k, r := mirrorFixture(t, "STOPPED", nil, nil, nil)
	res := importWithREST(t, k, r)

	require.Empty(t, res.Mirrors)
	names := []string{}
	for _, tp := range res.Topics {
		names = append(names, tp.Spec.TopicName)
	}
	require.ElementsMatch(t, []string{"payments.orders", "unrelated"}, names)
}

func TestImportPendingStoppedDeferred(t *testing.T) {
	k, r := mirrorFixture(t, "PENDING_STOPPED", nil, nil, nil)
	res := importWithREST(t, k, r)

	require.Empty(t, res.Mirrors)
	require.Len(t, res.Topics, 1)
	require.Equal(t, "unrelated", res.Topics[0].Spec.TopicName)
	require.Contains(t, res.Warnings,
		`MirrorImportDeferred: mirror topic "payments.orders" is being promoted (PENDING_STOPPED); re-run import after it finishes`)
	require.True(t, res.Partial)
}

func TestImportUnknownFails(t *testing.T) {
	k, r := mirrorFixture(t, "ACTIVE", nil, nil, nil)
	r.Fail("ListMirrors", errors.New("boom"))
	_, err := ReadSnapshot(context.Background(), k, nil, nil, nil,
		SnapshotOptions{SkipUsers: true, SkipQuotas: true, REST: r})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unknown")
}

func TestImportUnhealthyMirrorWarns(t *testing.T) {
	k, r := mirrorFixture(t, "PAUSED", nil, map[string]string{"acl.sync.enable": "false"}, nil)
	res := importWithREST(t, k, r)

	require.Len(t, res.Mirrors, 1)
	require.Contains(t, res.Warnings, `MirrorUnhealthy: mirror topic "payments.orders" is PAUSED`)
	require.False(t, res.Partial, "MirrorUnhealthy is diagnostic only")
}

func TestImportMirrorConfigsOmitted(t *testing.T) {
	k, r := mirrorFixture(t, "ACTIVE", map[string]string{"retention.ms": "604800000", "cleanup.policy": "delete"},
		map[string]string{"acl.sync.enable": "false"}, nil)
	res := importWithREST(t, k, r)

	require.Len(t, res.Mirrors, 1)
	require.Nil(t, res.Mirrors[0].Spec.Config)
	require.Contains(t, res.Warnings,
		`MirrorConfigNotImported: mirror topic "payments.orders" has 2 explicitly set configs; declare reviewed local keys manually`)
	require.True(t, res.Partial)
}

// mirrorACLs grants a consumer on the mirror topic and a producer on the normal
// topic "unrelated" (which folds into the KafkaTopic access block).
func mirrorACLs() []kafka.ACLState {
	return []kafka.ACLState{
		{Principal: "User:reader", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Read", Permission: "Allow"},
		{Principal: "User:writer", Host: "*", ResourceType: "topic", ResourceName: "unrelated", PatternType: "literal", Operation: "Write", Permission: "Allow"},
		{Principal: "User:writer", Host: "*", ResourceType: "topic", ResourceName: "unrelated", PatternType: "literal", Operation: "Describe", Permission: "Allow"},
	}
}

func policyRuleNames(res Result) []string {
	var out []string
	for _, p := range res.Policies {
		for _, rule := range p.Spec.Rules {
			out = append(out, rule.Resource.Name)
		}
	}
	return out
}

func TestImportACLSyncEnabledDropsACLs(t *testing.T) {
	for name, linkCfg := range map[string]map[string]string{
		"enabled": {"acl.sync.enable": "TRUE"},
		"unknown": {},
	} {
		t.Run(name, func(t *testing.T) {
			k, r := mirrorFixture(t, "ACTIVE", nil, linkCfg, mirrorACLs())
			res := importWithREST(t, k, r)

			require.NotContains(t, policyRuleNames(res), "payments.orders")
			require.Contains(t, res.Warnings,
				`ExternallyManagedOrAmbiguousACLs: 1 ACL(s) on mirror topic "payments.orders" were not imported (ACL sync on link "prod-to-dr" is enabled or unknown)`)
			for _, w := range res.Warnings {
				require.NotContains(t, w, "access reconstruction could not reproduce live state", "dropping ambiguous ACLs must not trigger the all-explicit fallback")
			}
			require.True(t, res.Partial)

			// The normal topic's producer still folds into its KafkaTopic.
			require.Len(t, res.Topics, 1)
			require.Len(t, res.Topics[0].Spec.Access.Producers, 1)
		})
	}
}

func TestImportACLSyncDisabledKeepsPolicy(t *testing.T) {
	k, r := mirrorFixture(t, "ACTIVE", nil, map[string]string{"acl.sync.enable": "false"}, mirrorACLs())
	res := importWithREST(t, k, r)

	require.Contains(t, policyRuleNames(res), "payments.orders")
	require.False(t, hasWarningPrefix(res.Warnings, "ExternallyManagedOrAmbiguousACLs:"))
	require.False(t, res.Partial)

	require.NoError(t, AssignNamespaces(&res, NamespaceStrategy{Kind: "prefix"}))
	require.Equal(t, "payments", res.Mirrors[0].Namespace)
	for _, p := range res.Policies {
		for _, rule := range p.Spec.Rules {
			if rule.Resource.Name == "payments.orders" {
				require.Equal(t, "payments", p.Namespace, "policy follows the mirror topic's namespace")
			}
		}
	}

	out, err := RenderManifests(res)
	require.NoError(t, err)
	require.Contains(t, string(out), "kind: KafkaMirrorTopic")

	dir := t.TempDir()
	outcome, err := WriteToDir(res, dir, "never")
	require.NoError(t, err)
	found := false
	for _, p := range outcome.Written {
		if strings.Contains(p, "mirrors") && strings.HasSuffix(p, "payments.orders.yaml") {
			found = true
		}
	}
	require.True(t, found, "mirror manifest written: %v", outcome.Written)

	sum := Summarize(res, "dr")
	require.Equal(t, 1, sum.Summary.Mirrors)
}

func TestImportWithoutRESTUnchanged(t *testing.T) {
	k, _ := mirrorFixture(t, "ACTIVE", nil, nil, nil)
	snap, err := ReadSnapshot(context.Background(), k, nil, nil, nil, SnapshotOptions{SkipUsers: true, SkipQuotas: true})
	require.NoError(t, err)
	require.Empty(t, snap.Mirrors)
	res := Build(snap, "dr", []string{"acl"}, nil)
	require.Empty(t, res.Mirrors)
	require.Len(t, res.Topics, 2)
	require.False(t, res.Partial)
}

func requireNoFallback(t *testing.T, res Result) {
	t.Helper()
	for _, w := range res.Warnings {
		require.NotContains(t, w, "access reconstruction could not reproduce live state", "dropped ACLs must not trigger the all-explicit fallback")
	}
}

func TestImportPendingStoppedDropsACLs(t *testing.T) {
	// ACL sync is conclusively disabled, yet the deferred mirror's ACLs are
	// still dropped: the topic is imported as neither kind.
	k, r := mirrorFixture(t, "PENDING_STOPPED", nil, map[string]string{"acl.sync.enable": "false"}, mirrorACLs())
	res := importWithREST(t, k, r)

	require.NotContains(t, policyRuleNames(res), "payments.orders")
	require.Contains(t, res.Warnings,
		`ExternallyManagedOrAmbiguousACLs: 1 ACL(s) on mirror topic "payments.orders" were not imported (mirror is being promoted; re-run import after it finishes)`)
	requireNoFallback(t, res)
	require.True(t, res.Partial)
	require.Len(t, res.Topics, 1)
	require.Len(t, res.Topics[0].Spec.Access.Producers, 1)
}

func TestImportMirrorWithoutKafkaTopicFails(t *testing.T) {
	k, r := mirrorFixture(t, "ACTIVE", nil, nil, nil)
	r.AddMirror(kafkarest.Mirror{LinkName: "prod-to-dr", MirrorTopicName: "ghost",
		SourceTopicName: "ghost", Status: "ACTIVE", NumPartitions: 1})
	_, err := ReadSnapshot(context.Background(), k, nil, nil, nil,
		SnapshotOptions{SkipUsers: true, SkipQuotas: true, REST: r})
	require.Error(t, err)
	require.Contains(t, err.Error(), `"ghost"`)
	require.Contains(t, err.Error(), "Unknown")
}

func TestImportClusterIDMismatchFails(t *testing.T) {
	k, r := mirrorFixture(t, "ACTIVE", nil, nil, nil)
	k.SetClusterID("some-other-cluster")
	_, err := ReadSnapshot(context.Background(), k, nil, nil, nil,
		SnapshotOptions{SkipUsers: true, SkipQuotas: true, REST: r})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ClusterIdentityMismatch")
}

func TestImportLinkConfigsUnreadableDropsACLs(t *testing.T) {
	k, r := mirrorFixture(t, "ACTIVE", nil, map[string]string{"acl.sync.enable": "false"}, mirrorACLs())
	r.Fail("LinkConfigs", errors.New("403 Forbidden"))
	snap, err := ReadSnapshot(context.Background(), k, nil, nil, nil,
		SnapshotOptions{SkipUsers: true, SkipQuotas: true, REST: r})
	require.NoError(t, err)
	require.Nil(t, snap.MirrorLinks["prod-to-dr"].ACLSync)

	res := Build(snap, "dr", []string{"acl"}, nil)
	require.NotContains(t, policyRuleNames(res), "payments.orders")
	require.Contains(t, res.Warnings,
		`ExternallyManagedOrAmbiguousACLs: 1 ACL(s) on mirror topic "payments.orders" were not imported (ACL sync on link "prod-to-dr" is enabled or unknown)`)
	requireNoFallback(t, res)
	require.True(t, res.Partial)
}

func TestImportStoppedMirrorWithoutTopicSkippedPartial(t *testing.T) {
	k, r := mirrorFixture(t, "ACTIVE", nil, map[string]string{"acl.sync.enable": "false"}, nil)
	r.AddMirror(kafkarest.Mirror{LinkName: "prod-to-dr", MirrorTopicName: "ghost",
		SourceTopicName: "ghost", Status: "STOPPED", NumPartitions: 1})
	res := importWithREST(t, k, r)

	require.Len(t, res.Mirrors, 1)
	require.Equal(t, "payments.orders", res.Mirrors[0].Spec.TopicName)
	for _, tp := range res.Topics {
		require.NotEqual(t, "ghost", tp.Spec.TopicName)
	}
	require.Contains(t, res.Warnings,
		`MirrorStaleEntry: REST lists stopped mirror "ghost" but the topic does not exist; skipped`)
	require.True(t, res.Partial)
}

func TestImportNonStoppedMirrorWithoutTopicStillFails(t *testing.T) {
	for _, status := range []string{"PAUSED", "PENDING_STOPPED", "SOMETHING_NEW"} {
		k, r := mirrorFixture(t, "ACTIVE", nil, nil, nil)
		r.AddMirror(kafkarest.Mirror{LinkName: "prod-to-dr", MirrorTopicName: "ghost",
			SourceTopicName: "ghost", Status: status, NumPartitions: 1})
		_, err := ReadSnapshot(context.Background(), k, nil, nil, nil,
			SnapshotOptions{SkipUsers: true, SkipQuotas: true, REST: r})
		require.Error(t, err, status)
		require.Contains(t, err.Error(), "Unknown", status)
	}
}

func TestImportMirrorSyncedConfigsNotPartial(t *testing.T) {
	k, r := mirrorFixture(t, "ACTIVE", map[string]string{"retention.ms": "604800000", "cleanup.policy": "delete"},
		map[string]string{"acl.sync.enable": "false", "topic.config.sync.include": "retention.ms,cleanup.*"}, nil)
	res := importWithREST(t, k, r)

	require.Len(t, res.Mirrors, 1)
	require.False(t, hasWarningPrefix(res.Warnings, "MirrorConfigNotImported"), "%v", res.Warnings)
	require.False(t, res.Partial)
}

func TestImportMirrorLocalConfigStillWarned(t *testing.T) {
	k, r := mirrorFixture(t, "ACTIVE", map[string]string{"retention.ms": "604800000", "min.insync.replicas": "2"},
		map[string]string{"acl.sync.enable": "false", "topic.config.sync.include": "retention.ms"}, nil)
	res := importWithREST(t, k, r)

	require.Contains(t, res.Warnings,
		`MirrorConfigNotImported: mirror topic "payments.orders" has 1 explicitly set configs; declare reviewed local keys manually`)
	require.True(t, res.Partial)
}
