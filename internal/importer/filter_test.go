package importer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

func TestTopicFilterMatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		f       TopicFilter
		topic   string
		matches bool
	}{
		{"zero filter matches everything", TopicFilter{}, "anything", true},
		{"include hit", TopicFilter{Include: []string{"payments.*"}}, "payments.orders", true},
		{"include miss", TopicFilter{Include: []string{"payments.*"}}, "search.idx", false},
		{"any include suffices", TopicFilter{Include: []string{"a.*", "b.*"}}, "b.x", true},
		{"exclude only", TopicFilter{Exclude: []string{"tmp.*"}}, "tmp.x", false},
		{"exclude only passes others", TopicFilter{Exclude: []string{"tmp.*"}}, "orders", true},
		{"exclude beats include", TopicFilter{Include: []string{"payments.*"}, Exclude: []string{"payments.tmp*"}}, "payments.tmp1", false},
		{"star does not stop at dots", TopicFilter{Include: []string{"payments*"}}, "payments.eu.orders", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.matches, tc.f.Match(tc.topic))
		})
	}
}

func TestTopicFilterValidate(t *testing.T) {
	require.NoError(t, TopicFilter{Include: []string{"a.*", "b?"}, Exclude: []string{"[ab]*"}}.Validate())
	require.Error(t, TopicFilter{Include: []string{"a["}}.Validate())
	require.Error(t, TopicFilter{Exclude: []string{"["}}.Validate())
	require.Error(t, TopicFilter{Include: []string{""}}.Validate())
}

func TestReadSnapshotTopicFilter(t *testing.T) {
	ctx := context.Background()
	c := mock.New([]kafka.TopicState{
		{Name: "payments.orders", Partitions: 1, ReplicationFactor: 1},
		{Name: "payments.tmp", Partitions: 1, ReplicationFactor: 1},
		{Name: "search.idx", Partitions: 1, ReplicationFactor: 1},
		{Name: "__consumer_offsets", Partitions: 50, ReplicationFactor: 3},
	}, nil)

	snap, err := ReadSnapshot(ctx, c, nil, nil, nil, SnapshotOptions{
		TopicFilter: TopicFilter{Include: []string{"payments.*"}, Exclude: []string{"payments.tmp"}},
	})
	require.NoError(t, err)
	require.Len(t, snap.Topics, 1)
	require.Equal(t, "payments.orders", snap.Topics[0].Name)
	// Internal topics are excluded by their own rule and are not counted as
	// filtered: the count answers "what did --topic/--exclude-topic leave out".
	require.Equal(t, 2, snap.Filtered.Topics)
}

func TestReadSnapshotTopicFilterACLs(t *testing.T) {
	ctx := context.Background()
	acl := func(principal, rtype, name, pattern, op string) kafka.ACLState {
		return kafka.ACLState{Principal: principal, Host: "*", ResourceType: rtype,
			ResourceName: name, PatternType: pattern, Operation: op, Permission: "ALLOW"}
	}
	keepTopic := acl("User:pay", "TOPIC", "payments.orders", "LITERAL", "READ")
	keepPrefixed := acl("User:pay", "TOPIC", "payments.", "PREFIXED", "WRITE")
	keepGroup := acl("User:pay", "GROUP", "pay-consumers", "LITERAL", "READ")
	dropTopic := acl("User:search", "TOPIC", "search.idx", "LITERAL", "READ")
	dropGroup := acl("User:search", "GROUP", "search-consumers", "LITERAL", "READ")
	dropWildcard := acl("User:admin", "TOPIC", "*", "LITERAL", "ALL")
	dropCluster := acl("User:admin", "CLUSTER", "kafka-cluster", "LITERAL", "ALTER")
	dropShortPrefix := acl("User:other", "TOPIC", "pay", "PREFIXED", "READ")

	c := mock.New([]kafka.TopicState{
		{Name: "payments.orders", Partitions: 1, ReplicationFactor: 1},
		{Name: "search.idx", Partitions: 1, ReplicationFactor: 1},
	}, []kafka.ACLState{keepTopic, keepPrefixed, keepGroup, dropTopic, dropGroup, dropWildcard, dropCluster, dropShortPrefix})

	snap, err := ReadSnapshot(ctx, c, nil, nil, nil, SnapshotOptions{
		TopicFilter: TopicFilter{Include: []string{"payments.*"}},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []kafka.ACLState{keepTopic, keepPrefixed, keepGroup}, snap.ACLs)
	require.Equal(t, 5, snap.Filtered.ACLs)
}

func TestReadSnapshotNoTopicFilterKeepsAllACLs(t *testing.T) {
	ctx := context.Background()
	acls := []kafka.ACLState{
		{Principal: "User:admin", Host: "*", ResourceType: "CLUSTER", ResourceName: "kafka-cluster", PatternType: "LITERAL", Operation: "ALTER", Permission: "ALLOW"},
		{Principal: "User:x", Host: "*", ResourceType: "TOPIC", ResourceName: "x", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
	}
	c := mock.New(nil, acls)
	snap, err := ReadSnapshot(ctx, c, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, snap.ACLs, 2)
	require.Zero(t, snap.Filtered)
}

func TestReadSnapshotTopicFilterRoleBindings(t *testing.T) {
	ctx := context.Background()
	scope := mds.Scope{Type: "kafka", KafkaCluster: "kid"}
	topicRB := func(principal, role, name, pattern string) mds.RoleBinding {
		return mds.RoleBinding{Principal: principal, Role: role, Scope: scope,
			Resource: &mds.ResourcePattern{Type: "Topic", Name: name, PatternType: pattern}}
	}
	keepTopic := topicRB("User:pay", "DeveloperRead", "payments.orders", "literal")
	keepGroup := mds.RoleBinding{Principal: "User:pay", Role: "DeveloperRead", Scope: scope,
		Resource: &mds.ResourcePattern{Type: "Group", Name: "pay-consumers", PatternType: "literal"}}
	dropTopic := topicRB("User:search", "DeveloperRead", "search.idx", "literal")
	dropCluster := mds.RoleBinding{Principal: "User:admin", Role: "SystemAdmin", Scope: scope}

	kc := mock.New([]kafka.TopicState{{Name: "payments.orders", Partitions: 1, ReplicationFactor: 1}}, nil)
	mm := mdsmock.New(keepTopic, keepGroup, dropTopic, dropCluster)

	snap, err := ReadSnapshot(ctx, kc, nil, mm, []mds.Scope{scope}, SnapshotOptions{
		TopicFilter: TopicFilter{Include: []string{"payments.*"}},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []mds.RoleBinding{keepTopic, keepGroup}, snap.RoleBindings)
	require.Equal(t, 2, snap.Filtered.RoleBindings)
}

func TestReadSnapshotTopicFilterSchemas(t *testing.T) {
	ctx := context.Background()
	c := mock.New([]kafka.TopicState{
		{Name: "payments.orders", Partitions: 1, ReplicationFactor: 1},
		{Name: "search.idx", Partitions: 1, ReplicationFactor: 1},
	}, nil)
	sr := schemamock.New()
	avro := schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroOrder}
	seedSubject(t, sr, "payments.orders-value", avro, "")
	// Subjects owned by the excluded topic: TopicName and TopicRecordName.
	seedSubject(t, sr, "search.idx-value", avro, "")
	seedSubject(t, sr, "search.idx-com.acme.Order", avro, "")
	// Owned by no topic at all: still reported.
	seedSubject(t, sr, "ghost-value", avro, "")

	snap, err := ReadSnapshot(ctx, c, sr, nil, nil, SnapshotOptions{
		TopicFilter: TopicFilter{Include: []string{"payments.*"}},
	})
	require.NoError(t, err)
	require.Contains(t, snap.Schemas, "payments.orders")
	require.NotContains(t, snap.Schemas, "search.idx")
	require.Equal(t, []string{"ghost-value"}, snap.UnmatchedSubjects,
		"subjects of filtered-out topics must not surface as unmatched")
	require.Equal(t, 2, snap.Filtered.Subjects)
}

func TestReadSnapshotTopicFilterKeepsSubjectOfLongerIncludedTopic(t *testing.T) {
	ctx := context.Background()
	// "orders" is filtered out but "orders-eu" is imported: the subject
	// "orders-eu-com.acme.Order" starts with "orders-" yet belongs to the
	// imported topic and must still be attributed to it.
	c := mock.New([]kafka.TopicState{
		{Name: "orders", Partitions: 1, ReplicationFactor: 1},
		{Name: "orders-eu", Partitions: 1, ReplicationFactor: 1},
	}, nil)
	sr := schemamock.New()
	seedSubject(t, sr, "orders-eu-com.acme.Order", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroOrder}, "")

	snap, err := ReadSnapshot(ctx, c, sr, nil, nil, SnapshotOptions{
		TopicFilter: TopicFilter{Include: []string{"orders-eu"}},
	})
	require.NoError(t, err)
	ts, ok := snap.Schemas["orders-eu"]
	require.True(t, ok)
	require.NotNil(t, ts.Value)
	require.Equal(t, "TopicRecordName", ts.ValueStrategy)
	require.Zero(t, snap.Filtered.Subjects)
}
