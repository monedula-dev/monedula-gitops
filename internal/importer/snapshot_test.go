package importer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// avro bodies used across gatherSchemas tests. Named so test cases can refer to
// the embedded record full name without repeating the literal.

// avroOrderCreated is the body for record com.acme.OrderCreated.
const avroOrderCreated = `{"type":"record","namespace":"com.acme","name":"OrderCreated","fields":[]}`

// avroOrder is the body for record com.acme.Order.
const avroOrder = `{"type":"record","namespace":"com.acme","name":"Order","fields":[]}`

// avroRecordA is the body for record com.acme.A.
const avroRecordA = `{"type":"record","namespace":"com.acme","name":"A","fields":[]}`

// avroRecordB is the body for record com.acme.B.
const avroRecordB = `{"type":"record","namespace":"com.acme","name":"B","fields":[]}`

// avroLegacy is the body for record com.legacy.Payload — record name that does
// NOT match subject "legacy-thing" and does NOT suffix-match topic "legacy-thing"
// for any topic in the test, so it falls through to generic-unmatched.
const avroLegacy = `{"type":"record","namespace":"com.legacy","name":"Payload","fields":[]}`

// avroValue is a record whose name is literally "value" — exercises the
// TopicName-beats-TopicRecordName precedence case.
const avroValue = `{"type":"record","name":"value","fields":[]}`

func TestReadSnapshotSortsAndFiltersConfig(t *testing.T) {
	ctx := context.Background()
	c := mock.New([]kafka.TopicState{
		{Name: "b.x", Partitions: 3, ReplicationFactor: 2, Config: map[string]string{"retention.ms": "1"}},
		{Name: "a.y", Partitions: 1, ReplicationFactor: 1},
		{Name: "__consumer_offsets", Partitions: 50, ReplicationFactor: 3, Config: map[string]string{"cleanup.policy": "compact"}},
	}, nil)

	snap, err := ReadSnapshot(ctx, c, nil, nil, nil)
	require.NoError(t, err)

	require.Len(t, snap.Topics, 2, "internal topic must be excluded")
	require.Equal(t, "a.y", snap.Topics[0].Name)
	require.Equal(t, "b.x", snap.Topics[1].Name)

	require.Empty(t, snap.Topics[0].Config, "a.y has no explicit config")

	require.Equal(t, map[string]string{"retention.ms": "1"}, snap.Topics[1].Config)
	require.Equal(t, 3, snap.Topics[1].Partitions)
	require.Equal(t, 2, snap.Topics[1].ReplicationFactor)

	for _, tp := range snap.Topics {
		require.NotEqual(t, "__consumer_offsets", tp.Name)
	}
}

// TestReadSnapshotFiltersConfluentInternalTopics verifies the M9 fix: by
// default, import excludes not just "__"-prefixed Kafka topics but also
// Confluent Platform housekeeping topics ("_schemas", "_confluent*") that the
// old filter let through as ordinary manifests.
func TestReadSnapshotFiltersConfluentInternalTopics(t *testing.T) {
	ctx := context.Background()
	c := mock.New([]kafka.TopicState{
		{Name: "orders", Partitions: 3, ReplicationFactor: 2},
		{Name: "__consumer_offsets", Partitions: 50, ReplicationFactor: 3},
		{Name: "_schemas", Partitions: 1, ReplicationFactor: 3},
		{Name: "_confluent-monitoring", Partitions: 12, ReplicationFactor: 3},
		{Name: "_confluent-command", Partitions: 1, ReplicationFactor: 1},
		{Name: "_confluent_balancer_api_state", Partitions: 1, ReplicationFactor: 1},
	}, nil)

	snap, err := ReadSnapshot(ctx, c, nil, nil, nil)
	require.NoError(t, err)

	require.Len(t, snap.Topics, 1, "only the application topic must survive the default filter")
	require.Equal(t, "orders", snap.Topics[0].Name)
}

// TestReadSnapshotIncludeInternalOverridesFilter verifies
// SnapshotOptions.IncludeInternal (the --include-internal CLI flag) disables
// the default internal-topic filter entirely, importing housekeeping topics
// like any other topic.
func TestReadSnapshotIncludeInternalOverridesFilter(t *testing.T) {
	ctx := context.Background()
	c := mock.New([]kafka.TopicState{
		{Name: "orders", Partitions: 3, ReplicationFactor: 2},
		{Name: "__consumer_offsets", Partitions: 50, ReplicationFactor: 3},
		{Name: "_schemas", Partitions: 1, ReplicationFactor: 3},
		{Name: "_confluent-monitoring", Partitions: 12, ReplicationFactor: 3},
	}, nil)

	snap, err := ReadSnapshot(ctx, c, nil, nil, nil, SnapshotOptions{IncludeInternal: true})
	require.NoError(t, err)

	require.Len(t, snap.Topics, 4, "--include-internal must import housekeeping topics too")
	names := make([]string, len(snap.Topics))
	for i, tp := range snap.Topics {
		names[i] = tp.Name
	}
	require.ElementsMatch(t, []string{"orders", "__consumer_offsets", "_schemas", "_confluent-monitoring"}, names)
}

// TestIsInternalTopic pins the exact patterns isInternalTopic recognizes and
// the ones it must NOT match (application topics that merely start with an
// underscore or contain "confluent" mid-name must stay importable).
func TestIsInternalTopic(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"__consumer_offsets", true},
		{"__transaction_state", true},
		{"__connect-configs", true},
		{"_schemas", true},
		{"_confluent-monitoring", true},
		{"_confluent-command", true},
		{"_confluent-telemetry-metrics", true},
		{"_confluent_balancer_api_state", true},
		{"orders", false},
		{"payments.orders", false},
		{"_myapp-internal", false},   // single-underscore, NOT _schemas/_confluent*
		{"myconfluent-topic", false}, // contains "confluent" but no leading "_"
		{"_schemas-backup", false},   // must be exactly "_schemas", not a prefix match
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isInternalTopic(tc.name))
		})
	}
}

func TestReadSnapshotSortsACLs(t *testing.T) {
	ctx := context.Background()
	acl := func(principal, op string) kafka.ACLState {
		return kafka.ACLState{
			Principal:    principal,
			Host:         "*",
			ResourceType: "TOPIC",
			ResourceName: "payments.orders",
			PatternType:  "LITERAL",
			Operation:    op,
			Permission:   "ALLOW",
		}
	}
	// Seeded in unsorted order.
	c := mock.New(nil, []kafka.ACLState{
		acl("User:c", "WRITE"),
		acl("User:a", "WRITE"),
		acl("User:a", "READ"),
		acl("User:b", "READ"),
	})

	snap, err := ReadSnapshot(ctx, c, nil, nil, nil)
	require.NoError(t, err)

	require.Len(t, snap.ACLs, 4)
	want := []kafka.ACLState{
		acl("User:a", "READ"),
		acl("User:a", "WRITE"),
		acl("User:b", "READ"),
		acl("User:c", "WRITE"),
	}
	require.Equal(t, want, snap.ACLs)
}

func TestSnapshotGathersSchemas(t *testing.T) {
	ctx := context.Background()
	c := mock.New([]kafka.TopicState{
		{Name: "payments.orders", Partitions: 1, ReplicationFactor: 1},
		{Name: "search.idx", Partitions: 1, ReplicationFactor: 1},
	}, nil)

	sr := schemamock.New()
	body := `{"type":"record","name":"Order","fields":[]}`
	if _, err := sr.RegisterSchema(ctx, "payments.orders-value", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: body}); err != nil {
		t.Fatal(err)
	}
	if err := sr.SetCompatibility(ctx, "payments.orders-value", "BACKWARD"); err != nil {
		t.Fatal(err)
	}
	if _, err := sr.RegisterSchema(ctx, "payments.orders-key", schemaregistry.Schema{Type: schemaregistry.JSON, Definition: `{"type":"string"}`}); err != nil {
		t.Fatal(err)
	}
	// Unmatched: no imported topic "ghost".
	if _, err := sr.RegisterSchema(ctx, "ghost-value", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: body}); err != nil {
		t.Fatal(err)
	}

	snap, err := ReadSnapshot(ctx, c, sr, nil, nil)
	require.NoError(t, err)

	ts, ok := snap.Schemas["payments.orders"]
	require.True(t, ok, "payments.orders schemas must be present")
	require.NotNil(t, ts.Value)
	require.Equal(t, schemaregistry.AVRO, ts.Value.Schema.Type)
	require.Equal(t, "BACKWARD", ts.Value.Compatibility)
	require.NotNil(t, ts.Key)
	require.Equal(t, schemaregistry.JSON, ts.Key.Schema.Type)

	// search.idx has no subjects.
	_, hasSearch := snap.Schemas["search.idx"]
	require.False(t, hasSearch)

	require.Equal(t, []string{"ghost-value"}, snap.UnmatchedSubjects)
}

// seedSubject is a helper that registers a schema and sets compatibility in the
// mock, fatally failing the test if either call returns an error.
func seedSubject(t *testing.T, sr *schemamock.Client, subject string, schema schemaregistry.Schema, compat string) {
	t.Helper()
	ctx := context.Background()
	if _, err := sr.RegisterSchema(ctx, subject, schema); err != nil {
		t.Fatalf("seed RegisterSchema %q: %v", subject, err)
	}
	if compat != "" {
		if err := sr.SetCompatibility(ctx, subject, compat); err != nil {
			t.Fatalf("seed SetCompatibility %q: %v", subject, err)
		}
	}
}

// TestGatherSchemasTopicName checks the existing (regression) TopicName path:
// subject "<topic>-value" must be matched to the topic and Strategy "TopicName".
func TestGatherSchemasTopicName(t *testing.T) {
	ctx := context.Background()
	sr := schemamock.New()
	body := `{"type":"record","name":"Order","fields":[]}`
	seedSubject(t, sr, "orders-value", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: body}, "BACKWARD")

	topics := []TopicSnapshot{{Name: "orders", Partitions: 1, ReplicationFactor: 1}}
	result, err := gatherSchemas(ctx, sr, topics)
	require.NoError(t, err)

	ts, ok := result.Schemas["orders"]
	require.True(t, ok, "orders must be present in Schemas")
	require.NotNil(t, ts.Value)
	require.Equal(t, "BACKWARD", ts.Value.Compatibility)
	require.Equal(t, "TopicName", ts.ValueStrategy)
	require.Nil(t, ts.Key)
	require.Empty(t, result.RecordName)
	require.Empty(t, result.Unmatched)
	require.Empty(t, result.Ambiguities)
}

// TestGatherSchemasTopicRecordName checks that subject "<topic>-<recordFullName>"
// whose body's record full name is <recordFullName> is matched to the topic with
// Strategy "TopicRecordName".
func TestGatherSchemasTopicRecordName(t *testing.T) {
	ctx := context.Background()
	sr := schemamock.New()
	// Subject: orders-com.acme.OrderCreated; record full name in body: com.acme.OrderCreated
	seedSubject(t, sr, "orders-com.acme.OrderCreated", schemaregistry.Schema{
		Type:       schemaregistry.AVRO,
		Definition: avroOrderCreated,
	}, "FULL")

	topics := []TopicSnapshot{{Name: "orders", Partitions: 1, ReplicationFactor: 1}}
	result, err := gatherSchemas(ctx, sr, topics)
	require.NoError(t, err)

	ts, ok := result.Schemas["orders"]
	require.True(t, ok, "orders must be present in Schemas")
	require.NotNil(t, ts.Value)
	require.Equal(t, "FULL", ts.Value.Compatibility)
	require.Equal(t, "TopicRecordName", ts.ValueStrategy)
	require.Nil(t, ts.Key)
	require.Empty(t, result.RecordName, "must not appear as RecordName subject")
	require.Empty(t, result.Unmatched, "must not appear as unmatched")
	require.Empty(t, result.Ambiguities)
}

// TestGatherSchemasRecordName checks that a subject whose name equals its body's
// record full name is classified as RecordName (report-only, not attributed to a
// topic).
func TestGatherSchemasRecordName(t *testing.T) {
	ctx := context.Background()
	sr := schemamock.New()
	// subject == record full name => RecordName strategy
	seedSubject(t, sr, "com.acme.Order", schemaregistry.Schema{
		Type:       schemaregistry.AVRO,
		Definition: avroOrder,
	}, "BACKWARD")

	topics := []TopicSnapshot{{Name: "payments", Partitions: 1, ReplicationFactor: 1}}
	result, err := gatherSchemas(ctx, sr, topics)
	require.NoError(t, err)

	require.Empty(t, result.Schemas, "must not be attributed to any topic")
	require.Empty(t, result.Unmatched)
	require.Empty(t, result.Ambiguities)
	require.Len(t, result.RecordName, 1)
	require.Equal(t, RecordNameSubject{
		Subject:    "com.acme.Order",
		RecordName: "com.acme.Order",
		SchemaType: "AVRO",
	}, result.RecordName[0])
}

// TestGatherSchemasTopicNameBeatsTopicRecordName checks that when the subject is
// an exact TopicName match ("<topic>-value") it wins regardless of whether the
// body record name happens to produce a TopicRecordName match. Here the record
// name is literally "value" so orders-value could potentially match
// TopicRecordName("orders","value") = "orders-value" — but TopicName wins.
func TestGatherSchemasTopicNameBeatsTopicRecordName(t *testing.T) {
	ctx := context.Background()
	sr := schemamock.New()
	// record name "value", so TopicRecordName("orders","value") = "orders-value"
	// BUT TopicName("orders") also = "orders-value" — TopicName must win.
	seedSubject(t, sr, "orders-value", schemaregistry.Schema{
		Type:       schemaregistry.AVRO,
		Definition: avroValue,
	}, "NONE")

	topics := []TopicSnapshot{{Name: "orders", Partitions: 1, ReplicationFactor: 1}}
	result, err := gatherSchemas(ctx, sr, topics)
	require.NoError(t, err)

	ts, ok := result.Schemas["orders"]
	require.True(t, ok)
	require.NotNil(t, ts.Value)
	require.Equal(t, "TopicName", ts.ValueStrategy, "TopicName must take precedence over TopicRecordName")
	require.Empty(t, result.RecordName)
	require.Empty(t, result.Unmatched)
	require.Empty(t, result.Ambiguities)
}

// TestGatherSchemasAmbiguity checks that when two TopicRecordName subjects both
// map to the same topic's value slot, the first (sorted) wins and the second is
// recorded in Ambiguities.
func TestGatherSchemasAmbiguity(t *testing.T) {
	ctx := context.Background()
	sr := schemamock.New()
	// Two subjects both resolve to topic "orders" value via TopicRecordName.
	// Sorted: orders-com.acme.A < orders-com.acme.B => A wins.
	seedSubject(t, sr, "orders-com.acme.A", schemaregistry.Schema{
		Type:       schemaregistry.AVRO,
		Definition: avroRecordA,
	}, "BACKWARD")
	seedSubject(t, sr, "orders-com.acme.B", schemaregistry.Schema{
		Type:       schemaregistry.AVRO,
		Definition: avroRecordB,
	}, "BACKWARD")

	topics := []TopicSnapshot{{Name: "orders", Partitions: 1, ReplicationFactor: 1}}
	result, err := gatherSchemas(ctx, sr, topics)
	require.NoError(t, err)

	ts, ok := result.Schemas["orders"]
	require.True(t, ok, "orders must be present")
	require.NotNil(t, ts.Value)
	// orders-com.acme.A is first alphabetically and wins.
	require.Equal(t, "orders-com.acme.A", ts.Value.Subject)
	require.Equal(t, "TopicRecordName", ts.ValueStrategy)

	require.Len(t, result.Ambiguities, 1, "second subject must be recorded as an ambiguity")
	require.Contains(t, result.Ambiguities[0], "orders-com.acme.B", "ambiguity message must mention the skipped subject")
	require.Contains(t, result.Ambiguities[0], "orders-com.acme.A", "ambiguity message must mention the winning subject")

	require.Empty(t, result.Unmatched)
	require.Empty(t, result.RecordName)
}

// TestGatherSchemasGenericUnmatched checks that a subject whose record name
// doesn't match any known topic prefix and doesn't equal the subject itself
// falls through to Unmatched.
func TestGatherSchemasGenericUnmatched(t *testing.T) {
	ctx := context.Background()
	sr := schemamock.New()
	// "legacy-thing": record full name is "com.legacy.Payload" (not the subject,
	// not <topic>-<recordName> for any topic in the list).
	seedSubject(t, sr, "legacy-thing", schemaregistry.Schema{
		Type:       schemaregistry.AVRO,
		Definition: avroLegacy,
	}, "")

	topics := []TopicSnapshot{{Name: "orders", Partitions: 1, ReplicationFactor: 1}}
	result, err := gatherSchemas(ctx, sr, topics)
	require.NoError(t, err)

	require.Equal(t, []string{"legacy-thing"}, result.Unmatched)
	require.Empty(t, result.Schemas)
	require.Empty(t, result.RecordName)
	require.Empty(t, result.Ambiguities)
}

// TestGatherSchemasExtractError checks the Step 2 exit: a subject whose schema
// body contains no extractable record name (e.g. an Avro primitive body
// "\"string\"") causes recordname.Extract to return an error, and the subject
// must land in Unmatched — not in RecordName or Schemas.
func TestGatherSchemasExtractError(t *testing.T) {
	ctx := context.Background()
	sr := schemamock.New()
	// Avro primitive body — not a named record, so recordname.Extract will error.
	seedSubject(t, sr, "raw-events-value", schemaregistry.Schema{
		Type:       schemaregistry.AVRO,
		Definition: `"string"`,
	}, "")

	topics := []TopicSnapshot{{Name: "other-topic", Partitions: 1, ReplicationFactor: 1}}
	result, err := gatherSchemas(ctx, sr, topics)
	require.NoError(t, err)

	require.Equal(t, []string{"raw-events-value"}, result.Unmatched,
		"primitive-body subject must land in Unmatched when Extract errors")
	require.Empty(t, result.Schemas)
	require.Empty(t, result.RecordName)
	require.Empty(t, result.Ambiguities)
}

// TestGatherSchemasTopicNameOverridesTopicRecordName_SurfacesDisplaced verifies
// Fix 2: when a TopicRecordName subject (e.g. orders-com.acme.Order) sorts
// before a TopicName value subject (orders-value) and fills the value slot first,
// then orders-value is processed by Step 1 (TopicName) and OVERWRITES the slot.
// TopicName correctly wins (precedence preserved), but the displaced
// record-based subject must appear in Ambiguities (not silently dropped).
//
// Sort order: "orders-com.acme.Order" < "orders-value" because "com.acme.Order"
// < "value" lexicographically, so the record-based subject is always processed
// first when both are present.
func TestGatherSchemasTopicNameOverridesTopicRecordName_SurfacesDisplaced(t *testing.T) {
	ctx := context.Background()
	sr := schemamock.New()

	// TopicRecordName subject — sorts before orders-value.
	seedSubject(t, sr, "orders-com.acme.Order", schemaregistry.Schema{
		Type:       schemaregistry.AVRO,
		Definition: avroOrder,
	}, "BACKWARD")
	// TopicName subject — sorts after, but must win on precedence.
	seedSubject(t, sr, "orders-value", schemaregistry.Schema{
		Type:       schemaregistry.AVRO,
		Definition: avroOrderCreated, // body content doesn't matter for this test
	}, "NONE")

	topics := []TopicSnapshot{{Name: "orders", Partitions: 1, ReplicationFactor: 1}}
	result, err := gatherSchemas(ctx, sr, topics)
	require.NoError(t, err)

	ts, ok := result.Schemas["orders"]
	require.True(t, ok, "orders must be present in Schemas")
	require.NotNil(t, ts.Value)

	// TopicName wins: the winning subject must be orders-value.
	require.Equal(t, "TopicName", ts.ValueStrategy, "TopicName must win over TopicRecordName")
	require.Equal(t, "orders-value", ts.Value.Subject, "orders-value must be the attributed value subject")

	// The displaced TopicRecordName subject must appear in Ambiguities.
	require.Len(t, result.Ambiguities, 1, "displaced record-based subject must appear in Ambiguities")
	require.Contains(t, result.Ambiguities[0], "orders-com.acme.Order", "ambiguity must name the displaced subject")
	require.Contains(t, result.Ambiguities[0], "orders-value", "ambiguity must name the TopicName subject")

	require.Empty(t, result.Unmatched)
	require.Empty(t, result.RecordName)
}

// TestGatherSchemasDeterminism checks that the results are sorted when multiple
// entries of the same classification are present.
func TestGatherSchemasDeterminism(t *testing.T) {
	ctx := context.Background()
	sr := schemamock.New()

	// Two RecordName subjects.
	seedSubject(t, sr, "com.acme.Order", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroOrder}, "")
	seedSubject(t, sr, "com.acme.OrderCreated", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroOrderCreated}, "")

	// Two unmatched subjects.
	legacy1 := `{"type":"record","namespace":"com.x","name":"Foo","fields":[]}`
	legacy2 := `{"type":"record","namespace":"com.x","name":"Bar","fields":[]}`
	seedSubject(t, sr, "unmatched-alpha", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: legacy1}, "")
	seedSubject(t, sr, "unmatched-beta", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: legacy2}, "")

	topics := []TopicSnapshot{{Name: "payments", Partitions: 1, ReplicationFactor: 1}}
	result, err := gatherSchemas(ctx, sr, topics)
	require.NoError(t, err)

	// RecordName subjects must be sorted by Subject.
	require.Len(t, result.RecordName, 2)
	require.Equal(t, "com.acme.Order", result.RecordName[0].Subject)
	require.Equal(t, "com.acme.OrderCreated", result.RecordName[1].Subject)

	// Unmatched must be sorted.
	require.Equal(t, []string{"unmatched-alpha", "unmatched-beta"}, result.Unmatched)
}

// TestReadSnapshotReadsRoleBindings exercises the global sort and dedup logic in
// ReadSnapshot. Two distinct MDS scopes are used:
//
//   - kafkaScope  (Type:"kafka",           KafkaCluster:"kid", SubCluster:"")
//   - srScope     (Type:"schema-registry", KafkaCluster:"kid", SubCluster:"sr")
//
// Seeding strategy (mock filters by scope):
//   - rbZ is added under kafkaScope  — its Key sorts LATER ("User:z|…")
//   - rbA is added under srScope     — its Key sorts EARLIER ("User:a|…")
//
// The scope list passed to ReadSnapshot is [kafkaScope, srScope, kafkaScope]:
// the third entry repeats kafkaScope so that rbZ appears in the results of two
// separate ListRoleBindings calls, exercising the dedup path (the second
// occurrence must be discarded). Combined unsorted results before dedup/sort:
// [rbZ(kafka), rbA(sr), rbZ(kafka-repeat)]. After dedup: [rbZ, rbA]. After
// outer sort.Slice: [rbA, rbZ].
//
// Removing the outer sort.Slice would leave [rbZ, rbA], failing the "rbA first"
// assertion. Removing the dedup would yield len==3, failing the length assertion.
func TestReadSnapshotReadsRoleBindings(t *testing.T) {
	ctx := context.Background()
	kc := mock.New([]kafka.TopicState{
		{Name: "orders", Partitions: 1, ReplicationFactor: 1},
	}, nil)

	kafkaScope := mds.Scope{Type: "kafka", KafkaCluster: "kid", SubCluster: ""}
	srScope := mds.Scope{Type: "schema-registry", KafkaCluster: "kid", SubCluster: "sr"}

	// rbZ: kafka scope, key is "User:z|SystemAdmin|kafka|kid||" — sorts LATER.
	rbZ := mds.RoleBinding{Principal: "User:z", Role: "SystemAdmin", Scope: kafkaScope}
	// rbA: SR scope, key is "User:a|DeveloperRead|schema-registry|kid|sr|" — sorts EARLIER.
	rbA := mds.RoleBinding{Principal: "User:a", Role: "DeveloperRead", Scope: srScope}

	mdsMock := mdsmock.New(rbZ, rbA)

	// Query kafka twice and sr once: [kafkaScope, srScope, kafkaScope].
	// ListRoleBindings(kafkaScope) → [rbZ]
	// ListRoleBindings(srScope)    → [rbA]
	// ListRoleBindings(kafkaScope) → [rbZ] (duplicate, must be deduped)
	scopes := []mds.Scope{kafkaScope, srScope, kafkaScope}

	snap, err := ReadSnapshot(ctx, kc, nil, mdsMock, scopes)
	require.NoError(t, err)

	// Dedup: rbZ seen in calls 1 and 3 — must appear exactly once.
	require.Len(t, snap.RoleBindings, 2, "dedup must eliminate the repeated kafkaScope binding")

	// Sort: global sort across scopes — rbA (key starts "User:a") < rbZ (key starts "User:z").
	require.Equal(t, "User:a", snap.RoleBindings[0].Principal, "globally sorted by Key: rbA must be first")
	require.Equal(t, "User:z", snap.RoleBindings[1].Principal, "globally sorted by Key: rbZ must be second")
}

// TestScopesFromMDSConfig verifies ScopesFromMDSConfig for all combinations of
// configured cluster IDs.
func TestScopesFromMDSConfig(t *testing.T) {
	const kid = "lkc-abc"
	const srID = "lsrc-001"
	const connID = "connect-001"
	const ksqlID = "ksql-001"

	tests := []struct {
		name string
		cfg  *v1alpha1.MDSConfig
		want []mds.Scope
	}{
		{
			name: "nil config returns nil",
			cfg:  nil,
			want: nil,
		},
		{
			name: "kafka-only returns one scope",
			cfg: &v1alpha1.MDSConfig{
				Clusters: v1alpha1.MDSClusters{KafkaCluster: kid},
			},
			want: []mds.Scope{
				{Type: "kafka", KafkaCluster: kid},
			},
		},
		{
			name: "kafka+SR returns two scopes",
			cfg: &v1alpha1.MDSConfig{
				Clusters: v1alpha1.MDSClusters{
					KafkaCluster:          kid,
					SchemaRegistryCluster: srID,
				},
			},
			want: []mds.Scope{
				{Type: "kafka", KafkaCluster: kid},
				{Type: "schema-registry", KafkaCluster: kid, SubCluster: srID},
			},
		},
		{
			name: "kafka+connect returns two scopes",
			cfg: &v1alpha1.MDSConfig{
				Clusters: v1alpha1.MDSClusters{
					KafkaCluster:   kid,
					ConnectCluster: connID,
				},
			},
			want: []mds.Scope{
				{Type: "kafka", KafkaCluster: kid},
				{Type: "connect", KafkaCluster: kid, SubCluster: connID},
			},
		},
		{
			name: "all four clusters returns four scopes in order kafka,schema-registry,connect,ksql",
			cfg: &v1alpha1.MDSConfig{
				Clusters: v1alpha1.MDSClusters{
					KafkaCluster:          kid,
					SchemaRegistryCluster: srID,
					ConnectCluster:        connID,
					KsqlCluster:           ksqlID,
				},
			},
			want: []mds.Scope{
				{Type: "kafka", KafkaCluster: kid},
				{Type: "schema-registry", KafkaCluster: kid, SubCluster: srID},
				{Type: "connect", KafkaCluster: kid, SubCluster: connID},
				{Type: "ksql", KafkaCluster: kid, SubCluster: ksqlID},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ScopesFromMDSConfig(tc.cfg)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestReadSnapshotNilMDSSkipsRoleBindings(t *testing.T) {
	ctx := context.Background()
	kc := mock.New([]kafka.TopicState{
		{Name: "orders", Partitions: 1, ReplicationFactor: 1},
	}, nil)
	snap, err := ReadSnapshot(ctx, kc, nil, nil, nil)
	require.NoError(t, err)
	require.Empty(t, snap.RoleBindings)
}
