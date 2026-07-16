package importer

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry/recordname"
)

// avroZebra is the body for record zebra.Z. Its TopicRecordName subject for
// topic "orders" is "orders-zebra.Z", which sorts AFTER "orders-key" — the
// opposite ordering of avroOrder ("orders-com.acme.Order" sorts BEFORE
// "orders-key"). The two orderings exercise both directions of the historical
// single-Strategy overwrite bug.
const avroZebra = `{"type":"record","namespace":"zebra","name":"Z","fields":[]}`

// RequireNoMutatingSchemaBlocks asserts the schema round-trip invariant on a
// built Result: every topic manifest that carries a spec.schema block must
// recompute — via recordname.Subjects, the exact computation the pipeline and
// the operator run at apply time — the very subjects attributed to the topic in
// the snapshot. Any mismatch means applying the import output would CREATE or
// overwrite a subject that differs from live state (mutate the registry).
//
// Exported (despite living in a _test.go file) so external test-package files
// in this package (e.g. validate_property_test.go, package importer_test) can
// reuse it instead of re-deriving the same check inline: Go compiles this
// file's _test.go identifiers into the "importer" package for the test
// binary, making exported names here visible to "importer_test" via the
// normal package import.
func RequireNoMutatingSchemaBlocks(t *testing.T, snap Snapshot, r Result) {
	t.Helper()
	for _, tp := range r.Topics {
		sc := tp.Spec.Schema
		if sc == nil {
			continue
		}
		s, ok := snap.Schemas[tp.Spec.TopicName]
		require.True(t, ok, "topic %q carries spec.schema but has no snapshot attribution", tp.Spec.TopicName)
		require.NotNil(t, s.Value, "topic %q carries spec.schema without a live value subject", tp.Spec.TopicName)

		var keyDef, wantKey string
		if s.Key != nil {
			keyDef = s.Key.Schema.Definition
			wantKey = s.Key.Subject
		}
		gotValue, gotKey, err := recordname.Subjects(sc.SubjectStrategy, tp.Spec.TopicName, sc, s.Value.Schema.Definition, keyDef)
		require.NoError(t, err, "topic %q: recomputing subjects from the emitted manifest", tp.Spec.TopicName)
		require.Equal(t, s.Value.Subject, gotValue,
			"topic %q: manifest recomputes a value subject that differs from live state", tp.Spec.TopicName)
		require.Equal(t, wantKey, gotKey,
			"topic %q: manifest recomputes a key subject that differs from live state", tp.Spec.TopicName)
	}
}

// TestGatherSchemasMixedStrategyPerSlot verifies the per-slot strategy fix in
// gatherSchemas for BOTH subject orderings: a TopicRecordName value subject and
// a TopicName key subject on the same topic must each keep their own detected
// strategy. Before the fix a single shared Strategy field was overwritten by
// whichever subject was processed last (subjects are processed in sorted
// order), corrupting the value attribution in one ordering and the key
// attribution in the other.
func TestGatherSchemasMixedStrategyPerSlot(t *testing.T) {
	cases := []struct {
		name         string
		valueSubject string
		valueBody    string
	}{
		// "orders-com.acme.Order" < "orders-key": the record-based subject fills
		// the value slot FIRST, then the TopicName key match must not flip it.
		{name: "record-subject-sorts-first", valueSubject: "orders-com.acme.Order", valueBody: avroOrder},
		// "orders-key" < "orders-zebra.Z": the TopicName key match lands FIRST,
		// then the record-based value match must not flip the key's strategy.
		{name: "key-subject-sorts-first", valueSubject: "orders-zebra.Z", valueBody: avroZebra},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			sr := schemamock.New()
			seedSubject(t, sr, tc.valueSubject, schemaregistry.Schema{
				Type:       schemaregistry.AVRO,
				Definition: tc.valueBody,
			}, "FULL")
			seedSubject(t, sr, "orders-key", schemaregistry.Schema{
				Type:       schemaregistry.AVRO,
				Definition: `"string"`,
			}, "")

			topics := []TopicSnapshot{{Name: "orders", Partitions: 1, ReplicationFactor: 1}}
			result, err := gatherSchemas(ctx, sr, topics)
			require.NoError(t, err)

			ts, ok := result.Schemas["orders"]
			require.True(t, ok, "orders must be present in Schemas")
			require.NotNil(t, ts.Value)
			require.Equal(t, tc.valueSubject, ts.Value.Subject)
			require.Equal(t, "TopicRecordName", ts.ValueStrategy,
				"value slot must keep its TopicRecordName attribution regardless of subject ordering")
			require.NotNil(t, ts.Key)
			require.Equal(t, "orders-key", ts.Key.Subject)
			require.Equal(t, "TopicName", ts.KeyStrategy,
				"key slot must keep its TopicName attribution regardless of subject ordering")

			require.Empty(t, result.Unmatched)
			require.Empty(t, result.RecordName)
			require.Empty(t, result.Ambiguities)
		})
	}
}

// TestBuildMixedStrategyFallsBackToExplicitFiles is the end-to-end proof for
// the mixed-strategy fix, run for BOTH subject orderings: gatherSchemas feeds
// Build, and the mixed topic must NOT receive a spec.schema block (no single
// subjectStrategy can reproduce both live subjects — applying one would create
// a subject that does not exist live). Instead, the schema bodies are emitted
// as explicit report-only files named after the live subjects, with a warning
// naming the topic and both strategies. The Result-wide round-trip invariant
// (no manifest recomputes subjects that differ from live state) must hold.
func TestBuildMixedStrategyFallsBackToExplicitFiles(t *testing.T) {
	cases := []struct {
		name         string
		valueSubject string
		valueBody    string
	}{
		{name: "record-subject-sorts-first", valueSubject: "orders-com.acme.Order", valueBody: avroOrder},
		{name: "key-subject-sorts-first", valueSubject: "orders-zebra.Z", valueBody: avroZebra},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			sr := schemamock.New()
			seedSubject(t, sr, tc.valueSubject, schemaregistry.Schema{
				Type:       schemaregistry.AVRO,
				Definition: tc.valueBody,
			}, "FULL")
			seedSubject(t, sr, "orders-key", schemaregistry.Schema{
				Type:       schemaregistry.AVRO,
				Definition: `"string"`,
			}, "")

			topics := []TopicSnapshot{{Name: "orders", Partitions: 1, ReplicationFactor: 1}}
			result, err := gatherSchemas(ctx, sr, topics)
			require.NoError(t, err)

			snap := Snapshot{Topics: topics, Schemas: result.Schemas}
			r := Build(snap, "prod", nil, nil)

			// No spec.schema block for the mixed topic.
			require.Len(t, r.Topics, 1)
			require.Nil(t, r.Topics[0].Spec.Schema,
				"mixed-strategy topic must not carry a spec.schema block")

			// Warning names the topic and both strategies.
			joined := strings.Join(r.Warnings, "\n")
			require.Contains(t, joined, `topic "orders"`)
			require.Contains(t, joined, "TopicRecordName")
			require.Contains(t, joined, "TopicName")
			require.Contains(t, joined, "different subject strategies")

			// Explicit files carry the live subject names and verbatim bodies.
			require.Len(t, r.SchemaFiles, 2)
			byBase := map[string]SchemaFile{}
			for _, f := range r.SchemaFiles {
				byBase[f.BaseName] = f
			}
			require.Contains(t, byBase, tc.valueSubject)
			require.Equal(t, tc.valueBody, byBase[tc.valueSubject].Content)
			require.Contains(t, byBase, "orders-key")
			require.Equal(t, `"string"`, byBase["orders-key"].Content)

			// The round-trip invariant holds across the whole Result.
			RequireNoMutatingSchemaBlocks(t, snap, r)
		})
	}
}

// TestVerifySchemaSubjectsCatchesCorruptedAttribution unit-tests the schema
// round-trip verify safety net: a deliberately corrupted attribution (the
// recorded live subject cannot be recomputed from the strategy + schema body)
// must clear spec.schema, replace the strategy-derived files with explicit
// live-subject-named files, and record a warning naming both subject sets.
func TestVerifySchemaSubjectsCatchesCorruptedAttribution(t *testing.T) {
	snap := Snapshot{
		Topics: []TopicSnapshot{{Name: "orders", Partitions: 1}},
		Schemas: map[string]TopicSchemas{
			"orders": {
				Value: &schemaregistry.SubjectState{
					// Corrupted: the body's record name is com.acme.Order, so
					// TopicRecordName recomputes "orders-com.acme.Order".
					Subject: "orders-WRONG",
					Schema:  schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroOrder},
				},
				ValueStrategy: "TopicRecordName",
			},
		},
	}
	tp := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec:       v1alpha1.KafkaTopicSpec{TopicName: "orders"},
	}
	topicByName := map[string]*v1alpha1.KafkaTopic{"orders": tp}

	var warnings []string
	files := applySchemas(snap, topicByName, &warnings)
	require.NotNil(t, tp.Spec.Schema, "applySchemas alone emits the (corrupt) block — the verify must catch it")
	require.Len(t, files, 1)
	require.Equal(t, "orders-value", files[0].BaseName)

	files = verifySchemaSubjects(snap, topicByName, files, &warnings)

	require.Nil(t, tp.Spec.Schema, "verify must clear the mutating spec.schema block")
	require.Len(t, files, 1, "strategy-derived file must be replaced with one explicit file")
	require.Equal(t, "orders-WRONG", files[0].BaseName, "explicit file must be named after the live subject")
	require.Equal(t, avroOrder, files[0].Content)

	joined := strings.Join(warnings, "\n")
	require.Contains(t, joined, "schema round-trip verify failed")
	require.Contains(t, joined, `"orders-com.acme.Order"`, "warning must name the recomputed subject")
	require.Contains(t, joined, `"orders-WRONG"`, "warning must name the live subject")
}

// TestVerifySchemaSubjectsFallbackWarnsUnknownTypeOnce is the regression test
// for the duplicate "unknown schema type" warning: a topic with BOTH an
// unrecognized schema type AND a corrupted attribution (forcing the
// verifySchemaSubjects fallback through explicitSchemaFiles) must produce
// exactly ONE unknown-type warning end-to-end, not two. Before the fix,
// applySchemas warned once via the strategy-derived "orders-value" name and
// verifySchemaSubjects's fallback warned again via the live "orders-WRONG"
// name for the very same subject.
//
// Strategy is TopicName (not TopicRecordName) so the corruption produces a
// genuine subject MISMATCH rather than a recordname extraction error: with
// TopicName the recomputed subject is always "orders-value" regardless of
// schema type, so an attribution of "orders-WRONG" cleanly exercises the
// mismatch branch of verifySchemaSubjects while keeping the unknown type as
// the sole reason to warn about schema type.
func TestVerifySchemaSubjectsFallbackWarnsUnknownTypeOnce(t *testing.T) {
	snap := Snapshot{
		Topics: []TopicSnapshot{{Name: "orders", Partitions: 1}},
		Schemas: map[string]TopicSchemas{
			"orders": {
				Value: &schemaregistry.SubjectState{
					// Corrupted (like TestVerifySchemaSubjectsCatchesCorruptedAttribution)
					// so verifySchemaSubjects falls back through explicitSchemaFiles...
					Subject: "orders-WRONG",
					// ...AND an unknown schema type, so the fallback path is the one
					// under test for the unknown-type warning.
					Schema: schemaregistry.Schema{Type: schemaregistry.SchemaType("WEIRD"), Definition: avroOrder},
				},
				ValueStrategy: "TopicName",
			},
		},
	}
	tp := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec:       v1alpha1.KafkaTopicSpec{TopicName: "orders"},
	}
	topicByName := map[string]*v1alpha1.KafkaTopic{"orders": tp}

	var warnings []string
	files := applySchemas(snap, topicByName, &warnings)
	files = verifySchemaSubjects(snap, topicByName, files, &warnings)

	require.Nil(t, tp.Spec.Schema, "verify must still clear the mutating spec.schema block")
	require.Len(t, files, 1)
	require.Equal(t, "orders-WRONG", files[0].BaseName)
	require.Equal(t, "txt", files[0].Ext, "unknown type still writes as .txt")

	count := 0
	for _, w := range warnings {
		if strings.Contains(w, "unknown schema type") {
			count++
		}
	}
	require.Equal(t, 1, count, "unknown-type warning must appear exactly once end-to-end; got warnings: %v", warnings)

	// The single warning is emitted by applySchemas, at the point the file is
	// first built and written under its strategy-derived name ("orders-value").
	// verifySchemaSubjects's later fallback (warn=false) discards that file and
	// re-emits the body under the live subject name ("orders-WRONG") without
	// warning again — so the warning names the ORIGINAL file, not the final one.
	joined := strings.Join(warnings, "\n")
	require.Contains(t, joined, `subject "orders-value" has unknown schema type "WEIRD"; writing as .txt`,
		"the single warning must name the file applySchemas actually built and warned about")
}

// TestVerifySchemaSubjectsCleanPassThrough verifies the verify is a no-op for
// faithful attributions: homogeneous TopicName value+key and a TopicRecordName
// value-only topic keep their spec.schema blocks and files untouched.
func TestVerifySchemaSubjectsCleanPassThrough(t *testing.T) {
	snap := Snapshot{
		Topics: []TopicSnapshot{{Name: "events", Partitions: 1}, {Name: "orders", Partitions: 1}},
		Schemas: map[string]TopicSchemas{
			"events": {
				Value: &schemaregistry.SubjectState{
					Subject: "events-value",
					Schema:  schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroOrderCreated},
				},
				ValueStrategy: "TopicName",
				Key: &schemaregistry.SubjectState{
					Subject: "events-key",
					Schema:  schemaregistry.Schema{Type: schemaregistry.JSON, Definition: `{"type":"string"}`},
				},
				KeyStrategy: "TopicName",
			},
			"orders": {
				Value: &schemaregistry.SubjectState{
					Subject: "orders-com.acme.Order",
					Schema:  schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroOrder},
				},
				ValueStrategy: "TopicRecordName",
			},
		},
	}
	topicByName := map[string]*v1alpha1.KafkaTopic{}
	for _, ts := range snap.Topics {
		topicByName[ts.Name] = &v1alpha1.KafkaTopic{
			ObjectMeta: metav1.ObjectMeta{Name: ts.Name},
			Spec:       v1alpha1.KafkaTopicSpec{TopicName: ts.Name},
		}
	}

	var warnings []string
	files := applySchemas(snap, topicByName, &warnings)
	require.Empty(t, warnings)
	require.Len(t, files, 3)

	verified := verifySchemaSubjects(snap, topicByName, files, &warnings)

	require.Empty(t, warnings, "clean attributions must not warn")
	require.Equal(t, files, verified, "clean attributions must keep their files untouched")
	require.NotNil(t, topicByName["events"].Spec.Schema)
	require.Equal(t, "TopicName", topicByName["events"].Spec.Schema.SubjectStrategy)
	require.NotNil(t, topicByName["orders"].Spec.Schema)
	require.Equal(t, "TopicRecordName", topicByName["orders"].Spec.Schema.SubjectStrategy)
}
