package mock

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
)

func ctx() context.Context { return context.Background() }

func TestRegisterThenGetSubject(t *testing.T) {
	c := New()
	s := schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"type":"record","name":"Order","fields":[]}`}
	id, err := c.RegisterSchema(ctx(), "payments.orders-value", s)
	require.NoError(t, err)
	require.Positive(t, id)

	got, err := c.GetSubject(ctx(), "payments.orders-value")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "payments.orders-value", got.Subject)
	require.Equal(t, 1, got.Version)
	require.Equal(t, id, got.ID)
	require.Equal(t, s, got.Schema)

	require.Equal(t, []string{"RegisterSchema payments.orders-value"}, c.Calls())
}

func TestGetSubjectAbsentReturnsNilNil(t *testing.T) {
	c := New()
	got, err := c.GetSubject(ctx(), "nope")
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestRegisterIdenticalTwiceIsIdempotent(t *testing.T) {
	c := New()
	s := schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"a":1}`}
	id1, err := c.RegisterSchema(ctx(), "subj", s)
	require.NoError(t, err)
	id2, err := c.RegisterSchema(ctx(), "subj", s)
	require.NoError(t, err)
	require.Equal(t, id1, id2)

	got, err := c.GetSubject(ctx(), "subj")
	require.NoError(t, err)
	require.Equal(t, 1, got.Version) // no new version added
	require.Equal(t, id1, got.ID)

	// Both calls are recorded even though the second did not mutate.
	require.Equal(t, []string{"RegisterSchema subj", "RegisterSchema subj"}, c.Calls())
}

func TestRegisterChangedDefinitionBumpsVersion(t *testing.T) {
	c := New()
	id1, err := c.RegisterSchema(ctx(), "subj", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"a":1}`})
	require.NoError(t, err)
	id2, err := c.RegisterSchema(ctx(), "subj", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"a":2}`})
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)

	got, err := c.GetSubject(ctx(), "subj")
	require.NoError(t, err)
	require.Equal(t, 2, got.Version)
	require.Equal(t, id2, got.ID)
	require.Equal(t, `{"a":2}`, got.Schema.Definition)
}

func TestRegisterSameDefinitionDifferentTypeBumpsVersion(t *testing.T) {
	c := New()
	id1, err := c.RegisterSchema(ctx(), "subj", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{}`})
	require.NoError(t, err)
	id2, err := c.RegisterSchema(ctx(), "subj", schemaregistry.Schema{Type: schemaregistry.JSON, Definition: `{}`})
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)

	got, err := c.GetSubject(ctx(), "subj")
	require.NoError(t, err)
	require.Equal(t, 2, got.Version)
	require.Equal(t, schemaregistry.JSON, got.Schema.Type)
}

func TestSetGetCompatibility(t *testing.T) {
	c := New()
	_, err := c.RegisterSchema(ctx(), "subj", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{}`})
	require.NoError(t, err)

	level, err := c.GetCompatibility(ctx(), "subj")
	require.NoError(t, err)
	require.Equal(t, "", level) // unset inherits global

	require.NoError(t, c.SetCompatibility(ctx(), "subj", "FULL"))
	level, err = c.GetCompatibility(ctx(), "subj")
	require.NoError(t, err)
	require.Equal(t, "FULL", level)

	got, err := c.GetSubject(ctx(), "subj")
	require.NoError(t, err)
	require.Equal(t, "FULL", got.Compatibility)
}

func TestSetCompatibilityOnNotYetExistingSubject(t *testing.T) {
	c := New()
	require.NoError(t, c.SetCompatibility(ctx(), "future", "BACKWARD"))

	level, err := c.GetCompatibility(ctx(), "future")
	require.NoError(t, err)
	require.Equal(t, "BACKWARD", level)

	// The subject still does not "exist" as a registered schema.
	got, err := c.GetSubject(ctx(), "future")
	require.NoError(t, err)
	require.Nil(t, got)

	require.Equal(t, []string{"SetCompatibility future BACKWARD"}, c.Calls())
}

func TestGetCompatibilityAbsentReturnsEmpty(t *testing.T) {
	c := New()
	level, err := c.GetCompatibility(ctx(), "nope")
	require.NoError(t, err)
	require.Equal(t, "", level)
}

func TestDeleteSubjectRemoves(t *testing.T) {
	c := New()
	_, err := c.RegisterSchema(ctx(), "subj", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{}`})
	require.NoError(t, err)

	require.NoError(t, c.DeleteSubject(ctx(), "subj"))
	got, err := c.GetSubject(ctx(), "subj")
	require.NoError(t, err)
	require.Nil(t, got)

	require.Contains(t, c.Calls(), "DeleteSubject subj")
}

func TestDeleteSubjectAbsentIsNoOp(t *testing.T) {
	c := New()
	require.NoError(t, c.DeleteSubject(ctx(), "nope"))
	require.Equal(t, []string{"DeleteSubject nope"}, c.Calls())
}

func TestListSubjectsSorted(t *testing.T) {
	c := New()
	for _, s := range []string{"c.subj", "a.subj", "b.subj"} {
		_, err := c.RegisterSchema(ctx(), s, schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{}`})
		require.NoError(t, err)
	}
	subs, err := c.ListSubjects(ctx())
	require.NoError(t, err)
	require.Equal(t, []string{"a.subj", "b.subj", "c.subj"}, subs)

	// Deterministic across calls.
	again, err := c.ListSubjects(ctx())
	require.NoError(t, err)
	require.Equal(t, subs, again)
}

func TestFromFileSeedsSubjects(t *testing.T) {
	c, err := FromFile("testdata/state.yaml")
	require.NoError(t, err)

	subs, err := c.ListSubjects(ctx())
	require.NoError(t, err)
	require.Equal(t, []string{"orders.events-value", "payments.orders-value"}, subs)

	got, err := c.GetSubject(ctx(), "payments.orders-value")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, 1, got.Version)
	require.Positive(t, got.ID)
	require.Equal(t, schemaregistry.AVRO, got.Schema.Type)
	require.Equal(t, `{"type":"record","name":"Order","fields":[]}`, got.Schema.Definition)
	require.Equal(t, "BACKWARD", got.Compatibility)

	level, err := c.GetCompatibility(ctx(), "payments.orders-value")
	require.NoError(t, err)
	require.Equal(t, "BACKWARD", level)

	// Subject without compatibility inherits global ("").
	level, err = c.GetCompatibility(ctx(), "orders.events-value")
	require.NoError(t, err)
	require.Equal(t, "", level)

	// Seeded subjects get distinct ids.
	a, _ := c.GetSubject(ctx(), "payments.orders-value")
	b, _ := c.GetSubject(ctx(), "orders.events-value")
	require.NotEqual(t, a.ID, b.ID)
}

func TestFromFileMissing(t *testing.T) {
	_, err := FromFile(filepath.Join("testdata", "does-not-exist.yaml"))
	require.Error(t, err)
}

func TestCheckCompatibilityDefaultTrue(t *testing.T) {
	c := New()
	ok, err := c.CheckCompatibility(ctx(), "subj", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{}`})
	require.NoError(t, err)
	require.True(t, ok)
}

func TestCheckCompatibilitySettable(t *testing.T) {
	c := New()
	c.SetCheckResult(false)
	ok, err := c.CheckCompatibility(ctx(), "subj", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{}`})
	require.NoError(t, err)
	require.False(t, ok)

	// CheckCompatibility is a read; it must not be recorded.
	require.Empty(t, c.Calls())
}

func TestFailOnRegisterReturnsErrAndAddsNoVersion(t *testing.T) {
	c := New()
	_, err := c.RegisterSchema(ctx(), "subj", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"a":1}`})
	require.NoError(t, err)

	boom := errors.New("boom")
	c.FailOn("RegisterSchema", "subj", boom)

	_, err = c.RegisterSchema(ctx(), "subj", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"a":2}`})
	require.ErrorIs(t, err, boom)

	// State unchanged: still version 1 with original definition.
	got, err := c.GetSubject(ctx(), "subj")
	require.NoError(t, err)
	require.Equal(t, 1, got.Version)
	require.Equal(t, `{"a":1}`, got.Schema.Definition)

	// The failing call is still recorded.
	require.Equal(t, []string{"RegisterSchema subj", "RegisterSchema subj"}, c.Calls())
}

func TestFailOnDeleteSubject(t *testing.T) {
	c := New()
	_, err := c.RegisterSchema(ctx(), "subj", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{}`})
	require.NoError(t, err)

	boom := errors.New("nope")
	c.FailOn("DeleteSubject", "subj", boom)
	require.ErrorIs(t, c.DeleteSubject(ctx(), "subj"), boom)

	// Not deleted.
	got, err := c.GetSubject(ctx(), "subj")
	require.NoError(t, err)
	require.NotNil(t, got)
}

func TestImplementsInterface(t *testing.T) {
	var _ schemaregistry.Client = New()
}

func TestLookupSchemaFindsOlderVersion(t *testing.T) {
	c := New()
	v1 := schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"type":"record","name":"Order","fields":[]}`}
	v2 := schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`}
	_, err := c.RegisterSchema(ctx(), "s", v1)
	require.NoError(t, err)
	_, err = c.RegisterSchema(ctx(), "s", v2)
	require.NoError(t, err)

	got, err := c.LookupSchema(ctx(), "s", v1)
	require.NoError(t, err)
	require.Equal(t, 1, got)

	got, err = c.LookupSchema(ctx(), "s", v2)
	require.NoError(t, err)
	require.Equal(t, 2, got)
}

func TestLookupSchemaNotRegistered(t *testing.T) {
	c := New()
	_, err := c.RegisterSchema(ctx(), "s", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"a":1}`})
	require.NoError(t, err)

	// Different definition: not registered -> (0, nil). Absent subject too.
	got, err := c.LookupSchema(ctx(), "s", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"b":2}`})
	require.NoError(t, err)
	require.Zero(t, got)

	got, err = c.LookupSchema(ctx(), "absent", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"a":1}`})
	require.NoError(t, err)
	require.Zero(t, got)
}

func TestLookupSchemaTrimsWhitespace(t *testing.T) {
	// The live registry compares canonically; the mock approximates with a
	// trimmed-string match so file-read bodies (trailing newline) still hit.
	c := New()
	_, err := c.RegisterSchema(ctx(), "s", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"a":1}`})
	require.NoError(t, err)
	got, err := c.LookupSchema(ctx(), "s", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: "{\"a\":1}\n"})
	require.NoError(t, err)
	require.Equal(t, 1, got)
}

func TestFromFileMultiVersionSubject(t *testing.T) {
	c, err := FromFile(filepath.Join("testdata", "versions.yaml"))
	require.NoError(t, err)

	st, err := c.GetSubject(ctx(), "multi-value")
	require.NoError(t, err)
	require.NotNil(t, st)
	require.Equal(t, 2, st.Version)
	require.Contains(t, st.Schema.Definition, "v2")

	v, err := c.LookupSchema(ctx(), "multi-value", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"name":"v1"}`})
	require.NoError(t, err)
	require.Equal(t, 1, v)
}
