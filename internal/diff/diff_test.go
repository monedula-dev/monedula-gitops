package diff

import (
	"sort"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/monedula-dev/monedula-gitops/internal/quota"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	"github.com/stretchr/testify/require"
)

func sp(s string) *string { return &s }

func quotaOps(res []operations.Operation) []operations.Operation {
	var out []operations.Operation
	for _, op := range res {
		switch op.Action {
		case operations.SetQuota, operations.UpdateQuota, operations.RemoveQuota:
			out = append(out, op)
		}
	}
	return out
}

func schemaOps(res []operations.Operation) []operations.Operation {
	var out []operations.Operation
	for _, op := range res {
		switch op.Action {
		case operations.RegisterSchema, operations.RaiseSchemaCompatibility, operations.LowerSchemaCompatibility, operations.DeleteSubject:
			out = append(out, op)
		}
	}
	return out
}

func findOp(res []operations.Operation, a operations.Action) *operations.Operation {
	for i := range res {
		if res[i].Action == a {
			return &res[i]
		}
	}
	return nil
}

func TestSchemaNewSubjectRegisters(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:    "payments.orders-value",
		Topic:      "payments.orders",
		Type:       "AVRO",
		Definition: `{"type":"record","name":"Order","fields":[]}`,
	}}}
	res := Compute(desired, Live{})
	sch := schemaOps(res)
	require.Len(t, sch, 1)
	require.Equal(t, operations.RegisterSchema, sch[0].Action)
	require.Equal(t, operations.RiskLow, sch[0].Risk)
	require.False(t, sch[0].RequiresApproval)
	require.Equal(t, "payments.orders-value", sch[0].Target)
	require.Equal(t, "payments.orders-value", sch[0].Subject)
	require.Equal(t, "AVRO", sch[0].SchemaType)
	require.Equal(t, `{"type":"record","name":"Order","fields":[]}`, sch[0].SchemaDef)
	require.Equal(t, "payments.orders", sch[0].Topic)
}

func TestSchemaNewSubjectWithCompatibility(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "payments.orders-value",
		Topic:         "payments.orders",
		Type:          "AVRO",
		Definition:    `{"type":"record","name":"Order","fields":[]}`,
		Compatibility: "BACKWARD",
	}}}
	res := Compute(desired, Live{})
	sch := schemaOps(res)
	require.Len(t, sch, 2)

	reg := findOp(sch, operations.RegisterSchema)
	require.NotNil(t, reg)
	require.Equal(t, operations.RiskLow, reg.Risk)
	require.False(t, reg.RequiresApproval)

	raise := findOp(sch, operations.RaiseSchemaCompatibility)
	require.NotNil(t, raise)
	require.Equal(t, operations.RiskLow, raise.Risk)
	require.False(t, raise.RequiresApproval)
	require.Equal(t, "payments.orders-value", raise.Subject)
	require.Equal(t, "BACKWARD", raise.Compatibility)
	require.Equal(t, "payments.orders", raise.Topic)
}

func TestSchemaChangedDefinitionRegisters(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:    "s-value",
		Type:       "AVRO",
		Definition: `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`,
	}}}
	live := Live{Schemas: []schemaregistry.SubjectState{{
		Subject: "s-value",
		Schema:  schemaregistry.Schema{Type: "AVRO", Definition: `{"type":"record","name":"Order","fields":[]}`},
	}}}
	res := Compute(desired, live)
	sch := schemaOps(res)
	require.Len(t, sch, 1)
	require.Equal(t, operations.RegisterSchema, sch[0].Action)
}

func TestSchemaIdenticalNoOp(t *testing.T) {
	// Differ only by whitespace and key order; canonically equal => no op.
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:    "s-value",
		Type:       "AVRO",
		Definition: `{ "name": "Order", "type": "record", "fields": [] }`,
	}}}
	live := Live{Schemas: []schemaregistry.SubjectState{{
		Subject: "s-value",
		Schema:  schemaregistry.Schema{Type: "AVRO", Definition: `{"type":"record","name":"Order","fields":[]}`},
	}}}
	res := Compute(desired, live)
	require.Empty(t, schemaOps(res))
}

func TestSchemaCompatRaise(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "s-value",
		Type:          "AVRO",
		Definition:    `{"type":"record","name":"Order","fields":[]}`,
		Compatibility: "FULL",
	}}}
	live := Live{Schemas: []schemaregistry.SubjectState{{
		Subject:       "s-value",
		Schema:        schemaregistry.Schema{Type: "AVRO", Definition: `{"type":"record","name":"Order","fields":[]}`},
		Compatibility: "BACKWARD",
	}}}
	res := Compute(desired, live)
	sch := schemaOps(res)
	require.Len(t, sch, 1)
	require.Equal(t, operations.RaiseSchemaCompatibility, sch[0].Action)
	require.Equal(t, operations.RiskLow, sch[0].Risk)
	require.False(t, sch[0].RequiresApproval)
	require.Equal(t, "FULL", sch[0].Compatibility)
}

func TestSchemaCompatLower(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "s-value",
		Type:          "AVRO",
		Definition:    `{"type":"record","name":"Order","fields":[]}`,
		Compatibility: "BACKWARD",
	}}}
	live := Live{Schemas: []schemaregistry.SubjectState{{
		Subject:       "s-value",
		Schema:        schemaregistry.Schema{Type: "AVRO", Definition: `{"type":"record","name":"Order","fields":[]}`},
		Compatibility: "FULL",
	}}}
	res := Compute(desired, live)
	sch := schemaOps(res)
	require.Len(t, sch, 1)
	require.Equal(t, operations.LowerSchemaCompatibility, sch[0].Action)
	require.Equal(t, operations.RiskHigh, sch[0].Risk)
	require.True(t, sch[0].RequiresApproval)
	require.Equal(t, "BACKWARD", sch[0].Compatibility)
}

func TestSchemaCompatSideways(t *testing.T) {
	// Equal rank, different value => treated as Lower (gated).
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "s-value",
		Type:          "AVRO",
		Definition:    `{"type":"record","name":"Order","fields":[]}`,
		Compatibility: "FORWARD",
	}}}
	live := Live{Schemas: []schemaregistry.SubjectState{{
		Subject:       "s-value",
		Schema:        schemaregistry.Schema{Type: "AVRO", Definition: `{"type":"record","name":"Order","fields":[]}`},
		Compatibility: "BACKWARD",
	}}}
	res := Compute(desired, live)
	sch := schemaOps(res)
	require.Len(t, sch, 1)
	require.Equal(t, operations.LowerSchemaCompatibility, sch[0].Action)
	require.True(t, sch[0].RequiresApproval)
}

// --- first-time compatibility set vs the registry GLOBAL default (spec §17.1) ---
//
// A subject with NO subject-level override EFFECTIVELY runs at the registry's
// global default, so a first-time set is classified against that baseline when
// the live reader could determine it (Live.GlobalCompatibility != "").

// Declaring a level BELOW the known global default on a never-configured
// subject is an effective lowering: gated Lower, exactly like an explicit one.
func TestSchemaCompatFirstSetBelowGlobalIsLower(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "s-value",
		Type:          "AVRO",
		Definition:    "", // governance mode; content mode covered below
		Compatibility: "NONE",
	}}}
	res := Compute(desired, Live{GlobalCompatibility: "BACKWARD"}) // subject absent
	sch := schemaOps(res)
	require.Len(t, sch, 1)
	require.Equal(t, operations.LowerSchemaCompatibility, sch[0].Action)
	require.Equal(t, operations.RiskHigh, sch[0].Risk)
	require.True(t, sch[0].RequiresApproval)
	require.Equal(t, "NONE", sch[0].Compatibility)
	// From surfaces the EFFECTIVE level being lowered (the inherited global).
	// From stays machine-consumed (bare level, no prose) — the inherited-baseline
	// provenance is surfaced separately via Message for human output.
	require.Equal(t, "BACKWARD", sch[0].From)
	require.Equal(t, "compatibility baseline BACKWARD inherited from the registry global default", sch[0].Message)
}

// Declaring a level ABOVE the known global default is a genuine strengthening:
// Raise, ungated.
func TestSchemaCompatFirstSetAboveGlobalIsRaise(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "s-value",
		Type:          "AVRO",
		Definition:    "",
		Compatibility: "FULL",
	}}}
	sch := schemaOps(Compute(desired, Live{GlobalCompatibility: "BACKWARD"}))
	require.Len(t, sch, 1)
	require.Equal(t, operations.RaiseSchemaCompatibility, sch[0].Action)
	require.False(t, sch[0].RequiresApproval)
}

// Pinning exactly the global default changes nothing effective: Raise, ungated
// (the op still runs — the subject-level override must be written).
func TestSchemaCompatFirstSetEqualGlobalIsRaise(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "s-value",
		Type:          "AVRO",
		Definition:    "",
		Compatibility: "BACKWARD",
	}}}
	sch := schemaOps(Compute(desired, Live{GlobalCompatibility: "BACKWARD"}))
	require.Len(t, sch, 1)
	require.Equal(t, operations.RaiseSchemaCompatibility, sch[0].Action)
	require.False(t, sch[0].RequiresApproval)
}

// Sideways from the global default (equal rank, different value) relaxes the
// previously-guaranteed direction: conservatively a gated Lower, mirroring the
// explicit sideways rule.
func TestSchemaCompatFirstSetSidewaysGlobalIsLower(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "s-value",
		Type:          "AVRO",
		Definition:    "",
		Compatibility: "BACKWARD",
	}}}
	sch := schemaOps(Compute(desired, Live{GlobalCompatibility: "FORWARD"}))
	require.Len(t, sch, 1)
	require.Equal(t, operations.LowerSchemaCompatibility, sch[0].Action)
	require.True(t, sch[0].RequiresApproval)
}

// Unknown global (older SR / GET /config failed): explicit legacy fallback —
// ANY first-time set is an ungated Raise, the run never fails on it.
func TestSchemaCompatFirstSetGlobalUnknownLegacyRaise(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "s-value",
		Type:          "AVRO",
		Definition:    "",
		Compatibility: "NONE",
	}}}
	sch := schemaOps(Compute(desired, Live{})) // GlobalCompatibility == "" (unknown)
	require.Len(t, sch, 1)
	require.Equal(t, operations.RaiseSchemaCompatibility, sch[0].Action)
	require.False(t, sch[0].RequiresApproval)
	require.Equal(t, "", sch[0].From)
}

// An EXPLICIT subject-level live value always wins over the global as the
// baseline: NONE-overridden subject raising to BACKWARD is a Raise even though
// BACKWARD sits below the FULL global, and a FULL-overridden subject dropping
// to BACKWARD is a Lower even though BACKWARD sits above a NONE global.
func TestSchemaCompatExplicitLiveLevelWinsOverGlobal(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "s-value",
		Type:          "AVRO",
		Definition:    "",
		Compatibility: "BACKWARD",
	}}}

	up := schemaOps(Compute(desired, Live{
		Schemas:             []schemaregistry.SubjectState{{Subject: "s-value", Compatibility: "NONE"}},
		GlobalCompatibility: "FULL",
	}))
	require.Len(t, up, 1)
	require.Equal(t, operations.RaiseSchemaCompatibility, up[0].Action)
	require.False(t, up[0].RequiresApproval)
	require.Equal(t, "NONE", up[0].From)
	// Baseline is an EXPLICIT live level, not an inherited global default, so
	// there is no provenance note to surface.
	require.Empty(t, up[0].Message)

	down := schemaOps(Compute(desired, Live{
		Schemas:             []schemaregistry.SubjectState{{Subject: "s-value", Compatibility: "FULL"}},
		GlobalCompatibility: "NONE",
	}))
	require.Len(t, down, 1)
	require.Equal(t, operations.LowerSchemaCompatibility, down[0].Action)
	require.True(t, down[0].RequiresApproval)
	require.Equal(t, "FULL", down[0].From)
	require.Empty(t, down[0].Message)
}

// Content mode: a brand-new subject (RegisterSchema) declaring a level below
// the known global default is gated too — the classification is uniform across
// governance and content first-time sets.
func TestSchemaNewSubjectCompatBelowGlobalIsLower(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "payments.orders-value",
		Topic:         "payments.orders",
		Type:          "AVRO",
		Definition:    `{"type":"record","name":"Order","fields":[]}`,
		Compatibility: "NONE",
	}}}
	sch := schemaOps(Compute(desired, Live{GlobalCompatibility: "BACKWARD"}))
	require.Len(t, sch, 2)
	require.NotNil(t, findOp(sch, operations.RegisterSchema))
	lower := findOp(sch, operations.LowerSchemaCompatibility)
	require.NotNil(t, lower)
	require.True(t, lower.RequiresApproval)
	require.Equal(t, "NONE", lower.Compatibility)
	require.Equal(t, "payments.orders", lower.Topic)
}

// Governance mode (spec §12.2): an empty Definition manages ONLY the subject
// compatibility level. With the subject entirely absent from live (level "")
// the diff emits exactly one compatibility op and never a RegisterSchema.
func TestSchemaGovernanceAbsentSubjectCompatOnly(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "payments.orders-value",
		Topic:         "payments.orders",
		Type:          "AVRO",
		Definition:    "",
		Compatibility: "BACKWARD",
	}}}
	res := Compute(desired, Live{}) // subject absent entirely
	sch := schemaOps(res)
	require.Len(t, sch, 1)
	require.Equal(t, operations.RaiseSchemaCompatibility, sch[0].Action)
	require.Equal(t, "BACKWARD", sch[0].Compatibility)
	require.Equal(t, "", sch[0].From)
	require.Nil(t, findOp(sch, operations.RegisterSchema))
}

// Governance mode, synthesized live entry (subject config exists, no versions)
// with the level already equal => no ops.
func TestSchemaGovernanceLevelEqualNoOp(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "s-value",
		Type:          "AVRO",
		Definition:    "",
		Compatibility: "BACKWARD",
	}}}
	live := Live{Schemas: []schemaregistry.SubjectState{{
		Subject:       "s-value",
		Compatibility: "BACKWARD", // ID/Version 0, empty Schema (synthesized)
	}}}
	require.Empty(t, schemaOps(Compute(desired, live)))
}

// Governance mode, live level differs => exactly one compatibility op.
func TestSchemaGovernanceLevelDiffersCompatOnly(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "s-value",
		Type:          "AVRO",
		Definition:    "",
		Compatibility: "FULL",
	}}}
	live := Live{Schemas: []schemaregistry.SubjectState{{
		Subject:       "s-value",
		Compatibility: "BACKWARD",
	}}}
	sch := schemaOps(Compute(desired, live))
	require.Len(t, sch, 1)
	require.Equal(t, operations.RaiseSchemaCompatibility, sch[0].Action)
	require.Equal(t, "FULL", sch[0].Compatibility)
	require.Nil(t, findOp(sch, operations.RegisterSchema))
}

// Governance mode where a PRODUCER has registered content out-of-band: a live
// SubjectState carries a real definition. A producer-registered version is NOT
// drift — with the level matching there are zero ops, and never a
// RegisterSchema or SchemaSuperseded (SupersededSchemas is not even consulted).
func TestSchemaGovernanceProducerRegisteredNoDrift(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "s-value",
		Type:          "AVRO",
		Definition:    "", // governance: monedula owns no content
		Compatibility: "BACKWARD",
	}}}
	live := Live{
		Schemas: []schemaregistry.SubjectState{{
			Subject:       "s-value",
			ID:            42,
			Version:       3,
			Schema:        schemaregistry.Schema{Type: "AVRO", Definition: `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`},
			Compatibility: "BACKWARD",
		}},
		// A SupersededSchemas entry must be ignored entirely in governance mode.
		SupersededSchemas: map[string]int{"s-value": 1},
	}
	res := Compute(desired, live)
	require.Empty(t, schemaOps(res))
	require.Nil(t, findOp(res, operations.SchemaSuperseded))
}

func TestSchemaEqualPreservesLargeInts(t *testing.T) {
	// Two AVRO definitions byte-identical except whitespace, both containing a
	// large integer literal beyond float64's exact range. Canonicalization must
	// preserve the literal verbatim so they compare EQUAL (no op). Decoding into
	// interface{} as float64 would corrupt the integer and falsely report drift.
	big := "9223372036854775807"
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:    "s-value",
		Type:       "AVRO",
		Definition: `{ "type": "record", "name": "Order", "fields": [ { "name": "n", "type": "long", "default": ` + big + ` } ] }`,
	}}}
	live := Live{Schemas: []schemaregistry.SubjectState{{
		Subject: "s-value",
		Schema:  schemaregistry.Schema{Type: "AVRO", Definition: `{"type":"record","name":"Order","fields":[{"name":"n","type":"long","default":` + big + `}]}`},
	}}}
	require.Empty(t, schemaOps(Compute(desired, live)), "large-int definitions differing only by whitespace must be equal")

	// "10" vs "10.0" are distinct literals and must NOT be equal => RegisterSchema.
	desired10 := Desired{Schemas: []DesiredSchema{{
		Subject:    "s-value",
		Type:       "AVRO",
		Definition: `{"type":"record","name":"Order","fields":[{"name":"n","type":"long","default":10}]}`,
	}}}
	live10 := Live{Schemas: []schemaregistry.SubjectState{{
		Subject: "s-value",
		Schema:  schemaregistry.Schema{Type: "AVRO", Definition: `{"type":"record","name":"Order","fields":[{"name":"n","type":"long","default":10.0}]}`},
	}}}
	sch := schemaOps(Compute(desired10, live10))
	require.Len(t, sch, 1)
	require.Equal(t, operations.RegisterSchema, sch[0].Action)
}

func TestSchemaEqualProtobufTrimmed(t *testing.T) {
	// PROTOBUF definitions equal after trimming leading/trailing whitespace => no op.
	body := "syntax = \"proto3\";\nmessage Order { int64 id = 1; }"
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:    "s-value",
		Type:       "PROTOBUF",
		Definition: "  \n" + body + "\n  ",
	}}}
	live := Live{Schemas: []schemaregistry.SubjectState{{
		Subject: "s-value",
		Schema:  schemaregistry.Schema{Type: "PROTOBUF", Definition: body},
	}}}
	require.Empty(t, schemaOps(Compute(desired, live)), "protobuf differing only by surrounding whitespace must be equal")

	// Differing bodies => not equal => RegisterSchema.
	desiredDiff := Desired{Schemas: []DesiredSchema{{
		Subject:    "s-value",
		Type:       "PROTOBUF",
		Definition: "syntax = \"proto3\";\nmessage Order { int64 id = 2; }",
	}}}
	sch := schemaOps(Compute(desiredDiff, live))
	require.Len(t, sch, 1)
	require.Equal(t, operations.RegisterSchema, sch[0].Action)
}

func TestSchemaEqualMalformedFallback(t *testing.T) {
	// AVRO type with a deliberately malformed (non-JSON) definition on both
	// sides. Canonicalization fails on both, so the trimmed-string fallback
	// applies: byte-identical (modulo trim) => equal, no op.
	malformed := "this is { not valid json"
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:    "s-value",
		Type:       "AVRO",
		Definition: "  " + malformed + "  ",
	}}}
	live := Live{Schemas: []schemaregistry.SubjectState{{
		Subject: "s-value",
		Schema:  schemaregistry.Schema{Type: "AVRO", Definition: malformed},
	}}}
	require.Empty(t, schemaOps(Compute(desired, live)), "malformed but identical defs must be equal via trimmed fallback")

	// Differing malformed bytes => not equal => RegisterSchema.
	desiredDiff := Desired{Schemas: []DesiredSchema{{
		Subject:    "s-value",
		Type:       "AVRO",
		Definition: "also { not valid json",
	}}}
	sch := schemaOps(Compute(desiredDiff, live))
	require.Len(t, sch, 1)
	require.Equal(t, operations.RegisterSchema, sch[0].Action)
}

func TestSchemaDeterministic(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{
		{Subject: "zzz-value", Type: "AVRO", Definition: `{"type":"record","name":"Z","fields":[]}`, Compatibility: "BACKWARD"},
		{Subject: "aaa-value", Type: "AVRO", Definition: `{"type":"record","name":"A","fields":[]}`},
	}}
	a := Compute(desired, Live{})
	b := Compute(desired, Live{})
	require.Equal(t, a, b)
}

func TestCreateTopicWhenAbsent(t *testing.T) {
	res := Compute(Desired{Topics: []DesiredTopic{{Name: "payments.orders", Partitions: 3, ReplicationFactor: 3}}}, Live{})
	require.Len(t, res, 1)
	require.Equal(t, operations.CreateTopic, res[0].Action)
	require.Equal(t, operations.RiskLow, res[0].Risk)
	require.False(t, res[0].RequiresApproval)
}

func TestCreateTopicPayloadPopulated(t *testing.T) {
	cfg := map[string]string{"retention.ms": "604800000"}
	res := Compute(Desired{Topics: []DesiredTopic{{Name: "payments.orders", Partitions: 6, ReplicationFactor: 3, Config: cfg}}}, Live{})
	require.Len(t, res, 1)
	require.Equal(t, operations.CreateTopic, res[0].Action)
	require.Equal(t, 6, res[0].Partitions)
	require.Equal(t, 3, res[0].ReplicationFactor)
	require.Equal(t, cfg, res[0].Config)
}

func TestIncreasePartitionsPayloadPopulated(t *testing.T) {
	desired := Desired{Topics: []DesiredTopic{{Name: "t", Partitions: 6, ReplicationFactor: 1}}}
	live := Live{Topics: []kafka.TopicState{{Name: "t", Partitions: 3, ReplicationFactor: 1}}}
	res := Compute(desired, live)
	require.Len(t, res, 1)
	require.Equal(t, operations.IncreasePartitions, res[0].Action)
	require.Equal(t, 6, res[0].Partitions)
}

func TestCreateAclPayloadPopulated(t *testing.T) {
	a := access.ACL{Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t", PatternType: "literal", Operation: "Write", Permission: "Allow"}
	desired := Desired{ACLs: []access.ACL{a}, Scope: access.BuildScope([]access.ACL{a})}
	res := Compute(desired, Live{})
	require.Len(t, res, 1)
	require.Equal(t, operations.CreateAcl, res[0].Action)
	require.NotNil(t, res[0].ACL)
	require.Equal(t, "t", res[0].ACL.ResourceName)
	require.Equal(t, "Write", res[0].ACL.Operation)
	require.Equal(t, "topic", res[0].ACL.ResourceType)
	require.Equal(t, "User:x", res[0].ACL.Principal)
}

func TestDeleteAclPayloadPopulated(t *testing.T) {
	desiredACL := access.ACL{Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t", PatternType: "literal", Operation: "Write", Permission: "Allow"}
	scope := access.BuildScope([]access.ACL{desiredACL})
	live := Live{ACLs: []access.ACL{
		desiredACL,
		{Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t", PatternType: "literal", Operation: "Read", Permission: "Allow"},
	}}
	res := Compute(Desired{ACLs: []access.ACL{desiredACL}, Scope: scope}, live)
	var del *operations.Operation
	for i := range res {
		if res[i].Action == operations.DeleteAcl {
			del = &res[i]
		}
	}
	require.NotNil(t, del)
	require.NotNil(t, del.ACL)
	require.Equal(t, "Read", del.ACL.Operation)
	require.Equal(t, "t", del.ACL.ResourceName)
}

func TestNoOpWhenTopicMatches(t *testing.T) {
	desired := Desired{Topics: []DesiredTopic{{Name: "t", Partitions: 3, ReplicationFactor: 3, Config: map[string]string{"a": "1"}}}}
	live := Live{Topics: []kafka.TopicState{{Name: "t", Partitions: 3, ReplicationFactor: 3, Config: map[string]string{"a": "1"}}}}
	res := Compute(desired, live)
	require.Empty(t, res) // no operations needed
}

func TestUpdateTopicConfigDrift(t *testing.T) {
	desired := Desired{Topics: []DesiredTopic{{Name: "t", Partitions: 1, ReplicationFactor: 1, Config: map[string]string{"retention.ms": "604800000"}}}}
	live := Live{Topics: []kafka.TopicState{{Name: "t", Partitions: 1, ReplicationFactor: 1, Config: map[string]string{"retention.ms": "86400000"}}}}
	res := Compute(desired, live)
	require.Len(t, res, 1)
	require.Equal(t, operations.UpdateTopicConfig, res[0].Action)
	require.Equal(t, "86400000", res[0].From)
	require.Equal(t, "604800000", res[0].To)
}

func TestUnknownLiveConfigKeyLeftUnchanged(t *testing.T) {
	// live has an extra config key not in desired => NOT managed, no op
	desired := Desired{Topics: []DesiredTopic{{Name: "t", Partitions: 1, ReplicationFactor: 1, Config: map[string]string{"a": "1"}}}}
	live := Live{Topics: []kafka.TopicState{{Name: "t", Partitions: 1, ReplicationFactor: 1, Config: map[string]string{"a": "1", "b": "2"}}}}
	res := Compute(desired, live)
	require.Empty(t, res)
}

func TestPartitionDecreaseRejected(t *testing.T) {
	desired := Desired{Topics: []DesiredTopic{{Name: "t", Partitions: 1, ReplicationFactor: 1}}}
	live := Live{Topics: []kafka.TopicState{{Name: "t", Partitions: 3, ReplicationFactor: 1}}}
	res := Compute(desired, live)
	require.Len(t, res, 1)
	require.Equal(t, operations.Rejected, res[0].Action)
}

func TestPartitionIncreaseRequiresApproval(t *testing.T) {
	desired := Desired{Topics: []DesiredTopic{{Name: "t", Partitions: 6, ReplicationFactor: 1}}}
	live := Live{Topics: []kafka.TopicState{{Name: "t", Partitions: 3, ReplicationFactor: 1}}}
	res := Compute(desired, live)
	require.Equal(t, operations.IncreasePartitions, res[0].Action)
	require.Equal(t, operations.RiskMedium, res[0].Risk)
	require.True(t, res[0].RequiresApproval)
}

func TestCreateAclWhenDesiredNotLive(t *testing.T) {
	a := access.ACL{Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t", PatternType: "literal", Operation: "Write", Permission: "Allow"}
	desired := Desired{ACLs: []access.ACL{a}, Scope: access.BuildScope([]access.ACL{a})}
	res := Compute(desired, Live{})
	require.Len(t, res, 1)
	require.Equal(t, operations.CreateAcl, res[0].Action)
}

func TestPruneAclInScope(t *testing.T) {
	desiredACL := access.ACL{Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t", PatternType: "literal", Operation: "Write", Permission: "Allow"}
	scope := access.BuildScope([]access.ACL{desiredACL})
	live := Live{ACLs: []access.ACL{
		desiredACL,
		{Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t", PatternType: "literal", Operation: "Read", Permission: "Allow"},
	}}
	res := Compute(Desired{ACLs: []access.ACL{desiredACL}, Scope: scope}, live)
	var deletes int
	for _, op := range res {
		if op.Action == operations.DeleteAcl {
			deletes++
		}
	}
	require.Equal(t, 1, deletes)
}

func TestIgnoreOutOfScopeLiveAcl(t *testing.T) {
	desiredACL := access.ACL{Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t", PatternType: "literal", Operation: "Write", Permission: "Allow"}
	scope := access.BuildScope([]access.ACL{desiredACL})
	live := Live{ACLs: []access.ACL{
		desiredACL,
		{Principal: "User:other", Host: "*", ResourceType: "topic", ResourceName: "z", PatternType: "literal", Operation: "Read", Permission: "Allow"},
	}}
	res := Compute(Desired{ACLs: []access.ACL{desiredACL}, Scope: scope}, live)
	for _, op := range res {
		require.NotEqual(t, operations.DeleteAcl, op.Action, "must not prune out-of-scope ACL")
	}
}

func TestUpdateReplicationFactorDrift(t *testing.T) {
	desired := Desired{Topics: []DesiredTopic{{Name: "t", Partitions: 3, ReplicationFactor: 5}}}
	live := Live{Topics: []kafka.TopicState{{Name: "t", Partitions: 3, ReplicationFactor: 3}}}
	res := Compute(desired, live)
	require.Len(t, res, 1)
	require.Equal(t, operations.UpdateReplicationFactor, res[0].Action)
	require.Equal(t, operations.RiskHigh, res[0].Risk)
	require.True(t, res[0].RequiresApproval)
	require.Equal(t, "3", res[0].From)
	require.Equal(t, "5", res[0].To)
}

func TestReplicationFactorSkippedWhenZero(t *testing.T) {
	desired := Desired{Topics: []DesiredTopic{{Name: "t", Partitions: 3, ReplicationFactor: 0}}}
	live := Live{Topics: []kafka.TopicState{{Name: "t", Partitions: 3, ReplicationFactor: 3}}}
	res := Compute(desired, live)
	require.Empty(t, res)
}

func TestAclTieOrderingDeterministic(t *testing.T) {
	// Two live ACLs that differ ONLY in Host, both in scope and not desired => both prune.
	a1 := access.ACL{Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t", PatternType: "literal", Operation: "Write", Permission: "Allow"}
	a2 := access.ACL{Principal: "User:x", Host: "10.0.0.1", ResourceType: "topic", ResourceName: "t", PatternType: "literal", Operation: "Write", Permission: "Allow"}
	// Scope must include the principal+resource pattern; build it from an ACL with that key.
	scope := access.BuildScope([]access.ACL{a1})
	live := Live{ACLs: []access.ACL{a1, a2}}
	desired := Desired{Scope: scope}

	first := Compute(desired, live)
	second := Compute(desired, live)
	require.Equal(t, first, second)

	var deletes []operations.Operation
	for _, op := range first {
		if op.Action == operations.DeleteAcl {
			deletes = append(deletes, op)
		}
	}
	require.Len(t, deletes, 2)
	// Stable order: sorted by Target (which embeds the differing Host indirectly via FullKey
	// sort of inputs, then final sort by Action/Target/Field). Assert a fixed order.
	require.True(t, deletes[0].Target <= deletes[1].Target)
}

func TestDeterministicOrdering(t *testing.T) {
	desired := Desired{Topics: []DesiredTopic{
		{Name: "zzz", Partitions: 1, ReplicationFactor: 1},
		{Name: "aaa", Partitions: 1, ReplicationFactor: 1},
	}}
	a := Compute(desired, Live{})
	b := Compute(desired, Live{})
	require.Equal(t, a, b)
}

func TestSchemaSupersededEmitsTerminalOp(t *testing.T) {
	// Spec §12.1: the manifest schema differs from the LATEST version but is
	// registered as an OLDER one -> re-registering would dedupe and never
	// converge. The diff emits SchemaSuperseded (never RegisterSchema).
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:    "s-value",
		Topic:      "s",
		Type:       "AVRO",
		Definition: `{"type":"record","name":"Order","fields":[]}`,
		Mode:       operations.ModeEnforce,
	}}}
	live := Live{
		Schemas: []schemaregistry.SubjectState{{
			Subject: "s-value",
			Version: 2,
			Schema:  schemaregistry.Schema{Type: "AVRO", Definition: `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`},
		}},
		SupersededSchemas: map[string]int{"s-value": 1},
	}
	res := Compute(desired, live)
	require.Len(t, res, 1)
	op := res[0]
	require.Equal(t, operations.SchemaSuperseded, op.Action)
	require.Equal(t, "s-value", op.Subject)
	require.Equal(t, "s-value", op.Target)
	require.Equal(t, operations.ModeEnforce, op.Mode)
	require.Equal(t, "s", op.Topic)
	// Carried like Rejected: no risk, no gate, never executed.
	require.Equal(t, operations.RiskNone, op.Risk)
	require.False(t, op.RequiresApproval)
	require.Contains(t, op.Message, "older version")
	require.Contains(t, op.Message, "v1")
	require.Contains(t, op.Message, "latest is v2")
	require.Nil(t, findOp(res, operations.RegisterSchema))
}

func TestSchemaChangedNotSupersededStillRegisters(t *testing.T) {
	// A brand-new definition (registered under NO version) registers as before
	// even when a superseded map is supplied for other subjects.
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:    "s-value",
		Type:       "AVRO",
		Definition: `{"type":"record","name":"Order","fields":[{"name":"x","type":"long"}]}`,
	}}}
	live := Live{
		Schemas: []schemaregistry.SubjectState{{
			Subject: "s-value",
			Version: 2,
			Schema:  schemaregistry.Schema{Type: "AVRO", Definition: `{"type":"record","name":"Order","fields":[]}`},
		}},
		SupersededSchemas: map[string]int{"other-value": 1},
	}
	res := Compute(desired, live)
	sch := schemaOps(res)
	require.Len(t, sch, 1)
	require.Equal(t, operations.RegisterSchema, sch[0].Action)
}

func TestSchemaSupersededIgnoredWhenInSync(t *testing.T) {
	// Defensive: a stale superseded entry for a subject already in sync must
	// not produce an op (in-sync wins).
	def := `{"type":"record","name":"Order","fields":[]}`
	desired := Desired{Schemas: []DesiredSchema{{Subject: "s-value", Type: "AVRO", Definition: def}}}
	live := Live{
		Schemas:           []schemaregistry.SubjectState{{Subject: "s-value", Version: 1, Schema: schemaregistry.Schema{Type: "AVRO", Definition: def}}},
		SupersededSchemas: map[string]int{"s-value": 1},
	}
	require.Empty(t, Compute(desired, live))
}

// ---- drift.ignoreFields (spec §16) ----

func TestIgnoreFieldsConfigKeySkipsDrift(t *testing.T) {
	desired := Desired{Topics: []DesiredTopic{{
		Name: "t", Partitions: 3,
		Config:       map[string]string{"retention.ms": "1000", "cleanup.policy": "compact"},
		IgnoreFields: []string{"config.retention.ms"},
	}}}
	live := Live{Topics: []kafka.TopicState{{
		Name: "t", Partitions: 3,
		Config: map[string]string{"retention.ms": "9999", "cleanup.policy": "delete"},
	}}}
	res := Compute(desired, live)
	require.Len(t, res, 1, "only the non-ignored key drifts: %v", res)
	require.Equal(t, operations.UpdateTopicConfig, res[0].Action)
	require.Equal(t, "cleanup.policy", res[0].Field)
}

func TestIgnoreFieldsPartitionsSkipsBothDirections(t *testing.T) {
	desired := Desired{Topics: []DesiredTopic{{
		Name: "t", Partitions: 6, IgnoreFields: []string{"partitions"},
	}}}
	// Increase ignored.
	res := Compute(desired, Live{Topics: []kafka.TopicState{{Name: "t", Partitions: 3}}})
	require.Empty(t, res)
	// Decrease (normally Rejected) ignored too: "partitions" is excluded from
	// drift calculation entirely.
	res = Compute(desired, Live{Topics: []kafka.TopicState{{Name: "t", Partitions: 12}}})
	require.Empty(t, res)
}

func TestIgnoreFieldsReplicationFactorSkips(t *testing.T) {
	desired := Desired{Topics: []DesiredTopic{{
		Name: "t", Partitions: 3, ReplicationFactor: 5, IgnoreFields: []string{"replicationFactor"},
	}}}
	live := Live{Topics: []kafka.TopicState{{Name: "t", Partitions: 3, ReplicationFactor: 3}}}
	require.Empty(t, Compute(desired, live))
}

func TestIgnoreFieldsDoNotAffectCreate(t *testing.T) {
	// An absent topic is still created in full; ignoreFields only excludes
	// fields from DRIFT calculation.
	desired := Desired{Topics: []DesiredTopic{{
		Name: "t", Partitions: 3, Config: map[string]string{"retention.ms": "1000"},
		IgnoreFields: []string{"partitions", "config.retention.ms"},
	}}}
	res := Compute(desired, Live{})
	require.Len(t, res, 1)
	require.Equal(t, operations.CreateTopic, res[0].Action)
	require.Equal(t, 3, res[0].Partitions)
}

// keysOf returns the sorted set of limit keys in a quota op payload.
func keysOf(m map[string]float64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestQuotaAbsentFromLiveSets(t *testing.T) {
	desired := Desired{Quotas: []quota.Desired{{
		Entity: quota.Entity{{Type: "user", Name: sp("svc-checkout")}},
		Limits: map[string]float64{"producer_byte_rate": 1024, "consumer_byte_rate": 2048},
	}}}
	res := quotaOps(Compute(desired, Live{}))
	require.Len(t, res, 1)
	require.Equal(t, operations.SetQuota, res[0].Action)
	require.Equal(t, operations.RiskLow, res[0].Risk)
	require.False(t, res[0].RequiresApproval)
	require.Equal(t, []kafka.QuotaEntityComponent{{Type: "user", Name: sp("svc-checkout")}}, res[0].QuotaEntity)
	require.Equal(t, []string{"consumer_byte_rate", "producer_byte_rate"}, keysOf(res[0].QuotaLimits))
	require.Equal(t, float64(1024), res[0].QuotaLimits["producer_byte_rate"])
}

func TestQuotaPresentValueDiffersUpdates(t *testing.T) {
	desired := Desired{Quotas: []quota.Desired{{
		Entity: quota.Entity{{Type: "user", Name: sp("svc-checkout")}},
		Limits: map[string]float64{"producer_byte_rate": 4096, "consumer_byte_rate": 2048},
	}}}
	live := Live{Quotas: []kafka.QuotaState{{
		Entity: []kafka.QuotaEntityComponent{{Type: "user", Name: sp("svc-checkout")}},
		Limits: map[string]float64{"producer_byte_rate": 1024, "consumer_byte_rate": 2048},
	}}}
	res := quotaOps(Compute(desired, live))
	require.Len(t, res, 1)
	require.Equal(t, operations.UpdateQuota, res[0].Action)
	require.Equal(t, []kafka.QuotaEntityComponent{{Type: "user", Name: sp("svc-checkout")}}, res[0].QuotaEntity)
	// Update carries the FULL desired limit set.
	require.Equal(t, []string{"consumer_byte_rate", "producer_byte_rate"}, keysOf(res[0].QuotaLimits))
	require.Equal(t, float64(4096), res[0].QuotaLimits["producer_byte_rate"])
}

func TestQuotaPresentDesiredKeyMissingLiveUpdates(t *testing.T) {
	desired := Desired{Quotas: []quota.Desired{{
		Entity: quota.Entity{{Type: "user", Name: sp("svc-checkout")}},
		Limits: map[string]float64{"producer_byte_rate": 1024, "consumer_byte_rate": 2048},
	}}}
	live := Live{Quotas: []kafka.QuotaState{{
		Entity: []kafka.QuotaEntityComponent{{Type: "user", Name: sp("svc-checkout")}},
		Limits: map[string]float64{"producer_byte_rate": 1024},
	}}}
	res := quotaOps(Compute(desired, live))
	require.Len(t, res, 1)
	require.Equal(t, operations.UpdateQuota, res[0].Action)
	require.Contains(t, res[0].QuotaLimits, "consumer_byte_rate")
	require.Equal(t, []string{"consumer_byte_rate", "producer_byte_rate"}, keysOf(res[0].QuotaLimits))
}

func TestQuotaLiveKeyNotInDesiredRemoves(t *testing.T) {
	desired := Desired{Quotas: []quota.Desired{{
		Entity: quota.Entity{{Type: "user", Name: sp("svc-checkout")}},
		Limits: map[string]float64{"producer_byte_rate": 1024},
	}}}
	live := Live{Quotas: []kafka.QuotaState{{
		Entity: []kafka.QuotaEntityComponent{{Type: "user", Name: sp("svc-checkout")}},
		Limits: map[string]float64{"producer_byte_rate": 1024, "consumer_byte_rate": 2048},
	}}}
	res := quotaOps(Compute(desired, live))
	require.Len(t, res, 1)
	require.Equal(t, operations.RemoveQuota, res[0].Action)
	require.Equal(t, []kafka.QuotaEntityComponent{{Type: "user", Name: sp("svc-checkout")}}, res[0].QuotaEntity)
	require.Equal(t, []string{"consumer_byte_rate"}, keysOf(res[0].QuotaLimits))
}

func TestQuotaIdenticalNoOp(t *testing.T) {
	desired := Desired{Quotas: []quota.Desired{{
		Entity: quota.Entity{{Type: "user", Name: sp("svc-checkout")}},
		Limits: map[string]float64{"producer_byte_rate": 1024, "consumer_byte_rate": 2048},
	}}}
	live := Live{Quotas: []kafka.QuotaState{{
		Entity: []kafka.QuotaEntityComponent{{Type: "user", Name: sp("svc-checkout")}},
		Limits: map[string]float64{"producer_byte_rate": 1024, "consumer_byte_rate": 2048},
	}}}
	require.Empty(t, quotaOps(Compute(desired, live)))
}

// An entity needing both an added/changed key AND a removed key emits BOTH an
// UpdateQuota (full desired set) and a RemoveQuota (the extra live key).
func TestQuotaBothUpdateAndRemove(t *testing.T) {
	desired := Desired{Quotas: []quota.Desired{{
		Entity: quota.Entity{{Type: "user", Name: sp("svc-checkout")}},
		Limits: map[string]float64{"producer_byte_rate": 4096},
	}}}
	live := Live{Quotas: []kafka.QuotaState{{
		Entity: []kafka.QuotaEntityComponent{{Type: "user", Name: sp("svc-checkout")}},
		Limits: map[string]float64{"producer_byte_rate": 1024, "consumer_byte_rate": 2048},
	}}}
	res := quotaOps(Compute(desired, live))
	require.Len(t, res, 2)
	upd := findOp(res, operations.UpdateQuota)
	rem := findOp(res, operations.RemoveQuota)
	require.NotNil(t, upd)
	require.NotNil(t, rem)
	require.Equal(t, []string{"producer_byte_rate"}, keysOf(upd.QuotaLimits))
	require.Equal(t, float64(4096), upd.QuotaLimits["producer_byte_rate"])
	require.Equal(t, []string{"consumer_byte_rate"}, keysOf(rem.QuotaLimits))
}

func TestQuotaModeThreadsOntoOp(t *testing.T) {
	desired := Desired{Quotas: []quota.Desired{{
		Entity: quota.Entity{{Type: "user", Name: sp("svc-checkout")}},
		Limits: map[string]float64{"producer_byte_rate": 1024},
		Mode:   operations.ModeDetectOnly,
	}}}
	res := quotaOps(Compute(desired, Live{}))
	require.Len(t, res, 1)
	require.Equal(t, operations.SetQuota, res[0].Action)
	require.Equal(t, operations.ModeDetectOnly, res[0].Mode)
}

func TestQuotaDefaultEntityMultiComponent(t *testing.T) {
	desired := Desired{Quotas: []quota.Desired{{
		Entity: quota.Entity{{Type: "user", Name: nil}, {Type: "client-id", Name: sp("batch")}},
		Limits: map[string]float64{"request_percentage": 50},
	}}}
	res := quotaOps(Compute(desired, Live{}))
	require.Len(t, res, 1)
	require.Equal(t, operations.SetQuota, res[0].Action)
	require.Equal(t, []kafka.QuotaEntityComponent{{Type: "user", Name: nil}, {Type: "client-id", Name: sp("batch")}}, res[0].QuotaEntity)
}

// TestQuotaUnmanagedLiveUntouched asserts that a live quota entity ABSENT from
// desired produces ZERO ops (spec §39.4 — quotas never touch unmanaged entities).
func TestQuotaUnmanagedLiveUntouched(t *testing.T) {
	desired := Desired{Quotas: []quota.Desired{{
		Entity: quota.Entity{{Type: "user", Name: sp("svc-checkout")}},
		Limits: map[string]float64{"producer_byte_rate": 1024},
	}}}
	live := Live{Quotas: []kafka.QuotaState{
		{Entity: []kafka.QuotaEntityComponent{{Type: "user", Name: sp("svc-checkout")}}, Limits: map[string]float64{"producer_byte_rate": 1024}},  // in sync → no op
		{Entity: []kafka.QuotaEntityComponent{{Type: "user", Name: sp("other-service")}}, Limits: map[string]float64{"producer_byte_rate": 9999}}, // unmanaged → must NOT be touched
	}}
	ops := quotaOps(Compute(desired, live))
	require.Empty(t, ops, "unmanaged live entity must produce zero quota ops")
}

// TestQuotaDeterministic asserts that Compute produces the same quota op slice
// across multiple calls for the same input (mirrors TestSchemaDeterministic /
// TestDeterministicOrdering style).
func TestQuotaDeterministic(t *testing.T) {
	desired := Desired{Quotas: []quota.Desired{
		{
			Entity: quota.Entity{{Type: "user", Name: sp("svc-payments")}},
			Limits: map[string]float64{"producer_byte_rate": 4096, "consumer_byte_rate": 8192},
		},
		{
			Entity: quota.Entity{{Type: "client-id", Name: sp("batch-importer")}},
			Limits: map[string]float64{"request_percentage": 25},
		},
		{
			Entity: quota.Entity{{Type: "user", Name: sp("svc-checkout")}},
			Limits: map[string]float64{"producer_byte_rate": 1024},
		},
	}}
	// Partial live: one entity in sync, one drifted, one absent.
	live := Live{Quotas: []kafka.QuotaState{{
		Entity: []kafka.QuotaEntityComponent{{Type: "user", Name: sp("svc-payments")}},
		Limits: map[string]float64{"producer_byte_rate": 999},
	}}}
	a := quotaOps(Compute(desired, live))
	b := quotaOps(Compute(desired, live))
	require.Equal(t, a, b)
}
