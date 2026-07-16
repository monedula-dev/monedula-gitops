package importer_test

import (
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/importer"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/validation"
)

// strp returns a pointer to the given string, for convenience in tests.
func strp(s string) *string { return &s }

// makeQuotaSnap builds a Snapshot with no topics/ACLs and the given quotas.
func makeQuotaSnap(quotas []kafka.QuotaState) importer.Snapshot {
	return importer.Snapshot{Quotas: quotas}
}

// TestBuildQuotaBasicUserAndClientId verifies that a QuotaState with both a
// named user and a named client-id entity component reconstructs a KafkaQuota
// with the correct entity and limits fields.
func TestBuildQuotaBasicUserAndClientId(t *testing.T) {
	snap := makeQuotaSnap([]kafka.QuotaState{
		{
			Entity: []kafka.QuotaEntityComponent{
				{Type: "user", Name: strp("svc-checkout")},
				{Type: "client-id", Name: strp("batch")},
			},
			Limits: map[string]float64{
				"producer_byte_rate": 1024,
			},
		},
	})

	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Quotas) != 1 {
		t.Fatalf("want 1 quota, got %d", len(r.Quotas))
	}
	q := r.Quotas[0]
	if q.Spec.Entity.User != "User:svc-checkout" {
		t.Errorf("want entity.user = User:svc-checkout, got %q", q.Spec.Entity.User)
	}
	if q.Spec.Entity.ClientId != "batch" {
		t.Errorf("want entity.clientId = batch, got %q", q.Spec.Entity.ClientId)
	}
	if q.Spec.Entity.UserDefault {
		t.Errorf("want entity.userDefault = false, got true")
	}
	if q.Spec.Entity.ClientIdDefault {
		t.Errorf("want entity.clientIdDefault = false, got true")
	}
	if q.Spec.Limits.ProducerByteRate == nil || *q.Spec.Limits.ProducerByteRate != 1024 {
		t.Errorf("want limits.producerByteRate = 1024, got %v", q.Spec.Limits.ProducerByteRate)
	}
	if q.Spec.Limits.ConsumerByteRate != nil {
		t.Errorf("want limits.consumerByteRate = nil, got %v", q.Spec.Limits.ConsumerByteRate)
	}
	if q.Spec.ClusterRef.Name != "prod" {
		t.Errorf("want clusterRef.name = prod, got %q", q.Spec.ClusterRef.Name)
	}
	if q.Kind != "KafkaQuota" {
		t.Errorf("want Kind = KafkaQuota, got %q", q.Kind)
	}
	if q.Name == "" {
		t.Errorf("want non-empty metadata.name")
	}
	// Name must be DNS-1123 safe: lowercase, alnum and hyphens only.
	for _, c := range q.Name {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			t.Errorf("metadata.name %q contains invalid character %q", q.Name, c)
		}
	}
	if q.Name[0] == '-' || q.Name[len(q.Name)-1] == '-' {
		t.Errorf("metadata.name %q must not start or end with '-'", q.Name)
	}
}

// TestBuildQuotaUserDefaultEntity verifies that a user entity component with
// Name==nil reconstructs as userDefault: true.
func TestBuildQuotaUserDefaultEntity(t *testing.T) {
	snap := makeQuotaSnap([]kafka.QuotaState{
		{
			Entity: []kafka.QuotaEntityComponent{
				{Type: "user", Name: nil}, // nil = default
			},
			Limits: map[string]float64{
				"consumer_byte_rate": 512,
			},
		},
	})

	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Quotas) != 1 {
		t.Fatalf("want 1 quota, got %d", len(r.Quotas))
	}
	q := r.Quotas[0]
	if !q.Spec.Entity.UserDefault {
		t.Errorf("want entity.userDefault = true, got false")
	}
	if q.Spec.Entity.User != "" {
		t.Errorf("want entity.user = empty, got %q", q.Spec.Entity.User)
	}
	// Name should reflect "default".
	if q.Name == "" {
		t.Errorf("want non-empty metadata.name for default-entity quota")
	}
}

// TestBuildQuotaClientIdDefault verifies client-id-default reconstruction.
func TestBuildQuotaClientIdDefault(t *testing.T) {
	snap := makeQuotaSnap([]kafka.QuotaState{
		{
			Entity: []kafka.QuotaEntityComponent{
				{Type: "client-id", Name: nil},
			},
			Limits: map[string]float64{
				"request_percentage": 25.0,
			},
		},
	})

	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Quotas) != 1 {
		t.Fatalf("want 1 quota, got %d", len(r.Quotas))
	}
	q := r.Quotas[0]
	if !q.Spec.Entity.ClientIdDefault {
		t.Errorf("want entity.clientIdDefault = true, got false")
	}
	if q.Spec.Entity.ClientId != "" {
		t.Errorf("want entity.clientId = empty, got %q", q.Spec.Entity.ClientId)
	}
}

// TestBuildQuotaAllFourLimitKeys verifies that all four limit keys map to the
// correct QuotaLimits fields.
func TestBuildQuotaAllFourLimitKeys(t *testing.T) {
	snap := makeQuotaSnap([]kafka.QuotaState{
		{
			Entity: []kafka.QuotaEntityComponent{
				{Type: "user", Name: strp("svc")},
			},
			Limits: map[string]float64{
				"producer_byte_rate":       1000,
				"consumer_byte_rate":       2000,
				"request_percentage":       50.5,
				"controller_mutation_rate": 10,
			},
		},
	})

	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Quotas) != 1 {
		t.Fatalf("want 1 quota, got %d", len(r.Quotas))
	}
	l := r.Quotas[0].Spec.Limits
	if l.ProducerByteRate == nil || *l.ProducerByteRate != 1000 {
		t.Errorf("want producerByteRate=1000, got %v", l.ProducerByteRate)
	}
	if l.ConsumerByteRate == nil || *l.ConsumerByteRate != 2000 {
		t.Errorf("want consumerByteRate=2000, got %v", l.ConsumerByteRate)
	}
	if l.RequestPercentage == nil || *l.RequestPercentage != 50.5 {
		t.Errorf("want requestPercentage=50.5, got %v", l.RequestPercentage)
	}
	if l.ControllerMutationRate == nil || *l.ControllerMutationRate != 10 {
		t.Errorf("want controllerMutationRate=10, got %v", l.ControllerMutationRate)
	}
}

// TestBuildQuotaAbsentLimitsNil verifies that a limit key absent in the kafka
// state maps to a nil pointer in QuotaLimits.
func TestBuildQuotaAbsentLimitsNil(t *testing.T) {
	snap := makeQuotaSnap([]kafka.QuotaState{
		{
			Entity: []kafka.QuotaEntityComponent{
				{Type: "user", Name: strp("svc")},
			},
			Limits: map[string]float64{
				"producer_byte_rate": 500,
				// consumer_byte_rate, request_percentage, controller_mutation_rate are absent
			},
		},
	})

	r := importer.Build(snap, "prod", nil, nil)

	l := r.Quotas[0].Spec.Limits
	if l.ConsumerByteRate != nil {
		t.Errorf("want consumerByteRate=nil, got %v", l.ConsumerByteRate)
	}
	if l.RequestPercentage != nil {
		t.Errorf("want requestPercentage=nil, got %v", l.RequestPercentage)
	}
	if l.ControllerMutationRate != nil {
		t.Errorf("want controllerMutationRate=nil, got %v", l.ControllerMutationRate)
	}
}

// TestBuildQuotaDeterminism verifies that two Build calls with the same
// snapshot produce byte-identical quota slices.
func TestBuildQuotaDeterminism(t *testing.T) {
	snap := makeQuotaSnap([]kafka.QuotaState{
		{
			Entity: []kafka.QuotaEntityComponent{
				{Type: "user", Name: strp("svc-b")},
			},
			Limits: map[string]float64{"producer_byte_rate": 100},
		},
		{
			Entity: []kafka.QuotaEntityComponent{
				{Type: "user", Name: strp("svc-a")},
			},
			Limits: map[string]float64{"consumer_byte_rate": 200},
		},
	})

	r1 := importer.Build(snap, "prod", nil, nil)
	r2 := importer.Build(snap, "prod", nil, nil)

	if len(r1.Quotas) != len(r2.Quotas) {
		t.Fatalf("determinism: quota count differs %d vs %d", len(r1.Quotas), len(r2.Quotas))
	}
	for i := range r1.Quotas {
		if r1.Quotas[i].Name != r2.Quotas[i].Name {
			t.Errorf("determinism: quota[%d].name differs: %q vs %q", i, r1.Quotas[i].Name, r2.Quotas[i].Name)
		}
	}
	// Verify they're sorted by name.
	for i := 1; i < len(r1.Quotas); i++ {
		if r1.Quotas[i-1].Name >= r1.Quotas[i].Name {
			t.Errorf("quotas not sorted: %q >= %q", r1.Quotas[i-1].Name, r1.Quotas[i].Name)
		}
	}
}

// TestBuildQuotaEmptySnap verifies that a snapshot with no quotas produces
// a nil (or empty) Quotas slice, not a crash.
func TestBuildQuotaEmptySnap(t *testing.T) {
	r := importer.Build(importer.Snapshot{}, "prod", nil, nil)
	if len(r.Quotas) != 0 {
		t.Errorf("want 0 quotas for empty snapshot, got %d", len(r.Quotas))
	}
}

// TestImportIPQuota verifies that a QuotaState with an ip entity component and
// connection_creation_rate limit reconstructs the correct KafkaQuota.
func TestImportIPQuota(t *testing.T) {
	ip := "10.0.0.1"
	snap := makeQuotaSnap([]kafka.QuotaState{{
		Entity: []kafka.QuotaEntityComponent{{Type: "ip", Name: &ip}},
		Limits: map[string]float64{"connection_creation_rate": 100},
	}})
	r := importer.Build(snap, "prod", nil, nil)
	if len(r.Quotas) != 1 {
		t.Fatalf("want 1 quota, got %d", len(r.Quotas))
	}
	q := r.Quotas[0]
	if q.Spec.Entity.Ip != "10.0.0.1" {
		t.Errorf("want entity.ip = 10.0.0.1, got %q", q.Spec.Entity.Ip)
	}
	if q.Spec.Entity.IpDefault {
		t.Errorf("want entity.ipDefault = false, got true")
	}
	if q.Spec.Limits.ConnectionCreationRate == nil {
		t.Fatal("want limits.connectionCreationRate to be set, got nil")
	}
	if *q.Spec.Limits.ConnectionCreationRate != 100 {
		t.Errorf("want limits.connectionCreationRate = 100, got %v", *q.Spec.Limits.ConnectionCreationRate)
	}
	if !strings.Contains(q.Name, "ip") {
		t.Errorf("metadata.name %q should contain 'ip'", q.Name)
	}
}

// TestImportIPDefaultQuota verifies that a QuotaState with an ip entity
// component where Name==nil reconstructs as ipDefault: true.
func TestImportIPDefaultQuota(t *testing.T) {
	snap := makeQuotaSnap([]kafka.QuotaState{{
		Entity: []kafka.QuotaEntityComponent{{Type: "ip", Name: nil}},
		Limits: map[string]float64{"connection_creation_rate": 50},
	}})
	r := importer.Build(snap, "prod", nil, nil)
	if len(r.Quotas) != 1 {
		t.Fatalf("want 1 quota, got %d", len(r.Quotas))
	}
	q := r.Quotas[0]
	if !q.Spec.Entity.IpDefault {
		t.Errorf("want entity.ipDefault = true, got false")
	}
	if q.Spec.Entity.Ip != "" {
		t.Errorf("want entity.ip = empty, got %q", q.Spec.Entity.Ip)
	}
	if q.Name == "" {
		t.Errorf("want non-empty metadata.name for ip-default entity quota")
	}
}

// TestBuildQuotaPassesValidateQuotaShape verifies the round-trip property:
// every reconstructed KafkaQuota must pass validation.ValidateQuotaShape.
func TestBuildQuotaPassesValidateQuotaShape(t *testing.T) {
	snap := makeQuotaSnap([]kafka.QuotaState{
		{
			Entity: []kafka.QuotaEntityComponent{
				{Type: "user", Name: strp("svc-checkout")},
				{Type: "client-id", Name: strp("batch")},
			},
			Limits: map[string]float64{"producer_byte_rate": 1024},
		},
		{
			Entity: []kafka.QuotaEntityComponent{
				{Type: "user", Name: nil}, // user-default
			},
			Limits: map[string]float64{"consumer_byte_rate": 512},
		},
		{
			Entity: []kafka.QuotaEntityComponent{
				{Type: "client-id", Name: nil}, // client-id-default
			},
			Limits: map[string]float64{"request_percentage": 10},
		},
		{
			Entity: []kafka.QuotaEntityComponent{
				{Type: "user", Name: strp("svc-admin")},
			},
			Limits: map[string]float64{
				"producer_byte_rate":       2048,
				"consumer_byte_rate":       4096,
				"request_percentage":       25,
				"controller_mutation_rate": 5,
			},
		},
		{
			Entity: []kafka.QuotaEntityComponent{
				{Type: "ip", Name: strp("192.168.1.1")},
			},
			Limits: map[string]float64{"connection_creation_rate": 200},
		},
		{
			Entity: []kafka.QuotaEntityComponent{
				{Type: "ip", Name: nil}, // ip-default
			},
			Limits: map[string]float64{"connection_creation_rate": 75},
		},
	})

	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Quotas) != 6 {
		t.Fatalf("want 6 quotas, got %d", len(r.Quotas))
	}
	for _, q := range r.Quotas {
		if errs := validation.ValidateQuotaShape(q); len(errs) != 0 {
			t.Errorf("quota %q failed ValidateQuotaShape: %v", q.Name, errs)
		}
	}
}

// TestBuildQuotaDefaultVsLiteralNameDistinct verifies the by-construction fix
// for the default-sentinel-vs-literal-name collision: {userDefault: true} and
// {user: "default"} must NOT produce the same metadata.name (the old scheme
// joined both to the literal string "user-default").
func TestBuildQuotaDefaultVsLiteralNameDistinct(t *testing.T) {
	snap := makeQuotaSnap([]kafka.QuotaState{
		{
			Entity: []kafka.QuotaEntityComponent{{Type: "user", Name: nil}}, // userDefault
			Limits: map[string]float64{"producer_byte_rate": 1},
		},
		{
			Entity: []kafka.QuotaEntityComponent{{Type: "user", Name: strp("default")}}, // literal "default"
			Limits: map[string]float64{"producer_byte_rate": 2},
		},
	})

	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Quotas) != 2 {
		t.Fatalf("want 2 quotas, got %d", len(r.Quotas))
	}
	if r.Quotas[0].Name == r.Quotas[1].Name {
		t.Fatalf("userDefault and literal user=default must have distinct names, both are %q", r.Quotas[0].Name)
	}
	// Confirm entity identity is preserved for each (order is by sorted name,
	// so check both are present rather than assuming index).
	var sawDefault, sawLiteral bool
	for _, q := range r.Quotas {
		switch {
		case q.Spec.Entity.UserDefault:
			sawDefault = true
		case q.Spec.Entity.User == "User:default":
			sawLiteral = true
		}
	}
	if !sawDefault || !sawLiteral {
		t.Fatalf("want one userDefault and one literal User:default entity, got %+v / %+v", r.Quotas[0].Spec.Entity, r.Quotas[1].Spec.Entity)
	}
}

// TestBuildQuotaCrossComponentCollisionDisambiguated verifies the
// disambiguateName safety net: entity {clientId: "b-user-a"} and entity
// {user: "a", clientId: "b"} both derive the joined key "client-id-b-user-a"
// (a genuine cross-component alias that no cheap by-construction fix
// resolves), so buildQuotas must disambiguate them deterministically instead
// of letting one silently clobber the other's file on write.
func TestBuildQuotaCrossComponentCollisionDisambiguated(t *testing.T) {
	snap := makeQuotaSnap([]kafka.QuotaState{
		{
			Entity: []kafka.QuotaEntityComponent{{Type: "client-id", Name: strp("b-user-a")}},
			Limits: map[string]float64{"producer_byte_rate": 1},
		},
		{
			Entity: []kafka.QuotaEntityComponent{
				{Type: "user", Name: strp("a")},
				{Type: "client-id", Name: strp("b")},
			},
			Limits: map[string]float64{"producer_byte_rate": 2},
		},
	})

	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Quotas) != 2 {
		t.Fatalf("want 2 quotas, got %d", len(r.Quotas))
	}
	if r.Quotas[0].Name == r.Quotas[1].Name {
		t.Fatalf("cross-component-colliding entities must have distinct names, both are %q", r.Quotas[0].Name)
	}
	var collisionWarn bool
	for _, w := range r.Warnings {
		if strings.Contains(w, "quota name collision") {
			collisionWarn = true
		}
	}
	if !collisionWarn {
		t.Fatalf("want a quota name collision warning, got %v", r.Warnings)
	}
	// Both entities must still be present with their correct shape.
	var sawClientOnly, sawUserAndClient bool
	for _, q := range r.Quotas {
		switch {
		case q.Spec.Entity.ClientId == "b-user-a" && q.Spec.Entity.User == "":
			sawClientOnly = true
		case q.Spec.Entity.User == "User:a" && q.Spec.Entity.ClientId == "b":
			sawUserAndClient = true
		}
	}
	if !sawClientOnly || !sawUserAndClient {
		t.Fatalf("want both distinct entities preserved, got %+v / %+v", r.Quotas[0].Spec.Entity, r.Quotas[1].Spec.Entity)
	}
}
