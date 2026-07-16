package quota

import (
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/stretchr/testify/require"
)

func f(v float64) *float64 { return &v }

func TestCompileUserStripsPrefix(t *testing.T) {
	q := &v1alpha1.KafkaQuota{}
	q.Spec.Entity = v1alpha1.QuotaEntity{User: "User:svc-checkout", ClientId: "batch"}
	q.Spec.Limits = v1alpha1.QuotaLimits{ProducerByteRate: f(1024)}
	d := Compile(q)
	require.Equal(t, Entity{{Type: "user", Name: strptr("svc-checkout")}, {Type: "client-id", Name: strptr("batch")}}, d.Entity)
	require.Equal(t, map[string]float64{"producer_byte_rate": 1024}, d.Limits)
}

func TestCompileDefaultsNilName(t *testing.T) {
	q := &v1alpha1.KafkaQuota{}
	q.Spec.Entity = v1alpha1.QuotaEntity{UserDefault: true}
	q.Spec.Limits = v1alpha1.QuotaLimits{ConsumerByteRate: f(2048)}
	d := Compile(q)
	require.Equal(t, Entity{{Type: "user", Name: nil}}, d.Entity)
	require.Equal(t, map[string]float64{"consumer_byte_rate": 2048}, d.Limits)
}

func TestCompileAllLimitsAndMode(t *testing.T) {
	q := &v1alpha1.KafkaQuota{}
	q.Spec.Entity = v1alpha1.QuotaEntity{ClientIdDefault: true}
	q.Spec.Limits = v1alpha1.QuotaLimits{ProducerByteRate: f(1), ConsumerByteRate: f(2), RequestPercentage: f(3), ControllerMutationRate: f(4)}
	q.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: "DetectOnly"}
	d := Compile(q)
	require.Equal(t, Entity{{Type: "client-id", Name: nil}}, d.Entity)
	require.Equal(t, map[string]float64{"producer_byte_rate": 1, "consumer_byte_rate": 2, "request_percentage": 3, "controller_mutation_rate": 4}, d.Limits)
	require.Equal(t, "DetectOnly", d.Mode)
}

func TestEntityKeyDeterministicAndSorted(t *testing.T) {
	a := Entity{{Type: "client-id", Name: strptr("b")}, {Type: "user", Name: strptr("a")}}
	b := Entity{{Type: "user", Name: strptr("a")}, {Type: "client-id", Name: strptr("b")}}
	require.Equal(t, a.Key(), b.Key())
	require.Equal(t, "client-id=b|user=a", a.Key())
}

func TestEntityKeyDefaultRendered(t *testing.T) {
	require.Equal(t, "user=<default>", Entity{{Type: "user", Name: nil}}.Key())
}

func TestCompileIPEntity(t *testing.T) {
	q := &v1alpha1.KafkaQuota{Spec: v1alpha1.KafkaQuotaSpec{
		Entity: v1alpha1.QuotaEntity{Ip: "10.0.0.1"},
		Limits: v1alpha1.QuotaLimits{ConnectionCreationRate: f(100)},
	}}
	d := Compile(q)
	if len(d.Entity) != 1 || d.Entity[0].Type != "ip" || d.Entity[0].Name == nil || *d.Entity[0].Name != "10.0.0.1" {
		t.Fatalf("ip component wrong: %+v", d.Entity)
	}
	if d.Limits["connection_creation_rate"] != 100 {
		t.Fatalf("connection_creation_rate missing: %+v", d.Limits)
	}
}

func TestCompileIPDefaultEntity(t *testing.T) {
	q := &v1alpha1.KafkaQuota{Spec: v1alpha1.KafkaQuotaSpec{
		Entity: v1alpha1.QuotaEntity{IpDefault: true},
		Limits: v1alpha1.QuotaLimits{ConnectionCreationRate: f(50)},
	}}
	d := Compile(q)
	if len(d.Entity) != 1 || d.Entity[0].Type != "ip" || d.Entity[0].Name != nil {
		t.Fatalf("ipDefault component wrong: %+v", d.Entity)
	}
}

func TestCompileIPKeyDistinct(t *testing.T) {
	ipQ := Compile(&v1alpha1.KafkaQuota{Spec: v1alpha1.KafkaQuotaSpec{Entity: v1alpha1.QuotaEntity{Ip: "10.0.0.1"}, Limits: v1alpha1.QuotaLimits{ConnectionCreationRate: f(1)}}})
	userQ := Compile(&v1alpha1.KafkaQuota{Spec: v1alpha1.KafkaQuotaSpec{Entity: v1alpha1.QuotaEntity{User: "User:10.0.0.1"}, Limits: v1alpha1.QuotaLimits{ProducerByteRate: f(1)}}})
	if ipQ.Entity.Key() == userQ.Entity.Key() {
		t.Fatalf("ip and user identities must differ: %q", ipQ.Entity.Key())
	}
}
