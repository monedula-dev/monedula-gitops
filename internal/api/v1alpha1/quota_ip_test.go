package v1alpha1

import "testing"

func TestQuotaEntityIPFieldsRoundTripDeepCopy(t *testing.T) {
	rate := 100.0
	q := &KafkaQuota{
		Spec: KafkaQuotaSpec{
			Entity: QuotaEntity{Ip: "10.0.0.1"},
			Limits: QuotaLimits{ConnectionCreationRate: &rate},
		},
	}
	cp := q.DeepCopy()
	if cp.Spec.Entity.Ip != "10.0.0.1" {
		t.Fatalf("Ip not copied: %q", cp.Spec.Entity.Ip)
	}
	if cp.Spec.Limits.ConnectionCreationRate == nil || *cp.Spec.Limits.ConnectionCreationRate != 100.0 {
		t.Fatalf("ConnectionCreationRate not deep-copied")
	}
	*cp.Spec.Limits.ConnectionCreationRate = 200.0
	if *q.Spec.Limits.ConnectionCreationRate != 100.0 {
		t.Fatalf("deep copy shares the *float64 pointer")
	}
	q.Spec.Entity.IpDefault = true
	if cp.Spec.Entity.IpDefault {
		t.Fatalf("IpDefault should be independent")
	}
}
