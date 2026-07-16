package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func f(v float64) *float64 { return &v }

// TestKafkaQuotaDeepCopyRoundTrip verifies DeepCopy independence for
// KafkaQuota: full field population, pointer independence for QuotaLimits,
// and the UserDefault entity variant (spec §39).
func TestKafkaQuotaDeepCopyRoundTrip(t *testing.T) {
	t.Run("user+clientId entity", func(t *testing.T) {
		orig := &KafkaQuota{
			Spec: KafkaQuotaSpec{
				ClusterRef: ClusterRef{Name: "prod"},
				Entity: QuotaEntity{
					User:     "User:svc",
					ClientId: "batch",
				},
				Limits: QuotaLimits{
					ProducerByteRate:       f(1048576),
					ConsumerByteRate:       f(2097152),
					RequestPercentage:      f(25.5),
					ControllerMutationRate: f(10),
				},
				Reconciliation: &Reconciliation{Mode: "Enforce"},
			},
			Status: &KafkaQuotaStatus{
				Phase: PhasePending,
				ObservedLimits: &QuotaLimits{
					ProducerByteRate: f(1048576),
				},
			},
		}

		cp := orig.DeepCopy()

		require.Equal(t, orig, cp, "DeepCopy result must equal original")

		// Mutate copy limit pointer targets — original must not be affected.
		*cp.Spec.Limits.ProducerByteRate = 0
		*cp.Spec.Limits.ConsumerByteRate = 0
		*cp.Spec.Limits.RequestPercentage = 0
		*cp.Spec.Limits.ControllerMutationRate = 0
		cp.Spec.Entity.User = "mutated"
		cp.Spec.Entity.ClientId = "mutated"
		*cp.Status.ObservedLimits.ProducerByteRate = 0

		require.Equal(t, float64(1048576), *orig.Spec.Limits.ProducerByteRate, "ProducerByteRate must not be corrupted")
		require.Equal(t, float64(2097152), *orig.Spec.Limits.ConsumerByteRate, "ConsumerByteRate must not be corrupted")
		require.Equal(t, float64(25.5), *orig.Spec.Limits.RequestPercentage, "RequestPercentage must not be corrupted")
		require.Equal(t, float64(10), *orig.Spec.Limits.ControllerMutationRate, "ControllerMutationRate must not be corrupted")
		require.Equal(t, "User:svc", orig.Spec.Entity.User, "Entity.User must not be corrupted")
		require.Equal(t, "batch", orig.Spec.Entity.ClientId, "Entity.ClientId must not be corrupted")
		require.Equal(t, float64(1048576), *orig.Status.ObservedLimits.ProducerByteRate, "Status.ObservedLimits.ProducerByteRate must not be corrupted")
	})

	t.Run("userDefault entity", func(t *testing.T) {
		orig := &KafkaQuota{
			Spec: KafkaQuotaSpec{
				ClusterRef: ClusterRef{Name: "staging"},
				Entity: QuotaEntity{
					UserDefault:     true,
					ClientIdDefault: true,
				},
				Limits: QuotaLimits{
					ProducerByteRate: f(512000),
					ConsumerByteRate: f(512000),
				},
			},
		}

		cp := orig.DeepCopy()

		require.Equal(t, orig, cp, "DeepCopy result must equal original")

		// Mutate copy — original must not be affected.
		*cp.Spec.Limits.ProducerByteRate = 0
		*cp.Spec.Limits.ConsumerByteRate = 0
		cp.Spec.Entity.UserDefault = false
		cp.Spec.Entity.ClientIdDefault = false

		require.Equal(t, float64(512000), *orig.Spec.Limits.ProducerByteRate, "ProducerByteRate must not be corrupted")
		require.Equal(t, float64(512000), *orig.Spec.Limits.ConsumerByteRate, "ConsumerByteRate must not be corrupted")
		require.True(t, orig.Spec.Entity.UserDefault, "Entity.UserDefault must not be corrupted")
		require.True(t, orig.Spec.Entity.ClientIdDefault, "Entity.ClientIdDefault must not be corrupted")
	})
}
