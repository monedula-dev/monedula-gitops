package manager

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// TestBuildScheme verifies the operator scheme is constructible without a real
// cluster and that all three Monedula GitOps kinds (plus their List kinds) are
// registered alongside the core Kubernetes types.
func TestBuildScheme(t *testing.T) {
	scheme, err := BuildScheme()
	require.NoError(t, err)
	require.NotNil(t, scheme)

	// Each v1alpha1 kind must resolve to a registered GVK in the API group.
	for _, obj := range []runtime.Object{
		&v1alpha1.KafkaCluster{},
		&v1alpha1.KafkaClusterList{},
		&v1alpha1.KafkaTopic{},
		&v1alpha1.KafkaTopicList{},
		&v1alpha1.KafkaAccessPolicy{},
		&v1alpha1.KafkaAccessPolicyList{},
	} {
		gvks, _, err := scheme.ObjectKinds(obj)
		require.NoError(t, err)
		require.NotEmpty(t, gvks)
		require.Equal(t, v1alpha1.GroupVersion.Group, gvks[0].Group)
		require.Equal(t, v1alpha1.GroupVersion.Version, gvks[0].Version)
	}

	// A core Kubernetes type must also be registered (client-go scheme), proving
	// the manager can read Secrets/Events.
	gvks, _, err := scheme.ObjectKinds(&corev1.Secret{})
	require.NoError(t, err)
	require.NotEmpty(t, gvks)
}
