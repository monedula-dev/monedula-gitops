package importer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/validation"
)

func TestRoleBindingManifestResourceScoped(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:svc", Role: "ResourceOwner",
		Scope:    mds.Scope{Type: "kafka", KafkaCluster: "kid"},
		Resource: &mds.ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
	}
	m := roleBindingManifest(rb, "prod")
	require.Equal(t, "prod", m.Spec.ClusterRef.Name)
	require.Equal(t, "User:svc", m.Spec.Principal)
	require.Equal(t, "ResourceOwner", m.Spec.Role)
	require.Equal(t, "kafka", m.Spec.Scope.Type)
	require.Len(t, m.Spec.Resources, 1)
	require.Equal(t, "Topic", m.Spec.Resources[0].Type)
	require.Equal(t, "orders", m.Spec.Resources[0].Name)
	require.Equal(t, "literal", m.Spec.Resources[0].PatternType)
	require.Equal(t, "Orphan", m.Spec.DeletionPolicy)
	require.Equal(t, "KafkaRoleBinding", m.Kind)
	require.Contains(t, m.Name, "svc")
	require.Contains(t, m.Name, "resourceowner")
	require.Equal(t, "prod", m.Annotations[importedFromAnnotation])
	require.Equal(t, v1alpha1.APIVersion, m.APIVersion)
}

func TestRoleBindingManifestClusterScoped(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:admin", Role: "SystemAdmin",
		Scope: mds.Scope{Type: "kafka", KafkaCluster: "kid"},
	}
	m := roleBindingManifest(rb, "prod")
	require.Empty(t, m.Spec.Resources)
	require.Equal(t, "SystemAdmin", m.Spec.Role)
}

func TestRoleBindingManifestNameDeterministicAndSafe(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:svc.checkout", Role: "DeveloperWrite",
		Scope:    mds.Scope{Type: "kafka", KafkaCluster: "kid"},
		Resource: &mds.ResourcePattern{Type: "Topic", Name: "orders.v1", PatternType: "literal"},
	}
	a := roleBindingManifest(rb, "prod")
	b := roleBindingManifest(rb, "prod")
	require.Equal(t, a.Name, b.Name)
	require.NotContains(t, a.Name, ":")
	require.NotContains(t, a.Name, ".")
}

func TestRoleBindingManifestPassesShapeValidation(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:svc", Role: "ResourceOwner",
		Scope:    mds.Scope{Type: "kafka", KafkaCluster: "kid"},
		Resource: &mds.ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
	}
	m := roleBindingManifest(rb, "prod")
	errs := validation.ValidateRoleBindingShape(m)
	require.Empty(t, errs)
}

func TestRoleBindingManifestPatternTypeInName(t *testing.T) {
	base := mds.RoleBinding{
		Principal: "User:svc", Role: "ResourceOwner",
		Scope: mds.Scope{Type: "kafka", KafkaCluster: "kid"},
	}
	literal := base
	literal.Resource = &mds.ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"}
	prefixed := base
	prefixed.Resource = &mds.ResourcePattern{Type: "Topic", Name: "orders", PatternType: "prefixed"}

	mLiteral := roleBindingManifest(literal, "prod")
	mPrefixed := roleBindingManifest(prefixed, "prod")
	require.NotEqual(t, mLiteral.Name, mPrefixed.Name)
}
