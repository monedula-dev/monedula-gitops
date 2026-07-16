package importer

import (
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
)

// roleBindingManifest builds an explicit KafkaRoleBinding manifest from a live
// MDS role binding (spec §40 import). Cluster-scoped bindings (nil Resource)
// carry no resources; resource-scoped bindings carry one. The metadata.name is
// deterministic and DNS-1123-safe:
// imported-rb-<principal>-<role>[-<type>-<name>-<patternType>].
func roleBindingManifest(rb mds.RoleBinding, clusterName string) *v1alpha1.KafkaRoleBinding {
	spec := v1alpha1.KafkaRoleBindingSpec{
		ClusterRef:     v1alpha1.ClusterRef{Name: clusterName},
		Principal:      rb.Principal,
		Role:           rb.Role,
		Scope:          v1alpha1.RoleBindingScope{Type: rb.Scope.Type},
		DeletionPolicy: "Orphan",
	}
	nameParts := []string{rb.Principal, rb.Role}
	if rb.Resource != nil {
		spec.Resources = []v1alpha1.RoleResource{{
			Type:        rb.Resource.Type,
			Name:        rb.Resource.Name,
			PatternType: rb.Resource.PatternType,
		}}
		nameParts = append(nameParts, rb.Resource.Type, rb.Resource.Name, rb.Resource.PatternType)
	}
	name := "imported-rb-" + slug(strings.Join(nameParts, "-"))
	if len(name) > 253 {
		name = strings.Trim(name[:253], "-")
	}
	return &v1alpha1.KafkaRoleBinding{
		TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: map[string]string{importedFromAnnotation: clusterName},
		},
		Spec: spec,
	}
}
