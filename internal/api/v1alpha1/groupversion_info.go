// Package v1alpha1 contains the Monedula GitOps API types.
// +kubebuilder:object:generate=true
// +groupName=gitops.monedula.dev
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the group/version for the Monedula GitOps API.
	GroupVersion = schema.GroupVersion{Group: "gitops.monedula.dev", Version: "v1alpha1"}
	// SchemeBuilder registers the API types into a runtime scheme. Uses the
	// apimachinery builder directly (sigs.k8s.io/controller-runtime/pkg/scheme.Builder
	// is deprecated in favor of this lower-dependency form).
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme adds the API types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&KafkaCluster{}, &KafkaClusterList{},
		&KafkaTopic{}, &KafkaTopicList{},
		&KafkaAccessPolicy{}, &KafkaAccessPolicyList{},
		&KafkaQuota{}, &KafkaQuotaList{},
		&KafkaRoleBinding{}, &KafkaRoleBindingList{},
		&KafkaUser{}, &KafkaUserList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
