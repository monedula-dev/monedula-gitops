package webhook

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/operator/index"
)

// RoleBindingClusterRefNameIndex is the manager cache field-index key on a
// KafkaRoleBinding's spec.clusterRef.name — re-exported from
// internal/operator/index (the neutral home of the shared field indexes) for
// this package's validators and their tests.
const RoleBindingClusterRefNameIndex = index.RoleBindingClusterRefNameIndex

// UserClusterRefNameIndex is the manager cache field-index key on a
// KafkaUser's spec.clusterRef.name — re-exported from
// internal/operator/index for this package's validators and their tests.
const UserClusterRefNameIndex = index.UserClusterRefNameIndex

// RegisterIndexes forwards to index.RegisterIndexes, the single registration
// of the shared spec.clusterRef.name field indexes. Kept so historical
// callers of the webhook package keep working; new code should call
// internal/operator/index directly (controllers must — importing the webhook
// package for an index constant was the layering inversion the index package
// fixes). Call once, before mgr.Start.
func RegisterIndexes(ctx context.Context, mgr ctrl.Manager) error {
	return index.RegisterIndexes(ctx, mgr)
}

// SetupWithManager registers the KafkaTopic validating webhook with mgr. The
// generated webhook path is /validate-gitops-monedula-dev-v1alpha1-kafkatopic
// (kept consistent with the +kubebuilder:webhook marker so Task 11's generated
// ValidatingWebhookConfiguration points at the right path).
func (v *KafkaTopicValidator) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &v1alpha1.KafkaTopic{}).
		WithValidator(v).
		Complete()
}

// SetupWithManager registers the KafkaQuota validating webhook with mgr. The
// generated webhook path is /validate-gitops-monedula-dev-v1alpha1-kafkaquota
// (kept consistent with the +kubebuilder:webhook marker so the generated
// ValidatingWebhookConfiguration points at the right path).
func (v *KafkaQuotaValidator) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &v1alpha1.KafkaQuota{}).
		WithValidator(v).
		Complete()
}

// SetupWithManager registers the KafkaAccessPolicy validating webhook with mgr.
// The generated webhook path is
// /validate-gitops-monedula-dev-v1alpha1-kafkaaccesspolicy (kept consistent
// with the +kubebuilder:webhook marker so the generated
// ValidatingWebhookConfiguration points at the right path).
func (v *KafkaAccessPolicyValidator) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &v1alpha1.KafkaAccessPolicy{}).
		WithValidator(v).
		Complete()
}

// SetupWithManager registers the KafkaRoleBinding validating webhook with mgr.
// The generated webhook path is
// /validate-gitops-monedula-dev-v1alpha1-kafkarolebinding (kept consistent with
// the +kubebuilder:webhook marker so the generated ValidatingWebhookConfiguration
// points at the right path).
func (v *KafkaRoleBindingValidator) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &v1alpha1.KafkaRoleBinding{}).
		WithValidator(v).
		Complete()
}

// SetupWithManager registers the KafkaUser validating webhook with mgr. The
// generated webhook path is /validate-gitops-monedula-dev-v1alpha1-kafkauser
// (kept consistent with the +kubebuilder:webhook marker so the generated
// ValidatingWebhookConfiguration points at the right path).
func (v *KafkaUserValidator) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &v1alpha1.KafkaUser{}).
		WithValidator(v).
		Complete()
}
