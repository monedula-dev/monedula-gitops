// KafkaMirrorTopic admission webhook. This validating webhook enforces, at
// admission time, invariants the reconciler can only surface terminally after
// the fact:
//
//   - Shape: required fields, replication factor, config keys, reconciliation
//     mode, and deletionPolicy (only Orphan is accepted). Reuses
//     validation.ValidateMirrorTopicShape so the CLI lint and the webhook agree.
//   - Immutability: spec.clusterRef.name, spec.linkName, spec.sourceTopicName
//     and the effective destination topic name cannot change on update.
//   - Identity uniqueness across BOTH kinds: at most one live KafkaTopic or
//     KafkaMirrorTopic may claim a given (resolved cluster, topic name). The
//     claim key is not kind-qualified.
//   - Tenancy: the effective destination name must satisfy the referenced
//     cluster's tenancy policy. The source topic lives on another cluster and
//     is not tenancy-checked.
//
// Deleting a KafkaMirrorTopic always orphans the Kafka topic, so ValidateDelete
// always allows. As with the KafkaTopic webhook, reads go through the manager
// cache, so uniqueness is best-effort at admission and the reconciler's
// claim resolution is the backstop.
package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/tenancy"
	"github.com/monedula-dev/monedula-gitops/internal/validation"
)

// +kubebuilder:webhook:path=/validate-gitops-monedula-dev-v1alpha1-kafkamirrortopic,mutating=false,failurePolicy=fail,sideEffects=None,groups=gitops.monedula.dev,resources=kafkamirrortopics,verbs=create;update,versions=v1alpha1,name=vkafkamirrortopic.gitops.monedula.dev,admissionReviewVersions=v1

// KafkaMirrorTopicValidator is the admission.Validator[*v1alpha1.KafkaMirrorTopic].
// ClusterNamespace has the same meaning as on KafkaTopicValidator and MUST
// match the value passed to the reconcilers.
type KafkaMirrorTopicValidator struct {
	// Reader is the manager's cached client. It must have the ClusterRefNameIndex
	// and MirrorTopicClusterRefNameIndex field indexes registered.
	Reader client.Reader
	// ClusterNamespace is where KafkaCluster CRs are resolved from. Empty means
	// each resource's own namespace.
	ClusterNamespace string
}

var _ admission.Validator[*v1alpha1.KafkaMirrorTopic] = &KafkaMirrorTopicValidator{}

func (v *KafkaMirrorTopicValidator) clusterNamespaceFor(ns string) string {
	if v.ClusterNamespace != "" {
		return v.ClusterNamespace
	}
	return ns
}

// ValidateCreate enforces shape, cross-kind identity uniqueness and tenancy.
func (v *KafkaMirrorTopicValidator) ValidateCreate(ctx context.Context, m *v1alpha1.KafkaMirrorTopic) (admission.Warnings, error) {
	if err := checkMirrorShape(m); err != nil {
		return nil, err
	}
	if err := v.checkIdentityUnique(ctx, m); err != nil {
		return nil, err
	}
	return nil, v.checkTenancy(ctx, m)
}

// ValidateUpdate enforces shape, identity immutability, cross-kind identity
// uniqueness and tenancy. Once the object is being deleted only immutability
// is checked: shape rules, a competing claim or a tightened tenancy policy can
// never reject a finalizer or annotation update on a deleting CR.
func (v *KafkaMirrorTopicValidator) ValidateUpdate(ctx context.Context, oldM, newM *v1alpha1.KafkaMirrorTopic) (admission.Warnings, error) {
	deleting := !newM.DeletionTimestamp.IsZero()
	if !deleting {
		if err := checkMirrorShape(newM); err != nil {
			return nil, err
		}
	}
	if err := checkMirrorImmutable(oldM, newM); err != nil {
		return nil, err
	}
	if deleting {
		return nil, nil
	}
	if err := v.checkIdentityUnique(ctx, newM); err != nil {
		return nil, err
	}
	return nil, v.checkTenancy(ctx, newM)
}

// ValidateDelete always allows: deleting a mirror topic CR only orphans the
// Kafka topic and can never violate an admission invariant.
func (v *KafkaMirrorTopicValidator) ValidateDelete(_ context.Context, _ *v1alpha1.KafkaMirrorTopic) (admission.Warnings, error) {
	return nil, nil
}

// checkMirrorShape runs the shared shape checks. Objects received at admission
// may carry an empty TypeMeta, so apiVersion is filled on a shallow copy.
func checkMirrorShape(m *v1alpha1.KafkaMirrorTopic) error {
	check := *m
	if check.APIVersion == "" {
		check.APIVersion = v1alpha1.APIVersion
	}
	shapeErrs := validation.ValidateMirrorTopicShape(&check)
	if len(shapeErrs) == 0 {
		return nil
	}
	msgs := make([]string, 0, len(shapeErrs))
	for _, e := range shapeErrs {
		msgs = append(msgs, e.Error())
	}
	return errors.New(strings.Join(msgs, "; "))
}

func checkMirrorImmutable(oldM, newM *v1alpha1.KafkaMirrorTopic) error {
	pre := fmt.Sprintf("KafkaMirrorTopic %s/%s", newM.Namespace, newM.Name)
	if o, n := oldM.Spec.ClusterRef.Name, newM.Spec.ClusterRef.Name; o != n {
		return fmt.Errorf("%s: spec.clusterRef.name is immutable: %s -> %s", pre, o, n)
	}
	if o, n := oldM.Spec.LinkName, newM.Spec.LinkName; o != n {
		return fmt.Errorf("%s: spec.linkName is immutable: %s -> %s", pre, o, n)
	}
	if o, n := oldM.Spec.SourceTopicName, newM.Spec.SourceTopicName; o != n {
		return fmt.Errorf("%s: spec.sourceTopicName is immutable: %s -> %s", pre, o, n)
	}
	// Compare EFFECTIVE names so an empty -> explicit-same topicName is not a change.
	if o, n := oldM.Spec.EffectiveTopicName(), newM.Spec.EffectiveTopicName(); o != n {
		return fmt.Errorf("%s: spec.topicName is immutable: %s -> %s (the effective destination topic name cannot change)", pre, o, n)
	}
	return nil
}

// checkIdentityUnique rejects when ANOTHER live KafkaMirrorTopic or KafkaTopic
// claims the same effective topic name on the same resolved cluster (cluster
// name AND effective cluster namespace; see KafkaTopicValidator.checkIdentityUnique
// for the namespace-scoping rules). Candidates already being deleted still
// occupy the identity, consistent with the KafkaTopic webhook.
func (v *KafkaMirrorTopicValidator) checkIdentityUnique(ctx context.Context, m *v1alpha1.KafkaMirrorTopic) error {
	wantName := m.Spec.EffectiveTopicName()
	wantClusterRef := m.Spec.ClusterRef.Name
	wantClusterNS := v.clusterNamespaceFor(m.Namespace)

	var mirrors v1alpha1.KafkaMirrorTopicList
	if err := v.Reader.List(ctx, &mirrors,
		client.MatchingFields{MirrorTopicClusterRefNameIndex: wantClusterRef},
	); err != nil {
		return fmt.Errorf("listing KafkaMirrorTopics for identity check: %w", err)
	}
	for i := range mirrors.Items {
		other := &mirrors.Items[i]
		if other.UID == m.UID {
			continue
		}
		if other.Spec.EffectiveTopicName() != wantName || v.clusterNamespaceFor(other.Namespace) != wantClusterNS {
			continue
		}
		return fmt.Errorf(
			"KafkaMirrorTopic %s/%s conflicts with KafkaMirrorTopic %s/%s: both claim topic %q on cluster %q (namespace %q); topic identity (cluster, topicName) must be unique",
			m.Namespace, m.Name, other.Namespace, other.Name, wantName, wantClusterRef, wantClusterNS)
	}

	var topics v1alpha1.KafkaTopicList
	if err := v.Reader.List(ctx, &topics,
		client.MatchingFields{ClusterRefNameIndex: wantClusterRef},
	); err != nil {
		return fmt.Errorf("listing KafkaTopics for identity check: %w", err)
	}
	for i := range topics.Items {
		other := &topics.Items[i]
		if ResolvedTopicName(other) != wantName || v.clusterNamespaceFor(other.Namespace) != wantClusterNS {
			continue
		}
		return fmt.Errorf(
			"KafkaMirrorTopic %s/%s conflicts with KafkaTopic %s/%s: both claim topic %q on cluster %q (namespace %q); topic identity (cluster, topicName) must be unique",
			m.Namespace, m.Name, other.Namespace, other.Name, wantName, wantClusterRef, wantClusterNS)
	}
	return nil
}

// checkTenancy runs tenancy.Check on the effective destination name. A missing
// cluster allows the operation (the reconciler surfaces it), like topics.
func (v *KafkaMirrorTopicValidator) checkTenancy(ctx context.Context, m *v1alpha1.KafkaMirrorTopic) error {
	var cluster v1alpha1.KafkaCluster
	key := types.NamespacedName{Namespace: v.clusterNamespaceFor(m.Namespace), Name: m.Spec.ClusterRef.Name}
	if err := v.Reader.Get(ctx, key, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("resolving clusterRef for tenancy check: %w", err)
	}
	return tenancy.Check(cluster.Spec.Tenancy, m.Namespace, m.Spec.EffectiveTopicName())
}
