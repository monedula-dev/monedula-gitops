// KafkaAccessPolicy shape + cross-resource ACL-conflict admission webhook (spec
// §20.3). It is a validating webhook that enforces, at admission time (before
// the object is persisted), two invariants the reconciler can only surface
// terminally after the fact:
//
//   - Shape (§20.3): per-resource rule well-formedness (non-empty rules, valid
//     resource types, permission, operations). Reuses
//     validation.ValidateAccessPolicyShape so the CLI lint and the webhook agree.
//   - Cross-resource conflict: the incoming policy must not introduce an
//     Allow/Deny disagreement on a tuple already claimed with the opposite
//     permission by another KafkaTopic or KafkaAccessPolicy on the same cluster.
//     This is the admission-time backstop for the ACLConflict condition the
//     reconciler surfaces terminally (spec §21). Without this, two resources
//     disagreeing on a tuple reach the broker in an undefined order.
//
// The validator reads only from the manager's cache (a client.Reader), so it
// never blocks admission on a live apiserver round-trip beyond the cache.
//
// # TOCTOU caveat
//
// Like the KafkaTopic and KafkaQuota validators, the conflict check reads from
// the manager CACHE, not the live apiserver. Two near-simultaneous Creates can
// therefore both pass (each sees a stale cache that does not yet contain the
// other's object). Admission shrinks the conflict window significantly but is
// NOT a linearizable guarantee. The reconciler's ACLConflict condition (spec
// §21) remains the backstop for the rare conflict that slips through during a
// cache-lag window.
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
	"github.com/monedula-dev/monedula-gitops/internal/operator/index"
	"github.com/monedula-dev/monedula-gitops/internal/operator/reconcile"
	"github.com/monedula-dev/monedula-gitops/internal/validation"
)

// PolicyClusterRefNameIndex is the manager cache field-index key on a
// KafkaAccessPolicy's spec.clusterRef.name. It mirrors ClusterRefNameIndex for
// topics and QuotaClusterRefNameIndex for quotas: the validator lists candidate
// conflict parties filtered by this index so it scans only policies referencing
// the same cluster ref, not every policy. Re-exported from
// internal/operator/index, the neutral home of the shared field indexes.
const PolicyClusterRefNameIndex = index.PolicyClusterRefNameIndex

// +kubebuilder:webhook:path=/validate-gitops-monedula-dev-v1alpha1-kafkaaccesspolicy,mutating=false,failurePolicy=fail,sideEffects=None,groups=gitops.monedula.dev,resources=kafkaaccesspolicies,verbs=create;update,versions=v1alpha1,name=vkafkaaccesspolicy.gitops.monedula.dev,admissionReviewVersions=v1

// KafkaAccessPolicyValidator is the admission.Validator[*v1alpha1.KafkaAccessPolicy]
// for v1alpha1.KafkaAccessPolicy. It reads from the manager cache (Reader) and
// resolves the referenced cluster per the operator's cluster-resolution
// convention (ClusterNamespace override, else the policy's own namespace) —
// mirroring KafkaTopicValidator and KafkaQuotaValidator exactly.
type KafkaAccessPolicyValidator struct {
	// Reader is the manager's cached client. It must have the
	// PolicyClusterRefNameIndex field index registered (RegisterIndexes does this).
	Reader client.Reader
	// ClusterNamespace is where KafkaCluster CRs are resolved from. Empty means
	// each policy's own namespace (clusterRef is namespace-local). This MUST match
	// the value passed to the reconcilers so admission and reconcile agree on the
	// cluster-resolution scope.
	ClusterNamespace string
}

// Compile-time assertion that the validator satisfies the generic Validator
// interface for *v1alpha1.KafkaAccessPolicy.
var _ admission.Validator[*v1alpha1.KafkaAccessPolicy] = &KafkaAccessPolicyValidator{}

// clusterNamespaceFor returns the namespace the policy's clusterRef resolves in,
// applying the ClusterNamespace override exactly as the reconciler does.
func (v *KafkaAccessPolicyValidator) clusterNamespaceFor(pol *v1alpha1.KafkaAccessPolicy) string {
	if v.ClusterNamespace != "" {
		return v.ClusterNamespace
	}
	return pol.Namespace
}

// ValidateCreate enforces shape and cross-resource ACL conflict on create.
func (v *KafkaAccessPolicyValidator) ValidateCreate(ctx context.Context, pol *v1alpha1.KafkaAccessPolicy) (admission.Warnings, error) {
	if err := checkPolicyShape(pol); err != nil {
		return nil, err
	}
	return nil, v.checkConflict(ctx, pol)
}

// ValidateUpdate enforces shape, clusterRef immutability, and cross-resource
// ACL conflict on update. Shape is checked first for the clearest rejection
// message; immutability next, before the conflict scan.
func (v *KafkaAccessPolicyValidator) ValidateUpdate(ctx context.Context, oldPol, newPol *v1alpha1.KafkaAccessPolicy) (admission.Warnings, error) {
	if err := checkPolicyShape(newPol); err != nil {
		return nil, err
	}
	// clusterRef immutability: repointing a policy to a different cluster orphans
	// the ACLs applied on the previous one. Mirrors the CEL rule on
	// KafkaAccessPolicySpec so webhook-on and webhook-off installs agree.
	if oldPol.Spec.ClusterRef.Name != newPol.Spec.ClusterRef.Name {
		return nil, fmt.Errorf(
			"KafkaAccessPolicy %s/%s: spec.clusterRef.name is immutable: cannot change from %q to %q (repointing a policy orphans ACLs on the previous cluster)",
			newPol.Namespace, newPol.Name, oldPol.Spec.ClusterRef.Name, newPol.Spec.ClusterRef.Name)
	}
	return nil, v.checkConflict(ctx, newPol)
}

// ValidateDelete always allows: removing a CR can never introduce a new conflict.
func (v *KafkaAccessPolicyValidator) ValidateDelete(_ context.Context, _ *v1alpha1.KafkaAccessPolicy) (admission.Warnings, error) {
	return nil, nil
}

// checkPolicyShape runs the standalone shape checks (reused from internal/
// validation) and aggregates any failures into a single rejection error.
//
// Objects received at admission have an empty TypeMeta (apiVersion/kind are
// stripped by the apiserver machinery). ValidateAccessPolicyShape checks
// apiVersion, so we fill it on a shallow copy before calling — mirroring how
// the reconciler sets pol.APIVersion before calling validation.Validate.
func checkPolicyShape(pol *v1alpha1.KafkaAccessPolicy) error {
	check := *pol
	if check.APIVersion == "" {
		check.APIVersion = v1alpha1.APIVersion
	}
	shapeErrs := validation.ValidateAccessPolicyShape(&check)
	if len(shapeErrs) == 0 {
		return nil
	}
	msgs := make([]string, 0, len(shapeErrs))
	for _, e := range shapeErrs {
		msgs = append(msgs, e.Error())
	}
	return errors.New(strings.Join(msgs, "; "))
}

// checkConflict rejects when the incoming policy introduces a cross-resource
// Allow/Deny conflict with any existing KafkaTopic or KafkaAccessPolicy on the
// same cluster.
//
// NOTE — TOCTOU: this check reads from the manager CACHE. Two near-simultaneous
// Creates can both pass (cache lag). Admission shrinks the conflict window but
// is not a linearizable guarantee; the reconciler's ACLConflict condition
// remains the backstop for conflicts that slip through.
//
// Algorithm:
//  1. List all KafkaTopics and KafkaAccessPolicies on the same cluster ref
//     (via the registered field indexes for efficient lookup).
//  2. Build the desired ACL union with the incoming policy substituted for any
//     stored version of it (exclude the same namespace/name on updates; on
//     creates there is no stored version). Append the incoming policy's ACLs.
//  3. Run BuildClusterACLView (reusing compile+stamp+sort+conflicts logic).
//     Pass nil clusterDefaults — the webhook does not resolve the cluster
//     object; cluster-not-found → allow, mirroring KafkaTopic/KafkaQuota.
//  4. If any returned Conflict names the INCOMING policy as a party, reject
//     naming the other party (kind/ns/name) and the contested subject.
//
// Cluster-not-found / list error → allow (defer to reconcile), mirroring
// the quota and topic webhook precedents exactly.
func (v *KafkaAccessPolicyValidator) checkConflict(ctx context.Context, pol *v1alpha1.KafkaAccessPolicy) error {
	wantClusterRef := pol.Spec.ClusterRef.Name
	wantClusterNS := v.clusterNamespaceFor(pol)

	// Resolve the referenced KafkaCluster to determine whether it exists. A
	// missing cluster means the policy may be applied before its cluster CR
	// arrives (a common GitOps push pattern). Allow and defer to reconcile.
	var cluster v1alpha1.KafkaCluster
	if err := v.Reader.Get(ctx, types.NamespacedName{Namespace: wantClusterNS, Name: wantClusterRef}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // cluster not yet present: allow; reconcile surfaces it
		}
		// Unresolvable cluster (read error): allow; defer to reconcile.
		return nil
	}

	// List all topics referencing the same cluster ref name.
	var topicList v1alpha1.KafkaTopicList
	if err := v.Reader.List(ctx, &topicList,
		client.MatchingFields{ClusterRefNameIndex: wantClusterRef},
	); err != nil {
		// List failure: allow; defer to reconcile (mirrors cluster-not-found pattern).
		return nil
	}

	// List all policies referencing the same cluster ref name.
	var policyList v1alpha1.KafkaAccessPolicyList
	if err := v.Reader.List(ctx, &policyList,
		client.MatchingFields{PolicyClusterRefNameIndex: wantClusterRef},
	); err != nil {
		return nil // list failure: allow; defer to reconcile
	}

	// Build topic slice (filter to same effective cluster namespace, skip deleting).
	var topics []*v1alpha1.KafkaTopic
	for i := range topicList.Items {
		tp := &topicList.Items[i]
		if !tp.DeletionTimestamp.IsZero() {
			continue
		}
		// Namespace scope filter: with ClusterNamespace override, only same cluster
		// ref is needed (same as quota webhook); without it, topics in a different
		// namespace resolve to a different cluster object.
		if v.ClusterNamespace == "" && tp.Namespace != pol.Namespace {
			continue
		}
		cp := tp.DeepCopy()
		topics = append(topics, cp)
	}

	// Build policy slice with the incoming policy swapped in: EXCLUDE the stored
	// version of pol (matched by namespace+name; on create no stored version exists)
	// and INCLUDE the incoming pol at the end. This lets BuildClusterACLView see
	// exactly the desired post-admission state.
	var policies []*v1alpha1.KafkaAccessPolicy
	for i := range policyList.Items {
		other := &policyList.Items[i]
		if !other.DeletionTimestamp.IsZero() {
			continue
		}
		// Skip the stored version of pol itself (update path; no stored version on
		// create, so this never matches).
		if other.Namespace == pol.Namespace && other.Name == pol.Name {
			continue
		}
		// Namespace scope filter (same logic as topics above).
		if v.ClusterNamespace == "" && other.Namespace != pol.Namespace {
			continue
		}
		cp := other.DeepCopy()
		policies = append(policies, cp)
	}
	// Append the incoming policy (the desired post-admission state).
	policies = append(policies, pol.DeepCopy())

	// Build the cluster-wide ACL view. clusterDefaults is nil: the webhook does
	// not resolve the cluster for defaulting (same as the reconciler's
	// cluster-not-found → allow behaviour, and consistent with the quota/topic
	// webhooks not applying cluster-level defaults). The view carries all conflicts
	// deterministically via BuildClusterACLView's sort+Conflicts logic.
	view := reconcile.BuildClusterACLView(topics, policies, nil, &cluster)

	// If any conflict names the incoming policy as a party, reject with the
	// other party's identity and the contested subject. Use the first (deterministic).
	for _, cf := range view.Conflicts {
		incomingIsA := cf.A.SourceKind == "KafkaAccessPolicy" &&
			cf.A.SourceNamespace == pol.Namespace &&
			cf.A.SourceName == pol.Name
		incomingIsB := cf.B.SourceKind == "KafkaAccessPolicy" &&
			cf.B.SourceNamespace == pol.Namespace &&
			cf.B.SourceName == pol.Name

		if !incomingIsA && !incomingIsB {
			continue
		}

		var otherKind, otherNS, otherName string
		if incomingIsA {
			otherKind, otherNS, otherName = cf.B.SourceKind, cf.B.SourceNamespace, cf.B.SourceName
		} else {
			otherKind, otherNS, otherName = cf.A.SourceKind, cf.A.SourceNamespace, cf.A.SourceName
		}
		return fmt.Errorf(
			"KafkaAccessPolicy %s/%s introduces an ACL conflict with %s %s/%s on subject %q: "+
				"Allow/Deny disagreement on the same ACL tuple (spec §21); "+
				"the tuple will be dropped from the applied set — resolve the disagreement before admission",
			pol.Namespace, pol.Name, otherKind, otherNS, otherName, cf.Subject)
	}
	return nil
}
