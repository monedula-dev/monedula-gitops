package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
)

// Finalizer cleanup-authorization gate (cleanupNotApplied + tenancy) for the
// KafkaUser, KafkaQuota and KafkaAccessPolicy finalizers: deleting a CR the
// operator refused, or whose spec changed after its last reconcile, must not
// destroy broker state that CR never managed.

func deniedNamespaceCluster() *v1alpha1.KafkaCluster {
	c := topicCluster()
	c.Spec.Tenancy = &v1alpha1.TenancyConfig{AllowedNamespaces: []string{"team-*"}}
	return c
}

func validationFailed(reason string) []metav1.Condition {
	return []metav1.Condition{{Type: v1alpha1.CondValidationFailed, Status: metav1.ConditionTrue, Reason: reason, Message: "denied"}}
}

func deleteAndReconcile(t *testing.T, obj client.Object, c *v1alpha1.KafkaCluster, run func(client.Client, *events.FakeRecorder)) []string {
	t.Helper()
	cl := dupFakeClient(t, topicScheme(t), obj, c)
	if err := cl.Delete(context.Background(), obj); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	rec := events.NewFakeRecorder(16)
	run(cl, rec)
	var evs []string
	for len(rec.Events) > 0 {
		evs = append(evs, <-rec.Events)
	}
	return evs
}

func TestUserDelete_TenancyDeniedKeepsCredential(t *testing.T) {
	u := stampedUser("ns1", "admin-hijack", "prod", "admin", dupT0)
	u.Spec.DeletionPolicy = "Delete"
	u.Finalizers = []string{FinalizerName}
	u.Status = &v1alpha1.KafkaUserStatus{ObservedGeneration: u.Generation, Conditions: validationFailed("TenancyDenied")}
	k := kafkamock.New(nil, nil)
	evs := deleteAndReconcile(t, u, deniedNamespaceCluster(), func(cl client.Client, rec *events.FakeRecorder) {
		r := &KafkaUserReconciler{Client: cl, Scheme: cl.Scheme(), Recorder: rec, Clients: stubFactory{k: k}}
		if _, err := r.Reconcile(context.Background(), reqFor("ns1", "admin-hijack")); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	})
	if callsContain(k.Calls(), "DeleteScramCredential") {
		t.Fatalf("refused KafkaUser must not delete the credential; kafka calls = %v", k.Calls())
	}
	if !hasEvent(evs, reasonCleanupSkipped) {
		t.Fatalf("expected a %s event, got %v", reasonCleanupSkipped, evs)
	}
}

func TestUserDelete_NeverReconciledKeepsCredential(t *testing.T) {
	u := stampedUser("ns1", "fresh", "prod", "admin", dupT0)
	u.Spec.DeletionPolicy = "Delete"
	u.Finalizers = []string{FinalizerName} // added, but no reconcile ever recorded status
	k := kafkamock.New(nil, nil)
	deleteAndReconcile(t, u, topicCluster(), func(cl client.Client, rec *events.FakeRecorder) {
		r := &KafkaUserReconciler{Client: cl, Scheme: cl.Scheme(), Recorder: rec, Clients: stubFactory{k: k}}
		if _, err := r.Reconcile(context.Background(), reqFor("ns1", "fresh")); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	})
	if callsContain(k.Calls(), "DeleteScramCredential") {
		t.Fatalf("never-reconciled KafkaUser must not delete the credential; kafka calls = %v", k.Calls())
	}
}

func TestQuotaDelete_TenancyDeniedKeepsQuota(t *testing.T) {
	q := stampedQuota("ns1", "strip-alice", "prod", "User:alice", dupT0)
	q.Finalizers = []string{FinalizerName}
	q.Status = &v1alpha1.KafkaQuotaStatus{ObservedGeneration: q.Generation}
	k := kafkamock.New(nil, nil)
	evs := deleteAndReconcile(t, q, deniedNamespaceCluster(), func(cl client.Client, rec *events.FakeRecorder) {
		r := &KafkaQuotaReconciler{Client: cl, Scheme: cl.Scheme(), Recorder: rec, Clients: stubFactory{k: k}}
		if _, err := r.Reconcile(context.Background(), reqFor("ns1", "strip-alice")); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	})
	if callsContain(k.Calls(), "DeleteQuota") {
		t.Fatalf("quota in a denied namespace must not be deleted; kafka calls = %v", k.Calls())
	}
	if !hasEvent(evs, reasonCleanupSkipped) {
		t.Fatalf("expected a %s event, got %v", reasonCleanupSkipped, evs)
	}
}

func TestQuotaDelete_ReconciledQuotaIsDeleted(t *testing.T) {
	q := stampedQuota("ns1", "alice", "prod", "User:alice", dupT0)
	q.Finalizers = []string{FinalizerName}
	q.Status = &v1alpha1.KafkaQuotaStatus{ObservedGeneration: q.Generation}
	k := kafkamock.New(nil, nil)
	deleteAndReconcile(t, q, topicCluster(), func(cl client.Client, rec *events.FakeRecorder) {
		r := &KafkaQuotaReconciler{Client: cl, Scheme: cl.Scheme(), Recorder: rec, Clients: stubFactory{k: k}}
		if _, err := r.Reconcile(context.Background(), reqFor("ns1", "alice")); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	})
	if !callsContain(k.Calls(), "DeleteQuota") {
		t.Fatalf("an applied, in-scope quota must still be deleted; kafka calls = %v", k.Calls())
	}
}

func TestPolicyDelete_SpecChangedAfterReconcileKeepsACLs(t *testing.T) {
	pol := policyObj("Enforce")
	pol.Spec.DeletionPolicy = "Delete"
	pol.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}
	pol.Finalizers = []string{FinalizerName}
	pol.Generation = 3 // reconciled at 1; +1 deletion bump, +1 spec edit
	pol.Status = &v1alpha1.KafkaAccessPolicyStatus{ObservedGeneration: 1}
	k := kafkamock.New(nil, nil)
	evs := deleteAndReconcile(t, pol, topicCluster(), func(cl client.Client, rec *events.FakeRecorder) {
		r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: cl.Scheme(), Recorder: rec, Clients: stubFactory{k: k}}
		if _, err := r.Reconcile(context.Background(), reqFor(pol.Namespace, pol.Name)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	})
	if callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("an unreconciled policy spec must not drive ACL deletion; kafka calls = %v", k.Calls())
	}
	if !hasEvent(evs, reasonCleanupSkipped) {
		t.Fatalf("expected a %s event, got %v", reasonCleanupSkipped, evs)
	}
}

func TestCleanupNotApplied(t *testing.T) {
	cases := []struct {
		name           string
		gen, observed  int64
		conds          []metav1.Condition
		wantAuthorized bool
	}{
		{"reconciled, fake client (no deletion bump)", 1, 1, nil, true},
		{"reconciled, apiserver deletion bump", 2, 1, nil, true},
		{"spec edited after deletion", 3, 1, nil, false},
		{"never reconciled", 2, 0, nil, false},
		{"refused by tenancy", 2, 1, validationFailed("TenancyDenied"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cleanupNotApplied(tc.gen, tc.observed, tc.conds) == ""
			if got != tc.wantAuthorized {
				t.Fatalf("authorized = %v, want %v", got, tc.wantAuthorized)
			}
		})
	}
}
