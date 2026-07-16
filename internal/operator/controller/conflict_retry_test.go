package controller

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
)

// These tests pin the retry-hygiene contract (review I9): a 409 Conflict on the
// status write must retry ONLY the write — it must never re-run the reconcile
// core (and so never re-issue Kafka mutations). The mock's recorded mutating
// calls are the witness: with a conflict injected on the first status write,
// each Kafka mutation must still be attempted exactly once.

// conflictingClient wraps a client.Client and makes the first `conflicts`
// Status().Update calls fail with a 409 Conflict, succeeding afterwards. It
// also counts status-update attempts so a test can prove the retry happened.
type conflictingClient struct {
	client.Client
	conflicts     int // remaining Status().Update calls to reject with Conflict
	statusUpdates int // total Status().Update attempts observed
}

func (c *conflictingClient) Status() client.SubResourceWriter {
	return &conflictingStatusWriter{w: c.Client.Status(), c: c}
}

type conflictingStatusWriter struct {
	w client.SubResourceWriter
	c *conflictingClient
}

func (s *conflictingStatusWriter) Create(ctx context.Context, obj, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return s.w.Create(ctx, obj, subResource, opts...)
}

func (s *conflictingStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	s.c.statusUpdates++
	if s.c.conflicts > 0 {
		s.c.conflicts--
		return apierrors.NewConflict(
			schema.GroupResource{Group: v1alpha1.GroupVersion.Group, Resource: "statusconflicts"},
			obj.GetName(), errReason("injected conflict"))
	}
	return s.w.Update(ctx, obj, opts...)
}

func (s *conflictingStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return s.w.Patch(ctx, obj, patch, opts...)
}

func (s *conflictingStatusWriter) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return s.w.Apply(ctx, obj, opts...)
}

// countCalls returns how many recorded mock calls start with prefix.
func countCalls(calls []string, prefix string) int {
	n := 0
	for _, c := range calls {
		if len(c) >= len(prefix) && c[:len(prefix)] == prefix {
			n++
		}
	}
	return n
}

// The CreateTopic is configured to FAIL on the mock so the reconcile does not
// converge: if the conflict retry re-ran the reconcile core, a second
// CreateTopic attempt would be recorded. (A succeeding CreateTopic would make a
// re-run a silent no-op and hide the bug.)
func TestTopicReconcile_StatusConflictDoesNotRerunKafkaMutations(t *testing.T) {
	s := topicScheme(t)
	tp := topicObj("Enforce")
	c := topicCluster()
	cl := &conflictingClient{Client: newTopicFakeClient(t, s, tp, c), conflicts: 1}
	k := kafkamock.New(nil, nil)
	k.FailOn("CreateTopic", "payments.orders", errReason("broker unavailable"))
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	// The reconcile itself reports the (injected) Kafka failure; the status
	// write must still land despite the conflict on its first attempt.
	if _, err := r.Reconcile(context.Background(), topicReq()); err == nil {
		t.Fatal("expected transient reconcile error from the failing CreateTopic")
	}

	if got := countCalls(k.Calls(), "CreateTopic payments.orders"); got != 1 {
		t.Errorf("CreateTopic attempted %d times, want exactly 1 (status conflict must not re-run the reconcile): calls=%v", got, k.Calls())
	}
	if cl.statusUpdates != 2 {
		t.Errorf("status updates = %d, want 2 (one Conflict + one success)", cl.statusUpdates)
	}

	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(tp), &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if got.Status == nil || got.Status.Phase == "" {
		t.Fatalf("status did not land after conflict retry: %+v", got.Status)
	}
}

// Policy analogue: CreateACLs fails on the mock so a re-run of the reconcile
// core under a status conflict would record a second attempt.
func TestPolicyReconcile_StatusConflictDoesNotRerunKafkaMutations(t *testing.T) {
	s := topicScheme(t)
	pol := policyObj("Enforce")
	c := policyCluster()
	cl := &conflictingClient{Client: newPolicyFakeClient(t, s, pol, c), conflicts: 1}
	k := kafkamock.New(nil, nil)
	// The executor applies ACLs one at a time, so every individual CreateACLs
	// call carries one ACL ("CreateACLs 1") — fail them all.
	k.FailOn("CreateACLs", "1", errReason("broker unavailable"))
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), policyReq()); err == nil {
		t.Fatal("expected transient reconcile error from the failing CreateACLs")
	}

	// One CreateACLs attempt per desired ACL — and no more: a re-run of the
	// reconcile core under the status conflict would double this.
	want := len(seededPolicyACLs(t))
	if got := countCalls(k.Calls(), "CreateACLs"); got != want {
		t.Errorf("CreateACLs attempted %d times, want exactly %d (status conflict must not re-run the reconcile): calls=%v", got, want, k.Calls())
	}
	if cl.statusUpdates != 2 {
		t.Errorf("status updates = %d, want 2 (one Conflict + one success)", cl.statusUpdates)
	}

	var got v1alpha1.KafkaAccessPolicy
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(pol), &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if got.Status == nil || got.Status.Phase == "" {
		t.Fatalf("status did not land after conflict retry: %+v", got.Status)
	}
}
