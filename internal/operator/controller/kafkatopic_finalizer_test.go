package controller

// Finalizer decision-ladder tests for KafkaTopic (mirror-topics design v6,
// "Decision precedence and co-claimant semantics"): the no-cleanup exits
// (Orphan, force-removal, positive co-claimant) and the claim-scan failure
// are all decided BEFORE any Kafka/REST client is built, so they hold even
// when the destination cluster or its REST endpoint is unreachable.

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
)

// finalizerMirrorClaimant is a live KafkaMirrorTopic in ns1 whose destination
// topic is payments.orders on cluster prod — the same canonical claim key as
// topicObj.
func finalizerMirrorClaimant() *v1alpha1.KafkaMirrorTopic {
	return &v1alpha1.KafkaMirrorTopic{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1", Name: "orders-mirror", Generation: 1,
			UID: types.UID("ns1/orders-mirror"),
		},
		Spec: v1alpha1.KafkaMirrorTopicSpec{
			ClusterRef:      v1alpha1.ClusterRef{Name: "prod"},
			LinkName:        "dr-link",
			SourceTopicName: "payments.orders",
		},
	}
}

// deletingDeleteTopic builds a fake-client world holding a being-deleted
// KafkaTopic (policy Delete, finalizer set, with ACL-bearing access so a
// cleanup would issue DeleteACLs) plus extra objects, and returns the client.
func deletingDeleteTopic(t *testing.T, annotations map[string]string, extra ...client.Object) client.Client {
	t.Helper()
	s := topicScheme(t)
	tp := coOwnedTopic()
	tp.Spec.DeletionPolicy = "Delete"
	tp.Finalizers = []string{FinalizerName}
	tp.Annotations = annotations
	objs := append([]client.Object{tp, topicCluster()}, extra...)
	cl := newTopicFakeClient(t, s, objs...)
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	return cl
}

func allowDelete() map[string]string {
	return map[string]string{"gitops.monedula.dev/allow-delete": "true"}
}

func assertTopicGone(t *testing.T, cl client.Client) {
	t.Helper()
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err == nil {
		t.Fatalf("topic CR still present (finalizer kept): finalizers %v", got.Finalizers)
	}
}

func assertFinalizerKept(t *testing.T, cl client.Client) {
	t.Helper()
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err != nil {
		t.Fatalf("topic CR should still exist (finalizer retained): %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, FinalizerName) {
		t.Fatal("finalizer must be retained")
	}
}

// drainEvents returns every event recorded so far.
func drainEvents(rec *events.FakeRecorder) []string {
	var out []string
	for {
		select {
		case ev := <-rec.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func findEvent(evs []string, parts ...string) bool {
	for _, ev := range evs {
		all := true
		for _, p := range parts {
			if !strings.Contains(ev, p) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

func TestFinalizerOrphanSkipsClients(t *testing.T) {
	cl, s := deleteSetup(t, "Orphan", nil, nil)
	f := &countingFactory{stubFactory: stubFactory{err: errors.New("unreachable"), restErr: errors.New("rest down")}}
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: f}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertTopicGone(t, cl)
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("Orphan must not build any client, factory calls = %d", n)
	}
}

func TestFinalizerForceRemovalSkipsClients(t *testing.T) {
	cl := deletingDeleteTopic(t, map[string]string{AnnotationForceFinalizerRemoval: "true"})
	k := kafkamock.New(seededTopics("payments.orders"), coOwnedLiveACLs())
	f := &countingFactory{stubFactory: stubFactory{k: k}}
	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Recorder: rec, Clients: f}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertTopicGone(t, cl)
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("force-removal must not build any client, factory calls = %d", n)
	}
	if calls := k.Calls(); len(calls) != 0 {
		t.Fatalf("force-removal is orphan-only, kafka calls = %v", calls)
	}
	if evs := drainEvents(rec); !findEvent(evs, "Warning", "ForceFinalizerRemoval") {
		t.Fatalf("expected a ForceFinalizerRemoval warning, got %v", evs)
	}
}

func TestFinalizerCoClaimantFullOrphan(t *testing.T) {
	cl := deletingDeleteTopic(t, allowDelete(), finalizerMirrorClaimant())
	k := kafkamock.New(seededTopics("payments.orders"), coOwnedLiveACLs())
	f := &countingFactory{stubFactory: stubFactory{k: k}}
	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Recorder: rec, Clients: f}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertTopicGone(t, cl)
	if callsContain(k.Calls(), "DeleteTopic") || callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("co-claimant must fully orphan: no DeleteTopic/DeleteACLs, calls %v", k.Calls())
	}
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("co-claimant orphan must not build any client, factory calls = %d", n)
	}
	if evs := drainEvents(rec); !findEvent(evs, "Warning", reasonOrphanedToCoClaimant, "KafkaMirrorTopic/ns1/orders-mirror") {
		t.Fatalf("expected an OrphanedToCoClaimant warning naming KafkaMirrorTopic/ns1/orders-mirror, got %v", evs)
	}
}

func TestFinalizerCoClaimantBeatsRESTOutage(t *testing.T) {
	cl := deletingDeleteTopic(t, allowDelete(), finalizerMirrorClaimant())
	f := &countingFactory{stubFactory: stubFactory{err: errors.New("unreachable"), restErr: errors.New("rest down")}}
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Clients: f}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertTopicGone(t, cl)
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("co-claimant orphan must not build any client, factory calls = %d", n)
	}
}

// TestFinalizerCoClaimantKafkaTopic: a live same-key KafkaTopic duplicate (a
// newer one, which would LOSE the regular contest) still preserves the topic.
func TestFinalizerCoClaimantKafkaTopic(t *testing.T) {
	dup := topicObj("Enforce")
	dup.Name = "orders-dup"
	dup.UID = types.UID("ns1/orders-dup")
	dup.CreationTimestamp = metav1.NewTime(dupT0.Add(3600e9))
	cl := deletingDeleteTopic(t, allowDelete(), dup)
	k := kafkamock.New(seededTopics("payments.orders"), coOwnedLiveACLs())
	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Recorder: rec, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertTopicGone(t, cl)
	if calls := k.Calls(); len(calls) != 0 {
		t.Fatalf("co-claimant must fully orphan, kafka calls = %v", calls)
	}
	if evs := drainEvents(rec); !findEvent(evs, reasonOrphanedToCoClaimant, "KafkaTopic/ns1/orders-dup") {
		t.Fatalf("expected an OrphanedToCoClaimant event naming KafkaTopic/ns1/orders-dup, got %v", evs)
	}
}

// TestFinalizerCoClaimantQuorumRecheck: the cached scan misses the claimant,
// the uncached APIReader sees it — the outcome flips to full orphan.
func TestFinalizerCoClaimantQuorumRecheck(t *testing.T) {
	cache := deletingDeleteTopic(t, allowDelete())
	quorum := deletingDeleteTopic(t, allowDelete(), finalizerMirrorClaimant())
	k := kafkamock.New(seededTopics("payments.orders"), coOwnedLiveACLs())
	f := &countingFactory{stubFactory: stubFactory{k: k}}
	r := &KafkaTopicReconciler{Client: cache, Scheme: topicScheme(t), Clients: f, APIReader: quorum}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertTopicGone(t, cache)
	if calls := k.Calls(); len(calls) != 0 {
		t.Fatalf("quorum-found co-claimant must fully orphan, kafka calls = %v", calls)
	}
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("no client may be built, factory calls = %d", n)
	}
}

// listErrClient fails every List of the given type.
func listErrClient(base client.Client, match func(client.ObjectList) bool) client.Client {
	return interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if match(list) {
				return errors.New("injected: apiserver unavailable")
			}
			return c.List(ctx, list, opts...)
		},
	})
}

func isMirrorList(l client.ObjectList) bool {
	_, ok := l.(*v1alpha1.KafkaMirrorTopicList)
	return ok
}

func TestFinalizerClaimScanErrorBlocks(t *testing.T) {
	base := deletingDeleteTopic(t, allowDelete())
	cl := listErrClient(base, isMirrorList)
	k := kafkamock.New(seededTopics("payments.orders"), coOwnedLiveACLs())
	f := &countingFactory{stubFactory: stubFactory{k: k}}
	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Recorder: rec, Clients: f}

	_, err := r.Reconcile(context.Background(), topicReq())
	if err == nil || !strings.Contains(err.Error(), "injected: apiserver unavailable") {
		t.Fatalf("claim-scan failure must return the scan error, got %v", err)
	}
	assertFinalizerKept(t, base)
	if calls := k.Calls(); len(calls) != 0 {
		t.Fatalf("claim-scan failure must not touch the broker, kafka calls = %v", calls)
	}
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("claim-scan failure must not build any client, factory calls = %d", n)
	}
	if evs := drainEvents(rec); !findEvent(evs, "Warning", "DeleteBlocked", "ClaimCheckFailed") {
		t.Fatalf("expected a DeleteBlocked/ClaimCheckFailed warning, got %v", evs)
	}
}

// TestFinalizerQuorumRecheckErrorBlocks: a clean cached scan followed by a
// failed APIReader recheck cannot confirm a negative result, so it blocks.
func TestFinalizerQuorumRecheckErrorBlocks(t *testing.T) {
	cache := deletingDeleteTopic(t, allowDelete())
	quorum := listErrClient(deletingDeleteTopic(t, allowDelete()), isMirrorList)
	k := kafkamock.New(seededTopics("payments.orders"), coOwnedLiveACLs())
	f := &countingFactory{stubFactory: stubFactory{k: k}}
	r := &KafkaTopicReconciler{Client: cache, Scheme: topicScheme(t), Clients: f, APIReader: quorum}

	if _, err := r.Reconcile(context.Background(), topicReq()); err == nil {
		t.Fatal("failed quorum recheck must return an error")
	}
	assertFinalizerKept(t, cache)
	if calls := k.Calls(); len(calls) != 0 {
		t.Fatalf("failed recheck must not touch the broker, kafka calls = %v", calls)
	}
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("failed recheck must not build any client, factory calls = %d", n)
	}
}

// TestFinalizerNoApprovalSkipsClients: a confirmed negative scan without the
// allow-delete approval keeps the finalizer (DeleteNotApproved) and still
// builds no client — an unreachable cluster is irrelevant to the approval gate.
func TestFinalizerNoApprovalSkipsClients(t *testing.T) {
	cl := deletingDeleteTopic(t, nil)
	f := &countingFactory{stubFactory: stubFactory{err: errors.New("unreachable")}}
	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Recorder: rec, Clients: f}

	res, err := r.Reconcile(context.Background(), topicReq())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("DeleteNotApproved must requeue at the resync cadence")
	}
	assertFinalizerKept(t, cl)
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("approval gate must not build any client, factory calls = %d", n)
	}
	if evs := drainEvents(rec); !findEvent(evs, "DeleteNotApproved") {
		t.Fatalf("expected DeleteNotApproved, got %v", evs)
	}
}

// TestFinalizerRESTBuildFailureBlocks: with approval and no claimant, a REST
// client build failure takes the unreachable-deletion path (finalizer kept,
// nothing deleted).
func TestFinalizerRESTBuildFailureBlocks(t *testing.T) {
	cl := deletingDeleteTopic(t, allowDelete())
	k := kafkamock.New(seededTopics("payments.orders"), coOwnedLiveACLs())
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t),
		Clients: stubFactory{k: k, restErr: errors.New("rest down")}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err == nil {
		t.Fatal("REST build failure on the cleanup path must return an error")
	}
	assertFinalizerKept(t, cl)
	if callsContain(k.Calls(), "DeleteTopic") || callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("REST build failure must not delete anything, calls %v", k.Calls())
	}
}
