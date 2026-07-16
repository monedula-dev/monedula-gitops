package controller

import (
	"context"
	"sync"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
	srmock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// The tests in this file pin the D1 quorum-recheck contract (duplicate.go):
// the duplicate gate and the deletion co-claimant scans re-run against the
// reconciler's APIReader on the CONTESTED paths only, and take that result as
// authoritative. Cache lag is modelled with two independent fake clients — the
// reconciler's Client plays the (stale) informer cache, the counting APIReader
// plays quorum — and the countingReader pins the hot-path guarantee: an
// established CR with no cached rival performs ZERO apiserver Lists.

// countingReader wraps a client.Reader, counting EVERY call (List and Get):
// the hot-path zero-round-trip pin must cover all APIReader usage, so a future
// Get sneaking onto the steady-state path fails the same assertions.
type countingReader struct {
	reader client.Reader
	mu     sync.Mutex
	calls  int
}

func (c *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.reader.Get(ctx, key, obj, opts...)
}

func (c *countingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.reader.List(ctx, list, opts...)
}

func (c *countingReader) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// withReadyCondition marks a status's Conditions slice with Ready=True — the
// identityMaterialized predicate — standing in for a CR that has successfully
// reconciled before.
func withReadyCondition(conds *[]metav1.Condition, gen int64) {
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:               v1alpha1.CondReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Reconciled",
		Message:            "reconciled",
		ObservedGeneration: gen,
	})
}

// TestQuorumRecheck_HotPathZeroLists pins the no-behavior-change guarantee for
// the steady-state hot path: an ESTABLISHED KafkaUser (Ready=True from a prior
// reconcile) with no cached rival reconciles without a single APIReader List —
// the quorum recheck must never tax healthy steady-state reconciles.
func TestQuorumRecheck_HotPathZeroLists(t *testing.T) {
	s := topicScheme(t)
	u := stampedUser("ns1", "svc-a", "prod", "svc-alpha", dupT0)
	u.Status = &v1alpha1.KafkaUserStatus{ObservedGeneration: 1, Phase: v1alpha1.PhaseReady}
	withReadyCondition(&u.Status.Conditions, 1)
	cl := dupFakeClient(t, s, u, topicCluster())
	quorum := &countingReader{reader: cl}
	k := kafkamock.New(nil, nil)
	r := &KafkaUserReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}, APIReader: quorum}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "svc-a")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := quorum.count(); got != 0 {
		t.Fatalf("hot path performed %d APIReader List(s), want 0", got)
	}
	var got v1alpha1.KafkaUser
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "svc-a"}, &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status = %+v, want Ready", got.Status)
	}
}

// TestQuorumRecheck_LoserPathRivalGoneAtQuorum pins the loser-path recheck AND
// that its result is authoritative: the cached scan sees an older rival, but at
// quorum the rival is already deleted — the CR must NOT go terminal, must
// reconcile normally, and must have paid exactly one APIReader List.
func TestQuorumRecheck_LoserPathRivalGoneAtQuorum(t *testing.T) {
	s := topicScheme(t)
	rival := stampedUser("ns1", "svc-old", "prod", "svc-shared", dupT0)
	self := stampedUser("ns1", "svc-new", "prod", "svc-shared", dupLater())
	cache := dupFakeClient(t, s, rival, self, topicCluster()) // stale: still shows the rival
	quorum := &countingReader{reader: dupFakeClient(t, s, self.DeepCopy(), topicCluster())}
	k := kafkamock.New(nil, nil)
	r := &KafkaUserReconciler{Client: cache, Scheme: s, Clients: stubFactory{k: k}, APIReader: quorum}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "svc-new")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := quorum.count(); got != 1 {
		t.Fatalf("loser path performed %d APIReader List(s), want exactly 1", got)
	}
	if !callsContain(k.Calls(), "UpsertScramCredential ") {
		t.Fatalf("quorum said the rival is gone; the CR must reconcile normally, kafka calls = %v", k.Calls())
	}
	var got v1alpha1.KafkaUser
	if err := cache.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "svc-new"}, &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status = %+v, want Ready (recheck result must be authoritative)", got.Status)
	}
	if vf := dupCond(got.Status.Conditions, v1alpha1.CondValidationFailed); vf != nil && vf.Status == metav1.ConditionTrue {
		t.Fatalf("CR went terminal on a quorum-deleted rival: %+v", vf)
	}
}

// TestQuorumRecheck_NeverReconciledFindsQuorumRival pins the second recheck
// trigger: a CR with NO cached rival but an empty status (it has never
// materialized its broker identity — the young-CR cache-lag danger case)
// rechecks at quorum, discovers the older rival the cache missed, and goes
// terminal WITHOUT any broker call.
func TestQuorumRecheck_NeverReconciledFindsQuorumRival(t *testing.T) {
	s := topicScheme(t)
	self := stampedUser("ns1", "svc-new", "prod", "svc-shared", dupLater())
	rival := stampedUser("ns1", "svc-old", "prod", "svc-shared", dupT0)
	cache := dupFakeClient(t, s, self, topicCluster()) // lagging: rival not yet visible
	quorum := &countingReader{reader: dupFakeClient(t, s, self.DeepCopy(), rival, topicCluster())}
	k := kafkamock.New(nil, nil)
	r := &KafkaUserReconciler{Client: cache, Scheme: s, Clients: stubFactory{k: k}, APIReader: quorum}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "svc-new")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := quorum.count(); got != 1 {
		t.Fatalf("never-reconciled path performed %d APIReader List(s), want exactly 1", got)
	}
	if calls := k.Calls(); len(calls) != 0 {
		t.Fatalf("the quorum loser must not touch the broker; kafka calls = %v", calls)
	}
	var got v1alpha1.KafkaUser
	if err := cache.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "svc-new"}, &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	vf := dupCond(got.Status.Conditions, v1alpha1.CondValidationFailed)
	if got.Status.Phase != v1alpha1.PhaseError || vf == nil || vf.Status != metav1.ConditionTrue ||
		vf.Reason != reasonDuplicateIdentity {
		t.Fatalf("status = phase %v, ValidationFailed %+v, want Error/DuplicateIdentity", got.Status.Phase, vf)
	}
}

// TestQuorumRecheck_DeletionRevealsCoClaimant pins the deletion-path recheck's
// fail-safe direction (leak, never destroy a survivor's state): the cached
// co-claimant scan finds nobody — the finalizer is about to delete the SCRAM
// credential — but quorum reveals a live co-claimant the cache missed. The
// cleanup must be SKIPPED (credential retained, orphaned to the survivor) and
// the finalizer still removed.
func TestQuorumRecheck_DeletionRevealsCoClaimant(t *testing.T) {
	s := topicScheme(t)
	doomed := stampedUser("ns1", "svc-del", "prod", "svc-shared", dupT0)
	doomed.Spec.DeletionPolicy = "Delete"
	doomed.Finalizers = []string{FinalizerName}
	survivor := stampedUser("ns1", "svc-live", "prod", "svc-shared", dupLater())
	cache := dupFakeClient(t, s, doomed, topicCluster()) // lagging: survivor not yet visible
	quorum := &countingReader{reader: dupFakeClient(t, s, survivor, topicCluster())}
	if err := cache.Delete(context.Background(), doomed); err != nil {
		t.Fatalf("mark deleting: %v", err)
	}
	k := kafkamock.NewWithScramCredentials(nil, nil,
		[]kafka.ScramCredential{{User: "svc-shared", Mechanism: "SCRAM-SHA-512", Iterations: 4096}})
	r := &KafkaUserReconciler{Client: cache, Scheme: s, Clients: stubFactory{k: k}, APIReader: quorum}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "svc-del")); err != nil {
		t.Fatalf("deletion reconcile: %v", err)
	}
	if got := quorum.count(); got != 1 {
		t.Fatalf("deletion path performed %d APIReader List(s), want exactly 1", got)
	}
	if callsContain(k.Calls(), "DeleteScramCredential ") {
		t.Fatalf("cleanup must be skipped when quorum reveals a co-claimant; kafka calls = %v", k.Calls())
	}
	if creds, _ := k.ListScramCredentials(context.Background(), "svc-shared"); len(creds) != 1 {
		t.Fatalf("live credentials = %+v, want the credential retained for the survivor", creds)
	}
	var gone v1alpha1.KafkaUser
	err := cache.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "svc-del"}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("the deleting CR must still finalize; get err = %v", err)
	}
}

// TestQuorumRecheck_TopicLoserPathRivalGoneAtQuorum is the KafkaTopic variant
// of the loser-path recheck (the gate code is duplicated per kind, so each
// kind's wiring is pinned at least once).
func TestQuorumRecheck_TopicLoserPathRivalGoneAtQuorum(t *testing.T) {
	s := topicScheme(t)
	rival := stampedTopic("ns1", "orders-old", "prod", "dup.orders", dupT0)
	self := stampedTopic("ns1", "orders-new", "prod", "dup.orders", dupLater())
	cache := dupFakeClient(t, s, rival, self, topicCluster())
	quorum := &countingReader{reader: dupFakeClient(t, s, self.DeepCopy(), topicCluster())}
	k := kafkamock.New(nil, nil)
	r := &KafkaTopicReconciler{Client: cache, Scheme: s, Clients: stubFactory{k: k, sr: srmock.New()}, APIReader: quorum}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "orders-new")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := quorum.count(); got != 1 {
		t.Fatalf("loser path performed %d APIReader List(s), want exactly 1", got)
	}
	if !callsContain(k.Calls(), "CreateTopic dup.orders") {
		t.Fatalf("quorum said the rival is gone; the topic must be created, kafka calls = %v", k.Calls())
	}
}

// TestQuorumRecheck_QuotaNeverReconciledFindsQuorumRival is the KafkaQuota
// variant of the never-reconciled recheck.
func TestQuorumRecheck_QuotaNeverReconciledFindsQuorumRival(t *testing.T) {
	s := topicScheme(t)
	self := stampedQuota("ns1", "quota-new", "prod", "User:alice", dupLater())
	rival := stampedQuota("ns1", "quota-old", "prod", "User:alice", dupT0)
	cache := dupFakeClient(t, s, self, topicCluster())
	quorum := &countingReader{reader: dupFakeClient(t, s, self.DeepCopy(), rival, topicCluster())}
	k := kafkamock.New(nil, nil)
	r := &KafkaQuotaReconciler{Client: cache, Scheme: s, Clients: stubFactory{k: k}, APIReader: quorum}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "quota-new")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := quorum.count(); got != 1 {
		t.Fatalf("never-reconciled path performed %d APIReader List(s), want exactly 1", got)
	}
	if calls := k.Calls(); len(calls) != 0 {
		t.Fatalf("the quorum loser must not touch the broker; kafka calls = %v", calls)
	}
	var got v1alpha1.KafkaQuota
	if err := cache.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "quota-new"}, &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	vf := dupCond(got.Status.Conditions, v1alpha1.CondValidationFailed)
	if got.Status.Phase != v1alpha1.PhaseError || vf == nil || vf.Reason != reasonDuplicateIdentity {
		t.Fatalf("status = phase %v, ValidationFailed %+v, want Error/DuplicateIdentity", got.Status.Phase, vf)
	}
}

// TestQuorumRecheck_RoleBindingHotPathZeroLists pins the hot-path guarantee for
// the compiled-identity kind: an established KafkaRoleBinding (Ready=True) with
// no overlapping cached rival performs zero APIReader Lists.
func TestQuorumRecheck_RoleBindingHotPathZeroLists(t *testing.T) {
	s := newScheme(t)
	rb := stampedRoleBinding("ns1", "rb-a", "prod", dupT0)
	rb.Status = &v1alpha1.KafkaRoleBindingStatus{ObservedGeneration: 1, Phase: v1alpha1.PhaseReady}
	withReadyCondition(&rb.Status.Conditions, 1)
	cl := dupFakeClient(t, s, rb, mdsCluster("ns1", "prod"))
	quorum := &countingReader{reader: cl}
	r := &KafkaRoleBindingReconciler{Client: cl, Scheme: s, Clients: stubFactory{mds: mdsmock.New()}, APIReader: quorum}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "rb-a")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := quorum.count(); got != 0 {
		t.Fatalf("hot path performed %d APIReader List(s), want 0", got)
	}
}
