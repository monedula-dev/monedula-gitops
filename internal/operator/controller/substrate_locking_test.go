package controller

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
	"github.com/monedula-dev/monedula-gitops/internal/operator/locking"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
)

// ---- overlap detector harness (test-only) ----
//
// The detector wraps the kafka / MDS mocks and tracks entry/exit of every
// substrate-touching call per (cluster, substrate). Two concurrent spans on
// the same (cluster, substrate) are recorded as a violation — the exact race
// the per-cluster substrate locks (see locks.go) must make impossible.
//
// To make the tests deterministic rather than timing-lucky, the FIRST tracked
// call parks on a channel (a deliberate hold that widens the race window):
//   - if the locks are broken, the concurrent peer enters the window while the
//     first span is parked → the overlap is caught, and the park is released
//     immediately (no slow failure path);
//   - if the locks work, the peer stays blocked on the substrate mutex, the
//     park times out after holdTimeout, and the spans stay serialized.
// globalMax additionally records the maximum number of tracked spans in
// flight across ALL keys, which lets the negative control prove that spans on
// DIFFERENT clusters really did overlap (the locks are per-cluster, not a
// global mutex).

type overlapTracker struct {
	mu          sync.Mutex
	holdTimeout time.Duration
	inFlight    map[string]int // (clusterKey + "/" + substrate) -> live span count
	globalIn    int
	globalMax   int
	violations  []string
	parked      map[string]chan struct{}
	heldOnce    map[string]bool
}

func newOverlapTracker(holdTimeout time.Duration) *overlapTracker {
	return &overlapTracker{
		holdTimeout: holdTimeout,
		inFlight:    map[string]int{},
		parked:      map[string]chan struct{}{},
		heldOnce:    map[string]bool{},
	}
}

// enter records the start of one substrate-touching call and returns the
// function recording its end. Call it as: defer trk.enter(ck, sub, call)().
func (t *overlapTracker) enter(clusterKey, substrate, call string) func() {
	key := clusterKey + "/" + substrate
	t.mu.Lock()
	t.inFlight[key]++
	t.globalIn++
	if t.globalIn > t.globalMax {
		t.globalMax = t.globalIn
	}
	if t.inFlight[key] > 1 {
		t.violations = append(t.violations,
			fmt.Sprintf("%s entered while another span was in flight on %s", call, key))
	}
	// A second concurrent span anywhere releases every parked first entrant:
	// the overlap window has served its purpose (either a violation above, or
	// the cross-cluster overlap the negative control asserts via globalMax).
	if t.globalIn > 1 {
		for k, ch := range t.parked {
			close(ch)
			delete(t.parked, k)
		}
	}
	var park chan struct{}
	if t.globalIn == 1 && !t.heldOnce[key] {
		t.heldOnce[key] = true
		park = make(chan struct{})
		t.parked[key] = park
	}
	t.mu.Unlock()

	if park != nil {
		select {
		case <-park:
		case <-time.After(t.holdTimeout):
			t.mu.Lock()
			if t.parked[key] == park {
				delete(t.parked, key)
			}
			t.mu.Unlock()
		}
	}
	return func() {
		t.mu.Lock()
		t.inFlight[key]--
		t.globalIn--
		t.mu.Unlock()
	}
}

func (t *overlapTracker) report() (violations []string, globalMax int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.violations...), t.globalMax
}

// trackedAdmin wraps a kafka.AdminClient, tracking the ACL-substrate calls.
type trackedAdmin struct {
	kafka.AdminClient
	trk        *overlapTracker
	clusterKey string
}

func (c *trackedAdmin) ListACLs(ctx context.Context) ([]kafka.ACLState, error) {
	defer c.trk.enter(c.clusterKey, "acl", "ListACLs")()
	return c.AdminClient.ListACLs(ctx)
}

func (c *trackedAdmin) CreateACLs(ctx context.Context, acls []kafka.ACLState) error {
	defer c.trk.enter(c.clusterKey, "acl", "CreateACLs")()
	return c.AdminClient.CreateACLs(ctx, acls)
}

func (c *trackedAdmin) DeleteACLs(ctx context.Context, acls []kafka.ACLState) error {
	defer c.trk.enter(c.clusterKey, "acl", "DeleteACLs")()
	return c.AdminClient.DeleteACLs(ctx, acls)
}

// trackedMDS wraps an mds.Client, tracking the RBAC-substrate calls.
type trackedMDS struct {
	mds.Client
	trk        *overlapTracker
	clusterKey string
}

func (c *trackedMDS) ListRoleBindings(ctx context.Context, scope mds.Scope) ([]mds.RoleBinding, error) {
	defer c.trk.enter(c.clusterKey, "rbac", "ListRoleBindings")()
	return c.Client.ListRoleBindings(ctx, scope)
}

func (c *trackedMDS) AddRoleBinding(ctx context.Context, rb mds.RoleBinding) error {
	defer c.trk.enter(c.clusterKey, "rbac", "AddRoleBinding")()
	return c.Client.AddRoleBinding(ctx, rb)
}

func (c *trackedMDS) RemoveRoleBinding(ctx context.Context, rb mds.RoleBinding) error {
	defer c.trk.enter(c.clusterKey, "rbac", "RemoveRoleBinding")()
	return c.Client.RemoveRoleBinding(ctx, rb)
}

// clusterKeyedFactory hands out a per-cluster (tracked) client, keyed by the
// KafkaCluster CR name — needed by the negative control, where each cluster
// must have its own mock so the unserialized overlap cannot race the mocks.
type clusterKeyedFactory struct {
	kafkas map[string]kafka.AdminClient
	mdses  map[string]mds.Client
}

func (f clusterKeyedFactory) For(_ context.Context, c *v1alpha1.KafkaCluster) (kafka.AdminClient, schemaregistry.Client, func(), error) {
	return f.kafkas[c.Name], nil, func() {}, nil
}

func (f clusterKeyedFactory) MDSFor(_ context.Context, c *v1alpha1.KafkaCluster) (mds.Client, error) {
	return f.mdses[c.Name], nil
}

// ---- object builders ----

// lockTestTopic builds an Enforce KafkaTopic with a producer access block (so
// the reconcile compiles + applies ACLs) referencing clusterName.
func lockTestTopic(ns, name, clusterName, topicName, principal string) *v1alpha1.KafkaTopic {
	return &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Generation: 1},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef:     v1alpha1.ClusterRef{Name: clusterName},
			TopicName:      topicName,
			Partitions:     3,
			Reconciliation: &v1alpha1.Reconciliation{Mode: "Enforce"},
			Access: v1alpha1.TopicAccess{
				Producers: []v1alpha1.ProducerAccess{{Principal: principal}},
			},
		},
	}
}

// lockTestPolicy builds an Enforce KafkaAccessPolicy with one Allow rule
// referencing clusterName.
func lockTestPolicy(ns, name, clusterName, topicName, principal string) *v1alpha1.KafkaAccessPolicy {
	return &v1alpha1.KafkaAccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Generation: 1},
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef:     v1alpha1.ClusterRef{Name: clusterName},
			Reconciliation: &v1alpha1.Reconciliation{Mode: "Enforce"},
			Rules: []v1alpha1.ACLRule{{
				Principal:  principal,
				Permission: "Allow",
				Host:       "*",
				Resource:   v1alpha1.ACLResource{Type: "topic", Name: topicName, PatternType: "literal"},
				Operations: []string{"Read", "Describe"},
			}},
		},
	}
}

func plainCluster(ns, name string) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Generation: 1},
		Spec:       v1alpha1.KafkaClusterSpec{BootstrapServers: "localhost:9092"},
	}
}

func req(ns, name string) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}
}

// newLockingFakeClient builds a fake client with the status subresource
// enabled for every kind these tests reconcile (the fake client fails a
// Status().Update with NotFound for unregistered types).
func newLockingFakeClient(t *testing.T, s *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.KafkaTopic{}, &v1alpha1.KafkaAccessPolicy{},
			&v1alpha1.KafkaRoleBinding{}, &v1alpha1.KafkaCluster{}).
		Build()
}

// runConcurrently starts every fn at the same instant and fails the test on
// the first returned error.
func runConcurrently(t *testing.T, fns ...func() error) {
	t.Helper()
	start := make(chan struct{})
	errs := make([]error, len(fns))
	var wg sync.WaitGroup
	for i, fn := range fns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = fn()
		}()
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent reconcile %d: %v", i, err)
		}
	}
}

// overlapHold is the park window for the positive (must-serialize) tests: with
// working locks the first span waits this long for a peer that never comes.
const overlapHold = 200 * time.Millisecond

// ---- tests ----

// TestSubstrateLocking_TwoTopicsSameCluster verifies that two concurrent
// KafkaTopic reconciles on the SAME cluster never overlap on the ACL
// substrate (same-kind concurrency, the --max-concurrent-reconciles >1 case).
func TestSubstrateLocking_TwoTopicsSameCluster(t *testing.T) {
	t.Parallel()
	s := topicScheme(t)
	c := plainCluster("ns1", "prod")
	t1 := lockTestTopic("ns1", "orders", "prod", "payments.orders", "User:p1")
	t2 := lockTestTopic("ns1", "refunds", "prod", "payments.refunds", "User:p2")
	cl := newLockingFakeClient(t, s, c, t1, t2)

	trk := newOverlapTracker(overlapHold)
	k := &trackedAdmin{AdminClient: kafkamock.New(nil, nil), trk: trk, clusterKey: "ns1/prod"}
	reg := &locking.Registry{}
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}, Locks: reg}

	runConcurrently(t,
		func() error { _, err := r.Reconcile(context.Background(), req("ns1", "orders")); return err },
		func() error { _, err := r.Reconcile(context.Background(), req("ns1", "refunds")); return err },
	)

	if violations, _ := trk.report(); len(violations) > 0 {
		t.Fatalf("ACL substrate spans overlapped: %v", violations)
	}
}

// TestSubstrateLocking_TopicVsPolicySameCluster verifies the CROSS-KIND fix:
// a KafkaTopic reconcile and a KafkaAccessPolicy reconcile on the same
// cluster must never overlap on the ACL substrate.
func TestSubstrateLocking_TopicVsPolicySameCluster(t *testing.T) {
	t.Parallel()
	s := topicScheme(t)
	c := plainCluster("ns1", "prod")
	tp := lockTestTopic("ns1", "orders", "prod", "payments.orders", "User:p1")
	pol := lockTestPolicy("ns1", "payments-access", "prod", "payments.orders", "User:svc-payments")
	cl := newLockingFakeClient(t, s, c, tp, pol)

	trk := newOverlapTracker(overlapHold)
	k := &trackedAdmin{AdminClient: kafkamock.New(nil, nil), trk: trk, clusterKey: "ns1/prod"}
	reg := &locking.Registry{}
	tr := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}, Locks: reg}
	pr := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}, Locks: reg}

	runConcurrently(t,
		func() error { _, err := tr.Reconcile(context.Background(), req("ns1", "orders")); return err },
		func() error { _, err := pr.Reconcile(context.Background(), req("ns1", "payments-access")); return err },
	)

	if violations, _ := trk.report(); len(violations) > 0 {
		t.Fatalf("cross-kind ACL substrate spans overlapped: %v", violations)
	}
}

// TestSubstrateLocking_RoleBindingVsRBACTopic verifies the MDS cross-kind
// fix: a KafkaRoleBinding reconcile and a KafkaTopic reconcile whose cluster
// has the rbac access backend (so the topic runs the §40 rbac auto-map) must
// never overlap on the RBAC substrate.
func TestSubstrateLocking_RoleBindingVsRBACTopic(t *testing.T) {
	t.Parallel()
	s := topicScheme(t)
	c := mdsClusterForView("ns1", "prod", "acl", "rbac")
	tp := lockTestTopic("ns1", "orders", "prod", "payments.orders", "User:p1")
	rb := rbForView("ns1", "rb-consumer", "prod")
	cl := newLockingFakeClient(t, s, c, tp, rb)

	trk := newOverlapTracker(overlapHold)
	k := &trackedAdmin{AdminClient: kafkamock.New(nil, nil), trk: trk, clusterKey: "ns1/prod"}
	m := &trackedMDS{Client: mdsmock.New(), trk: trk, clusterKey: "ns1/prod"}
	reg := &locking.Registry{}
	tr := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, mds: m}, Locks: reg}
	rr := &KafkaRoleBindingReconciler{Client: cl, Scheme: s, Clients: stubFactory{mds: m}, Locks: reg}

	runConcurrently(t,
		func() error { _, err := tr.Reconcile(context.Background(), req("ns1", "orders")); return err },
		func() error { _, err := rr.Reconcile(context.Background(), req("ns1", "rb-consumer")); return err },
	)

	if violations, _ := trk.report(); len(violations) > 0 {
		t.Fatalf("RBAC substrate spans overlapped: %v", violations)
	}
}

// TestSubstrateLocking_TopicFinalizerVsPolicy verifies the finalizer path: a
// deleting KafkaTopic's ACL cleanup (co-ownership shield + DeleteACLs) and a
// live KafkaAccessPolicy reconcile on the same cluster must never overlap on
// the ACL substrate.
func TestSubstrateLocking_TopicFinalizerVsPolicy(t *testing.T) {
	t.Parallel()
	s := topicScheme(t)
	c := plainCluster("ns1", "prod")
	tp := lockTestTopic("ns1", "orders", "prod", "payments.orders", "User:p1")
	tp.Spec.DeletionPolicy = "Delete"
	tp.Finalizers = []string{FinalizerName}
	tp.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}
	pol := lockTestPolicy("ns1", "payments-access", "prod", "payments.orders", "User:svc-payments")
	cl := newLockingFakeClient(t, s, c, tp, pol)
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	trk := newOverlapTracker(overlapHold)
	k := &trackedAdmin{AdminClient: kafkamock.New(seededTopics("payments.orders"), nil), trk: trk, clusterKey: "ns1/prod"}
	reg := &locking.Registry{}
	tr := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}, Locks: reg}
	pr := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}, Locks: reg}

	runConcurrently(t,
		func() error { _, err := tr.Reconcile(context.Background(), req("ns1", "orders")); return err },
		func() error { _, err := pr.Reconcile(context.Background(), req("ns1", "payments-access")); return err },
	)

	if violations, _ := trk.report(); len(violations) > 0 {
		t.Fatalf("finalizer vs live-reconcile ACL spans overlapped: %v", violations)
	}
}

// TestSubstrateLocking_PolicyFinalizerVsTopic verifies the policy finalizer
// path: a deleting KafkaAccessPolicy's ACL cleanup (co-ownership shield +
// DeleteACLs in deletePolicyACLs) and a live KafkaTopic reconcile on the same
// cluster must never overlap on the ACL substrate.
func TestSubstrateLocking_PolicyFinalizerVsTopic(t *testing.T) {
	t.Parallel()
	s := topicScheme(t)
	c := plainCluster("ns1", "prod")
	tp := lockTestTopic("ns1", "orders", "prod", "payments.orders", "User:p1")
	pol := lockTestPolicy("ns1", "payments-access", "prod", "payments.orders", "User:svc-payments")
	pol.Finalizers = []string{FinalizerName}
	pol.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}
	cl := newLockingFakeClient(t, s, c, tp, pol)
	if err := cl.Delete(context.Background(), pol); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	trk := newOverlapTracker(overlapHold)
	k := &trackedAdmin{AdminClient: kafkamock.New(nil, nil), trk: trk, clusterKey: "ns1/prod"}
	reg := &locking.Registry{}
	tr := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}, Locks: reg}
	pr := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}, Locks: reg}

	runConcurrently(t,
		func() error { _, err := pr.Reconcile(context.Background(), req("ns1", "payments-access")); return err },
		func() error { _, err := tr.Reconcile(context.Background(), req("ns1", "orders")); return err },
	)

	if violations, _ := trk.report(); len(violations) > 0 {
		t.Fatalf("policy-finalizer vs live-topic ACL spans overlapped: %v", violations)
	}
}

// TestSubstrateLocking_RoleBindingFinalizerVsLive verifies the role-binding
// finalizer path (the mid-function span in handleDeletionWithClient: shield
// view build → RemoveRoleBinding loop): a deleting KafkaRoleBinding's MDS
// cleanup and a live KafkaRoleBinding reconcile on the same cluster must
// never overlap on the RBAC substrate.
func TestSubstrateLocking_RoleBindingFinalizerVsLive(t *testing.T) {
	t.Parallel()
	s := topicScheme(t)
	c := mdsClusterForView("ns1", "prod", "acl", "rbac")
	rbDel := rbForView("ns1", "rb-del", "prod")
	rbDel.Spec.Principal = "User:svc-del"
	rbDel.Spec.Resources[0].Name = "del.topic"
	rbDel.Spec.DeletionPolicy = "Delete"
	rbDel.Finalizers = []string{FinalizerName}
	rbLive := rbForView("ns1", "rb-live", "prod")
	cl := newLockingFakeClient(t, s, c, rbDel, rbLive)
	if err := cl.Delete(context.Background(), rbDel); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	trk := newOverlapTracker(overlapHold)
	m := &trackedMDS{Client: mdsmock.New(), trk: trk, clusterKey: "ns1/prod"}
	reg := &locking.Registry{}
	r := &KafkaRoleBindingReconciler{Client: cl, Scheme: s, Clients: stubFactory{mds: m}, Locks: reg}

	runConcurrently(t,
		func() error { _, err := r.Reconcile(context.Background(), req("ns1", "rb-del")); return err },
		func() error { _, err := r.Reconcile(context.Background(), req("ns1", "rb-live")); return err },
	)

	if violations, _ := trk.report(); len(violations) > 0 {
		t.Fatalf("rolebinding-finalizer vs live-reconcile RBAC spans overlapped: %v", violations)
	}
}

// TestSubstrateLocking_DifferentClustersOverlap is the NEGATIVE control: two
// concurrent topic reconciles on DIFFERENT clusters must be free to overlap —
// proving the substrate locks are per-cluster, not a global mutex. The
// generous park window makes the overlap deterministic: the first span waits
// for the second, which is only blocked if a (wrong) global lock exists.
func TestSubstrateLocking_DifferentClustersOverlap(t *testing.T) {
	t.Parallel()
	s := topicScheme(t)
	ca := plainCluster("ns1", "prod-a")
	cb := plainCluster("ns1", "prod-b")
	ta := lockTestTopic("ns1", "orders-a", "prod-a", "a.orders", "User:pa")
	tb := lockTestTopic("ns1", "orders-b", "prod-b", "b.orders", "User:pb")
	cl := newLockingFakeClient(t, s, ca, cb, ta, tb)

	// Long hold: released the moment the second cluster's span arrives, so the
	// full 5s is only ever waited when a global mutex (wrongly) serializes them.
	trk := newOverlapTracker(5 * time.Second)
	factory := clusterKeyedFactory{kafkas: map[string]kafka.AdminClient{
		"prod-a": &trackedAdmin{AdminClient: kafkamock.New(nil, nil), trk: trk, clusterKey: "ns1/prod-a"},
		"prod-b": &trackedAdmin{AdminClient: kafkamock.New(nil, nil), trk: trk, clusterKey: "ns1/prod-b"},
	}}
	reg := &locking.Registry{}
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: factory, Locks: reg}

	runConcurrently(t,
		func() error { _, err := r.Reconcile(context.Background(), req("ns1", "orders-a")); return err },
		func() error { _, err := r.Reconcile(context.Background(), req("ns1", "orders-b")); return err },
	)

	violations, globalMax := trk.report()
	if len(violations) > 0 {
		t.Fatalf("same-(cluster,substrate) spans overlapped: %v", violations)
	}
	if globalMax < 2 {
		t.Fatalf("spans on different clusters never overlapped (globalMax=%d): the substrate locks behave like a global mutex", globalMax)
	}
}
