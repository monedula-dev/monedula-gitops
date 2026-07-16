package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/operator/locking"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
)

// ---- per-identity overlap tracking (reuses the overlapTracker harness from
// substrate_locking_test.go, keyed by resolved broker identity instead of a
// substrate) ----

// trackedScramAdmin wraps a kafka.AdminClient, tracking the SCRAM-credential
// calls per (cluster, username) — the KafkaUser identity-lock unit. The
// mechanism is deliberately NOT part of the key: the identity lock is keyed on
// the username alone (mirroring the gate), so a deletion span and a live span
// on different mechanisms of one username must still be mutually exclusive.
type trackedScramAdmin struct {
	kafka.AdminClient
	trk        *overlapTracker
	clusterKey string
}

func (c *trackedScramAdmin) ListScramCredentials(ctx context.Context, usernames ...string) ([]kafka.ScramCredential, error) {
	id := "*"
	if len(usernames) > 0 {
		id = usernames[0]
	}
	defer c.trk.enter(c.clusterKey, "user:"+id, "ListScramCredentials")()
	return c.AdminClient.ListScramCredentials(ctx, usernames...)
}

func (c *trackedScramAdmin) UpsertScramCredential(ctx context.Context, up kafka.ScramUpsert) error {
	defer c.trk.enter(c.clusterKey, "user:"+up.User, "UpsertScramCredential")()
	return c.AdminClient.UpsertScramCredential(ctx, up)
}

func (c *trackedScramAdmin) DeleteScramCredential(ctx context.Context, username, mechanism string) error {
	defer c.trk.enter(c.clusterKey, "user:"+username, "DeleteScramCredential")()
	return c.AdminClient.DeleteScramCredential(ctx, username, mechanism)
}

// trackedQuotaAdmin wraps a kafka.AdminClient, tracking the client-quota calls
// under ONE fixed identity key. ListQuotas carries no entity argument, so the
// calls cannot be attributed per-entity from the call site; the tests using
// this wrapper reconcile a single contested identity, making the fixed key
// exact.
type trackedQuotaAdmin struct {
	kafka.AdminClient
	trk        *overlapTracker
	clusterKey string
	identity   string
}

func (c *trackedQuotaAdmin) ListQuotas(ctx context.Context) ([]kafka.QuotaState, error) {
	defer c.trk.enter(c.clusterKey, "quota:"+c.identity, "ListQuotas")()
	return c.AdminClient.ListQuotas(ctx)
}

func (c *trackedQuotaAdmin) SetQuota(ctx context.Context, entity []kafka.QuotaEntityComponent, limits map[string]float64) error {
	defer c.trk.enter(c.clusterKey, "quota:"+c.identity, "SetQuota")()
	return c.AdminClient.SetQuota(ctx, entity, limits)
}

func (c *trackedQuotaAdmin) DeleteQuota(ctx context.Context, entity []kafka.QuotaEntityComponent, keys []string) error {
	defer c.trk.enter(c.clusterKey, "quota:"+c.identity, "DeleteQuota")()
	return c.AdminClient.DeleteQuota(ctx, entity, keys)
}

// sequenceFactory hands out one client per For call, in order — needed by the
// negative control, where two reconciles on the SAME cluster deliberately
// overlap and must therefore not share a (non-thread-safe) mock; the shared
// overlapTracker still observes both. Mirrors clusterKeyedFactory's rationale
// in substrate_locking_test.go.
type sequenceFactory struct {
	mu      sync.Mutex
	clients []kafka.AdminClient
	next    int
}

func (f *sequenceFactory) For(context.Context, *v1alpha1.KafkaCluster) (kafka.AdminClient, schemaregistry.Client, func(), error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.clients[f.next%len(f.clients)]
	f.next++
	return c, nil, func() {}, nil
}

func (f *sequenceFactory) MDSFor(context.Context, *v1alpha1.KafkaCluster) (mds.Client, error) {
	return nil, nil
}

// hideRivals wraps a client so every KafkaUserList / KafkaQuotaList List
// returns EMPTY — simulating an informer cache lagging behind the rival's
// creation, the exact window the identity lock + quorum recheck close. With
// rivals hidden, BOTH same-identity reconciles pass the duplicate gate and
// reach their broker mutations, so only the identity lock can keep the spans
// exclusive.
func hideRivals(base client.WithWatch) client.Client {
	return interceptor.NewClient(base, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			switch list.(type) {
			case *v1alpha1.KafkaUserList, *v1alpha1.KafkaQuotaList:
				return nil // leave the list empty: the cache has not seen the rival yet
			}
			return c.List(ctx, list, opts...)
		},
	})
}

// lockTestUser builds a generate-password KafkaUser claiming username on
// clusterName.
func lockTestUser(ns, name, clusterName, username string) *v1alpha1.KafkaUser {
	return &v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Generation: 1},
		Spec: v1alpha1.KafkaUserSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterName},
			Username:   username,
			Password:   &v1alpha1.UserPassword{Generate: &v1alpha1.GeneratePassword{}},
		},
	}
}

// lockTestQuota builds a KafkaQuota for the given user entity on clusterName.
func lockTestQuota(ns, name, clusterName, user string) *v1alpha1.KafkaQuota {
	return &v1alpha1.KafkaQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Generation: 1},
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterName},
			Entity:     v1alpha1.QuotaEntity{User: user},
			Limits:     v1alpha1.QuotaLimits{ProducerByteRate: ptr.To(1048576.0)},
		},
	}
}

// newIdentityFakeClient builds a fake client with the status subresource for
// the identity-locked kinds these tests reconcile.
func newIdentityFakeClient(t *testing.T, objs ...client.Object) client.WithWatch {
	t.Helper()
	s := topicScheme(t)
	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.KafkaUser{}, &v1alpha1.KafkaQuota{}, &v1alpha1.KafkaCluster{}).
		Build()
}

// TestIdentityLocking_SameUsernameSerialized verifies invariant 3: two
// concurrent KafkaUser reconciles claiming the SAME (cluster, username) never
// overlap their gate→broker-mutation spans. The rivals are hidden from the
// gate's cached scan (hideRivals — simulated cache lag), so BOTH reconciles
// pass the gate and mutate the credential; only the per-identity lock
// serializes them.
func TestIdentityLocking_SameUsernameSerialized(t *testing.T) {
	t.Parallel()
	s := topicScheme(t)
	c := plainCluster("ns1", "prod")
	ua := lockTestUser("ns1", "svc-a", "prod", "svc-shared")
	ub := lockTestUser("ns1", "svc-b", "prod", "svc-shared")
	cl := hideRivals(newIdentityFakeClient(t, c, ua, ub))

	trk := newOverlapTracker(overlapHold)
	k := &trackedScramAdmin{AdminClient: kafkamock.New(nil, nil), trk: trk, clusterKey: "ns1/prod"}
	reg := &locking.Registry{}
	r := &KafkaUserReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}, Locks: reg}

	runConcurrently(t,
		func() error { _, err := r.Reconcile(context.Background(), req("ns1", "svc-a")); return err },
		func() error { _, err := r.Reconcile(context.Background(), req("ns1", "svc-b")); return err },
	)

	if violations, _ := trk.report(); len(violations) > 0 {
		t.Fatalf("same-username credential spans overlapped: %v", violations)
	}
}

// TestIdentityLocking_DifferentUsernamesOverlap is the NEGATIVE control: two
// concurrent KafkaUser reconciles on DIFFERENT usernames (same cluster) must
// be free to overlap — proving the identity locks are per-identity, not a
// per-cluster or global mutex. Long park window as in the substrate negative
// control: only a (wrong) shared lock would make the second reconcile miss it.
func TestIdentityLocking_DifferentUsernamesOverlap(t *testing.T) {
	t.Parallel()
	s := topicScheme(t)
	c := plainCluster("ns1", "prod")
	ua := lockTestUser("ns1", "svc-a", "prod", "svc-alpha")
	ub := lockTestUser("ns1", "svc-b", "prod", "svc-beta")
	cl := newIdentityFakeClient(t, c, ua, ub)

	trk := newOverlapTracker(5 * time.Second)
	factory := &sequenceFactory{clients: []kafka.AdminClient{
		&trackedScramAdmin{AdminClient: kafkamock.New(nil, nil), trk: trk, clusterKey: "ns1/prod"},
		&trackedScramAdmin{AdminClient: kafkamock.New(nil, nil), trk: trk, clusterKey: "ns1/prod"},
	}}
	reg := &locking.Registry{}
	r := &KafkaUserReconciler{Client: cl, Scheme: s, Clients: factory, Locks: reg}

	runConcurrently(t,
		func() error { _, err := r.Reconcile(context.Background(), req("ns1", "svc-a")); return err },
		func() error { _, err := r.Reconcile(context.Background(), req("ns1", "svc-b")); return err },
	)

	violations, globalMax := trk.report()
	if len(violations) > 0 {
		t.Fatalf("distinct-username spans overlapped on ONE key: %v", violations)
	}
	if globalMax < 2 {
		t.Fatalf("spans on different usernames never overlapped (globalMax=%d): the identity locks behave like a shared mutex", globalMax)
	}
}

// TestIdentityLocking_SameQuotaEntitySerialized is the KafkaQuota analogue of
// the same-username test: two concurrent reconciles of the same (cluster,
// entity), rivals hidden from the cached gate scan, must not overlap their
// ListQuotas/SetQuota spans.
func TestIdentityLocking_SameQuotaEntitySerialized(t *testing.T) {
	t.Parallel()
	s := topicScheme(t)
	c := plainCluster("ns1", "prod")
	qa := lockTestQuota("ns1", "quota-a", "prod", "User:alice")
	qb := lockTestQuota("ns1", "quota-b", "prod", "User:alice")
	cl := hideRivals(newIdentityFakeClient(t, c, qa, qb))

	trk := newOverlapTracker(overlapHold)
	k := &trackedQuotaAdmin{AdminClient: kafkamock.New(nil, nil), trk: trk, clusterKey: "ns1/prod", identity: "user=User:alice"}
	reg := &locking.Registry{}
	r := &KafkaQuotaReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}, Locks: reg}

	runConcurrently(t,
		func() error { _, err := r.Reconcile(context.Background(), req("ns1", "quota-a")); return err },
		func() error { _, err := r.Reconcile(context.Background(), req("ns1", "quota-b")); return err },
	)

	if violations, _ := trk.report(); len(violations) > 0 {
		t.Fatalf("same-entity quota spans overlapped: %v", violations)
	}
}

// TestIdentityLocking_UserDeletionVsLiveSameUsername verifies invariant 4 on
// the deletion path: a deleting KafkaUser's co-claimant-scan→broker-cleanup
// span and a live same-username reconcile's gate→mutation span are mutually
// exclusive. The deleting CR carries the default SCRAM-SHA-512 mechanism and
// deletionPolicy Delete; the live rival claims the same username on
// SCRAM-SHA-256, so the deleter's co-claimant scan does NOT shield the cleanup
// (different mechanism = different credential) and BOTH reconciles reach the
// broker — the deleter with DeleteScramCredential, the live CR (whose gate
// skips the deleting rival) with ListScramCredentials/Upsert. Same username →
// same identity lock → the spans must serialize.
func TestIdentityLocking_UserDeletionVsLiveSameUsername(t *testing.T) {
	t.Parallel()
	s := topicScheme(t)
	c := plainCluster("ns1", "prod")
	del := lockTestUser("ns1", "svc-del", "prod", "svc-shared")
	del.Spec.DeletionPolicy = "Delete"
	del.Finalizers = []string{FinalizerName}
	live := lockTestUser("ns1", "svc-live", "prod", "svc-shared")
	live.Spec.Mechanism = "SCRAM-SHA-256"
	cl := newIdentityFakeClient(t, c, del, live)
	if err := cl.Delete(context.Background(), del); err != nil {
		t.Fatalf("mark deleting: %v", err)
	}

	trk := newOverlapTracker(overlapHold)
	k := &trackedScramAdmin{
		AdminClient: kafkamock.NewWithScramCredentials(nil, nil,
			[]kafka.ScramCredential{{User: "svc-shared", Mechanism: "SCRAM-SHA-512", Iterations: 4096}}),
		trk: trk, clusterKey: "ns1/prod",
	}
	reg := &locking.Registry{}
	r := &KafkaUserReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}, Locks: reg}

	runConcurrently(t,
		func() error { _, err := r.Reconcile(context.Background(), req("ns1", "svc-del")); return err },
		func() error { _, err := r.Reconcile(context.Background(), req("ns1", "svc-live")); return err },
	)

	violations, _ := trk.report()
	if len(violations) > 0 {
		t.Fatalf("deletion-cleanup vs live-reconcile spans overlapped: %v", violations)
	}
	// Sanity: both sides really did reach the broker (the test would otherwise
	// pass vacuously).
	if !callsContain(k.AdminClient.(*kafkamock.Client).Calls(), "DeleteScramCredential ") {
		t.Fatalf("deleter never cleaned up; kafka calls = %v", k.AdminClient.(*kafkamock.Client).Calls())
	}
	if !callsContain(k.AdminClient.(*kafkamock.Client).Calls(), "UpsertScramCredential ") {
		t.Fatalf("live reconcile never upserted; kafka calls = %v", k.AdminClient.(*kafkamock.Client).Calls())
	}
}
