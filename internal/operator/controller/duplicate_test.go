package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
	"github.com/monedula-dev/monedula-gitops/internal/operator"
	srmock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// dupT0 is the "older" creation time used across the duplicate-gate tests;
// newer claimants get dupT0 + an hour so timestamp ordering is unambiguous.
var dupT0 = metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

func dupLater() metav1.Time { return metav1.NewTime(dupT0.Add(time.Hour)) }

// dupFakeClient builds a fake client with status subresources for every kind
// the duplicate-gate tests reconcile.
func dupFakeClient(t *testing.T, s *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(
			&v1alpha1.KafkaTopic{}, &v1alpha1.KafkaQuota{}, &v1alpha1.KafkaUser{},
			&v1alpha1.KafkaRoleBinding{}, &v1alpha1.KafkaCluster{},
		).
		Build()
}

// dupCond returns the named condition or nil.
func dupCond(conds []metav1.Condition, typ string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}
	return nil
}

// stampedTopic builds a KafkaTopic with an explicit creationTimestamp and UID
// (the fake client does not stamp creation times, and the gate skips
// candidates by UID).
func stampedTopic(ns, name, clusterRef, topicName string, ts metav1.Time) *v1alpha1.KafkaTopic {
	return &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name, CreationTimestamp: ts, UID: types.UID(ns + "/" + name), Generation: 1,
		},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			TopicName:  topicName,
			Partitions: 3,
		},
	}
}

// --- ordering core ---

func TestOlderIdentityClaim(t *testing.T) {
	mk := func(ns, name string, ts metav1.Time) client.Object {
		return stampedTopic(ns, name, "prod", "t", ts)
	}
	cases := []struct {
		name string
		a, b client.Object
		want bool
	}{
		{"earlier timestamp wins", mk("ns1", "b", dupT0), mk("ns1", "a", dupLater()), true},
		{"later timestamp loses", mk("ns1", "a", dupLater()), mk("ns1", "b", dupT0), false},
		{"equal ts: smaller name wins", mk("ns1", "aaa", dupT0), mk("ns1", "zzz", dupT0), true},
		{"equal ts: larger name loses", mk("ns1", "zzz", dupT0), mk("ns1", "aaa", dupT0), false},
		{"equal ts: smaller namespace wins", mk("ns-a", "same", dupT0), mk("ns-b", "same", dupT0), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := olderIdentityClaim(tc.a, tc.b); got != tc.want {
				t.Fatalf("olderIdentityClaim = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOldestRival(t *testing.T) {
	obj := stampedTopic("ns1", "mid", "prod", "t", dupT0)
	older := stampedTopic("ns1", "older", "prod", "t", metav1.NewTime(dupT0.Add(-2*time.Hour)))
	oldest := stampedTopic("ns1", "oldest", "prod", "t", metav1.NewTime(dupT0.Add(-4*time.Hour)))
	newer := stampedTopic("ns1", "newer", "prod", "t", dupLater())

	if w := oldestRival(obj, []client.Object{newer}); w != nil {
		t.Fatalf("only-newer rivals must not beat obj, got %v", w.GetName())
	}
	if w := oldestRival(obj, nil); w != nil {
		t.Fatalf("no rivals must yield nil, got %v", w.GetName())
	}
	if w := oldestRival(obj, []client.Object{newer, older, oldest}); w == nil || w.GetName() != "oldest" {
		t.Fatalf("winner = %v, want oldest", w)
	}
}

// --- KafkaTopic (unit; the envtest flagship is duplicate_envtest_test.go) ---

func TestTopicDuplicateGate_NewerLosesTerminally(t *testing.T) {
	s := topicScheme(t)
	older := stampedTopic("ns1", "orders-a", "prod", "dup.orders", dupT0)
	newer := stampedTopic("ns1", "orders-b", "prod", "dup.orders", dupLater())
	cl := dupFakeClient(t, s, older, newer, topicCluster())
	k := kafkamock.New(nil, nil)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, sr: srmock.New()}}

	// v0.36 Task 7: the duplicate-identity gate is one of the terminal-outcome
	// seams outside the controller-runtime-free reconcile engine, so it
	// increments monedula_reconcile_terminal_total directly at its own call
	// site (see duplicate.go) rather than through setTerminalValidationFailed.
	before := operator.ReconcileTerminalCount(controllerKafkaTopic, reasonDuplicateIdentity)

	res, err := r.Reconcile(context.Background(), reqFor("ns1", "orders-b"))
	if err != nil {
		t.Fatalf("loser reconcile must not error (terminal, no backoff): %v", err)
	}
	if got := operator.ReconcileTerminalCount(controllerKafkaTopic, reasonDuplicateIdentity); got != before+1 {
		t.Fatalf("terminal counter = %v, want %v (before %v + 1)", got, before+1, before)
	}
	if res.RequeueAfter != topicRequeueAfter {
		t.Fatalf("RequeueAfter = %v, want resync cadence %v", res.RequeueAfter, topicRequeueAfter)
	}
	if calls := k.Calls(); len(calls) != 0 {
		t.Fatalf("loser must not touch the broker; kafka calls = %v", calls)
	}

	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "orders-b"}, &got); err != nil {
		t.Fatalf("re-get loser: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("status = %+v, want phase Error", got.Status)
	}
	vf := dupCond(got.Status.Conditions, v1alpha1.CondValidationFailed)
	if vf == nil || vf.Status != metav1.ConditionTrue || vf.Reason != reasonDuplicateIdentity {
		t.Fatalf("ValidationFailed = %+v, want True/DuplicateIdentity", vf)
	}
	if !strings.Contains(vf.Message, "KafkaTopic ns1/orders-a") || !strings.Contains(vf.Message, "(older)") {
		t.Fatalf("message must name the winner: %q", vf.Message)
	}
	rd := dupCond(got.Status.Conditions, v1alpha1.CondReady)
	if rd == nil || rd.Status != metav1.ConditionFalse || rd.Reason != reasonDuplicateIdentity {
		t.Fatalf("Ready = %+v, want False/DuplicateIdentity", rd)
	}
	if controllerutil.ContainsFinalizer(&got, FinalizerName) {
		t.Fatal("loser must not gain a finalizer (it owns no broker state)")
	}
}

// TestTopicDuplicateGate_DeletingRivalDoesNotBlock mirrors the KafkaQuota case
// (TestQuotaDuplicateGate_DeletingRivalDoesNotBlock) for KafkaTopic: a deleting
// rival must never block another CR's identity claim, so a copy-paste slip in
// this controller's DeletionTimestamp.IsZero() rival-skip guard would be caught.
func TestTopicDuplicateGate_DeletingRivalDoesNotBlock(t *testing.T) {
	s := topicScheme(t)
	older := stampedTopic("ns1", "orders-a", "prod", "dup.orders", dupT0)
	older.Finalizers = []string{FinalizerName} // so Delete only marks it
	newer := stampedTopic("ns1", "orders-b", "prod", "dup.orders", dupLater())
	cl := dupFakeClient(t, s, older, newer, topicCluster())
	if err := cl.Delete(context.Background(), older); err != nil {
		t.Fatalf("mark older deleting: %v", err)
	}
	k := kafkamock.New(nil, nil)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, sr: srmock.New()}}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "orders-b")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "orders-b"}, &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("a deleting rival must not block; status = %+v", got.Status)
	}
}

// TestTopicDuplicateGate_DeletingLoserStillFinalizes mirrors the KafkaQuota
// case (TestQuotaDuplicateGate_DeletingLoserStillFinalizes) for KafkaTopic: the
// gate must never run on the deletion path, so a deleting loser still reaches
// its finalizer despite an older duplicate existing.
func TestTopicDuplicateGate_DeletingLoserStillFinalizes(t *testing.T) {
	s := topicScheme(t)
	older := stampedTopic("ns1", "orders-a", "prod", "dup.orders", dupT0)
	newer := stampedTopic("ns1", "orders-b", "prod", "dup.orders", dupLater())
	newer.Finalizers = []string{FinalizerName} // simulates a past winning reconcile
	// DeletionPolicy left unset: it resolves to "Orphan" (effectiveDeletionPolicy's
	// final default, no cluster-level override here), so finalizing makes no
	// broker call — keeping this test focused on the gate, not teardown.
	cl := dupFakeClient(t, s, older, newer, topicCluster())
	if err := cl.Delete(context.Background(), newer); err != nil {
		t.Fatalf("mark loser deleting: %v", err)
	}
	k := kafkamock.New(nil, nil)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, sr: srmock.New()}}

	// The gate must NOT run on the deletion path: despite the older duplicate,
	// the deleting loser finalizes and is garbage-collected.
	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "orders-b")); err != nil {
		t.Fatalf("deletion reconcile: %v", err)
	}
	var gone v1alpha1.KafkaTopic
	err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "orders-b"}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("deleting loser must finalize; get err = %v", err)
	}
}

func TestTopicDuplicateGate_OlderWinnerUnaffected(t *testing.T) {
	s := topicScheme(t)
	older := stampedTopic("ns1", "orders-a", "prod", "dup.orders", dupT0)
	newer := stampedTopic("ns1", "orders-b", "prod", "dup.orders", dupLater())
	cl := dupFakeClient(t, s, older, newer, topicCluster())
	k := kafkamock.New(nil, nil)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, sr: srmock.New()}}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "orders-a")); err != nil {
		t.Fatalf("winner reconcile: %v", err)
	}
	if calls := k.Calls(); !callsContain(calls, "CreateTopic dup.orders") {
		t.Fatalf("winner must reconcile normally; kafka calls = %v", calls)
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "orders-a"}, &got); err != nil {
		t.Fatalf("re-get winner: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("winner status = %+v, want phase Ready", got.Status)
	}
	if vf := dupCond(got.Status.Conditions, v1alpha1.CondValidationFailed); vf != nil && vf.Status == metav1.ConditionTrue {
		t.Fatalf("winner must not carry ValidationFailed: %+v", vf)
	}
}

func TestTopicDuplicateGate_TiebreakSmallerNameWins(t *testing.T) {
	s := topicScheme(t)
	// Equal creationTimestamps: the lexicographically smaller namespace/name
	// wins, so "orders-a" beats "orders-z" regardless of reconcile order.
	a := stampedTopic("ns1", "orders-a", "prod", "dup.orders", dupT0)
	z := stampedTopic("ns1", "orders-z", "prod", "dup.orders", dupT0)
	cl := dupFakeClient(t, s, a, z, topicCluster())
	k := kafkamock.New(nil, nil)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, sr: srmock.New()}}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "orders-z")); err != nil {
		t.Fatalf("loser reconcile: %v", err)
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "orders-z"}, &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	vf := dupCond(got.Status.Conditions, v1alpha1.CondValidationFailed)
	if vf == nil || vf.Status != metav1.ConditionTrue || !strings.Contains(vf.Message, "ns1/orders-a") {
		t.Fatalf("tiebreak loser condition = %+v, want True naming ns1/orders-a", vf)
	}
}

func TestTopicDuplicateGate_DifferentIdentityNotGated(t *testing.T) {
	s := topicScheme(t)
	older := stampedTopic("ns1", "orders-a", "prod", "orders.a", dupT0)
	newer := stampedTopic("ns1", "orders-b", "prod", "orders.b", dupLater())
	cl := dupFakeClient(t, s, older, newer, topicCluster())
	k := kafkamock.New(nil, nil)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, sr: srmock.New()}}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "orders-b")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "orders-b"}, &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("distinct identities must both reconcile; status = %+v", got.Status)
	}
}

// --- KafkaQuota ---

func stampedQuota(ns, name, clusterRef, user string, ts metav1.Time) *v1alpha1.KafkaQuota {
	return &v1alpha1.KafkaQuota{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name, CreationTimestamp: ts, UID: types.UID(ns + "/" + name), Generation: 1,
		},
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			Entity:     v1alpha1.QuotaEntity{User: user},
			Limits:     v1alpha1.QuotaLimits{ProducerByteRate: ptr.To(1048576.0)},
		},
	}
}

func TestQuotaDuplicateGate_NewerLosesOlderWins(t *testing.T) {
	s := topicScheme(t)
	older := stampedQuota("ns1", "quota-a", "prod", "User:alice", dupT0)
	newer := stampedQuota("ns1", "quota-b", "prod", "User:alice", dupLater())
	cl := dupFakeClient(t, s, older, newer, topicCluster())
	k := kafkamock.New(nil, nil)
	r := &KafkaQuotaReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	// Newer loses: terminal, no quota written.
	res, err := r.Reconcile(context.Background(), reqFor("ns1", "quota-b"))
	if err != nil {
		t.Fatalf("loser reconcile: %v", err)
	}
	if res.RequeueAfter != quotaRequeueAfter {
		t.Fatalf("RequeueAfter = %v, want %v", res.RequeueAfter, quotaRequeueAfter)
	}
	if quotas, _ := k.ListQuotas(context.Background()); len(quotas) != 0 {
		t.Fatalf("loser must not write a quota; mock has %+v", quotas)
	}
	var lost v1alpha1.KafkaQuota
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "quota-b"}, &lost); err != nil {
		t.Fatalf("re-get loser: %v", err)
	}
	vf := dupCond(lost.Status.Conditions, v1alpha1.CondValidationFailed)
	if lost.Status.Phase != v1alpha1.PhaseError || vf == nil || vf.Status != metav1.ConditionTrue ||
		vf.Reason != reasonDuplicateIdentity || !strings.Contains(vf.Message, "KafkaQuota ns1/quota-a") {
		t.Fatalf("loser status = phase %v, ValidationFailed %+v", lost.Status.Phase, vf)
	}

	// Older wins: reconciles normally.
	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "quota-a")); err != nil {
		t.Fatalf("winner reconcile: %v", err)
	}
	var won v1alpha1.KafkaQuota
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "quota-a"}, &won); err != nil {
		t.Fatalf("re-get winner: %v", err)
	}
	if won.Status == nil || won.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("winner status = %+v, want Ready", won.Status)
	}
	if quotas, _ := k.ListQuotas(context.Background()); len(quotas) != 1 {
		t.Fatalf("winner must write its quota; mock has %+v", quotas)
	}
}

func TestQuotaDuplicateGate_DeletingRivalDoesNotBlock(t *testing.T) {
	s := topicScheme(t)
	older := stampedQuota("ns1", "quota-a", "prod", "User:alice", dupT0)
	older.Finalizers = []string{FinalizerName} // so Delete only marks it
	newer := stampedQuota("ns1", "quota-b", "prod", "User:alice", dupLater())
	cl := dupFakeClient(t, s, older, newer, topicCluster())
	if err := cl.Delete(context.Background(), older); err != nil {
		t.Fatalf("mark older deleting: %v", err)
	}
	k := kafkamock.New(nil, nil)
	r := &KafkaQuotaReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "quota-b")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got v1alpha1.KafkaQuota
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "quota-b"}, &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("a deleting rival must not block; status = %+v", got.Status)
	}
}

// TestQuotaDuplicateGate_ScanFailureIsTransient pins the DuplicateCheckFailed
// path: when the duplicate-identity scan's List call itself fails (as opposed
// to finding an older rival), the reconcile must surface a REAL error so it is
// requeued with backoff — not the terminal nil-error outcome a lost identity
// contest produces — and must report Ready False / DuplicateCheckFailed
// without ever touching the broker. KafkaQuota is the representative kind; the
// gate's List-failure handling is identical across all four kinds (duplicate.go).
func TestQuotaDuplicateGate_ScanFailureIsTransient(t *testing.T) {
	s := topicScheme(t)
	q := stampedQuota("ns1", "quota-a", "prod", "User:alice", dupT0)
	base := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(q, topicCluster()).
		WithStatusSubresource(&v1alpha1.KafkaQuota{}, &v1alpha1.KafkaCluster{}).
		Build()
	listErr := errors.New("injected: apiserver unavailable")
	cl := interceptor.NewClient(base, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*v1alpha1.KafkaQuotaList); ok {
				return listErr
			}
			return c.List(ctx, list, opts...)
		},
	})
	k := kafkamock.New(nil, nil)
	r := &KafkaQuotaReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	_, err := r.Reconcile(context.Background(), reqFor("ns1", "quota-a"))
	if err == nil {
		t.Fatal("scan failure must surface a real error (backoff), got nil")
	}
	if !strings.Contains(err.Error(), "injected: apiserver unavailable") {
		t.Fatalf("error = %v, want it to wrap the injected List failure", err)
	}
	if calls := k.Calls(); len(calls) != 0 {
		t.Fatalf("a failed scan must not touch the broker; kafka calls = %v", calls)
	}

	var got v1alpha1.KafkaQuota
	if gerr := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "quota-a"}, &got); gerr != nil {
		t.Fatalf("re-get: %v", gerr)
	}
	rd := dupCond(got.Status.Conditions, v1alpha1.CondReady)
	if got.Status.Phase != v1alpha1.PhaseError || rd == nil || rd.Status != metav1.ConditionFalse ||
		rd.Reason != reasonDuplicateCheckFailed {
		t.Fatalf("status = phase %v, Ready %+v, want Error/DuplicateCheckFailed", got.Status.Phase, rd)
	}
}

func TestQuotaDuplicateGate_DeletingLoserStillFinalizes(t *testing.T) {
	s := topicScheme(t)
	older := stampedQuota("ns1", "quota-a", "prod", "User:alice", dupT0)
	newer := stampedQuota("ns1", "quota-b", "prod", "User:alice", dupLater())
	newer.Finalizers = []string{FinalizerName} // simulates a past winning reconcile
	cl := dupFakeClient(t, s, older, newer, topicCluster())
	if err := cl.Delete(context.Background(), newer); err != nil {
		t.Fatalf("mark loser deleting: %v", err)
	}
	k := kafkamock.New(nil, nil)
	r := &KafkaQuotaReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	// The gate must NOT run on the deletion path: despite the older duplicate,
	// the deleting loser finalizes and is garbage-collected.
	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "quota-b")); err != nil {
		t.Fatalf("deletion reconcile: %v", err)
	}
	var gone v1alpha1.KafkaQuota
	err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "quota-b"}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("deleting loser must finalize; get err = %v", err)
	}
	// Co-claimant shield: the older CR (quota-a) is a live claimant of the same
	// (cluster, entity) identity, so the loser's finalizer must NOT delete the
	// broker-side quota out from under it — no DeleteQuota call at all.
	if callsContain(k.Calls(), "DeleteQuota ") {
		t.Fatalf("loser deletion must orphan the quota to the surviving claimant; kafka calls = %v", k.Calls())
	}
}

// --- KafkaUser ---

func stampedUser(ns, name, clusterRef, username string, ts metav1.Time) *v1alpha1.KafkaUser {
	return &v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name, CreationTimestamp: ts, UID: types.UID(ns + "/" + name), Generation: 1,
		},
		Spec: v1alpha1.KafkaUserSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			Username:   username,
			Password:   &v1alpha1.UserPassword{Generate: &v1alpha1.GeneratePassword{}},
		},
	}
}

// TestUserDuplicateGate_DeletingRivalDoesNotBlock mirrors the KafkaQuota case
// (TestQuotaDuplicateGate_DeletingRivalDoesNotBlock) for KafkaUser: a deleting
// rival must never block another CR's identity claim, so a copy-paste slip in
// this controller's DeletionTimestamp.IsZero() rival-skip guard would be caught.
func TestUserDuplicateGate_DeletingRivalDoesNotBlock(t *testing.T) {
	s := topicScheme(t)
	older := stampedUser("ns1", "svc-a", "prod", "svc-shared", dupT0)
	older.Finalizers = []string{FinalizerName} // so Delete only marks it
	newer := stampedUser("ns1", "svc-b", "prod", "svc-shared", dupLater())
	cl := dupFakeClient(t, s, older, newer, topicCluster())
	if err := cl.Delete(context.Background(), older); err != nil {
		t.Fatalf("mark older deleting: %v", err)
	}
	k := kafkamock.New(nil, nil)
	r := &KafkaUserReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "svc-b")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got v1alpha1.KafkaUser
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "svc-b"}, &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("a deleting rival must not block; status = %+v", got.Status)
	}
}

// TestUserDuplicateGate_DeletingLoserStillFinalizes mirrors the KafkaQuota
// case (TestQuotaDuplicateGate_DeletingLoserStillFinalizes) for KafkaUser: the
// gate must never run on the deletion path, so a deleting loser still reaches
// its finalizer despite an older duplicate existing.
func TestUserDuplicateGate_DeletingLoserStillFinalizes(t *testing.T) {
	s := topicScheme(t)
	older := stampedUser("ns1", "svc-a", "prod", "svc-shared", dupT0)
	newer := stampedUser("ns1", "svc-b", "prod", "svc-shared", dupLater())
	newer.Finalizers = []string{FinalizerName} // simulates a past winning reconcile
	cl := dupFakeClient(t, s, older, newer, topicCluster())
	if err := cl.Delete(context.Background(), newer); err != nil {
		t.Fatalf("mark loser deleting: %v", err)
	}
	// Seed the shared credential so the survival assertion below is about real
	// broker state, not just the absence of a call.
	k := kafkamock.NewWithScramCredentials(nil, nil,
		[]kafka.ScramCredential{{User: "svc-shared", Mechanism: "SCRAM-SHA-512", Iterations: 4096}})
	r := &KafkaUserReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	// The gate must NOT run on the deletion path: despite the older duplicate,
	// the deleting loser finalizes and is garbage-collected.
	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "svc-b")); err != nil {
		t.Fatalf("deletion reconcile: %v", err)
	}
	var gone v1alpha1.KafkaUser
	err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "svc-b"}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("deleting loser must finalize; get err = %v", err)
	}
	// Co-claimant shield: the older CR (svc-a) is a live claimant of the same
	// (cluster, username, mechanism) credential, so the loser's finalizer must
	// NOT delete it — the shared principal keeps authenticating.
	if callsContain(k.Calls(), "DeleteScramCredential ") {
		t.Fatalf("loser deletion must orphan the credential to the surviving claimant; kafka calls = %v", k.Calls())
	}
	if creds, _ := k.ListScramCredentials(context.Background(), "svc-shared"); len(creds) != 1 {
		t.Fatalf("live credentials = %+v, want the shared credential retained", creds)
	}
}

// TestUserCoClaimantShield_DeletingWinnerOrphansToLoser pins the age-blind
// rule of the deletion-path co-claimant scan (findLiveUserCoClaimant): unlike
// the gate, deleting the WINNER while a loser still exists must ALSO skip the
// broker cleanup — the loser takes over at its next resync and must not find
// the shared credential destroyed and recreated in between.
func TestUserCoClaimantShield_DeletingWinnerOrphansToLoser(t *testing.T) {
	s := topicScheme(t)
	older := stampedUser("ns1", "svc-a", "prod", "svc-shared", dupT0)
	older.Finalizers = []string{FinalizerName} // simulates its past winning reconciles
	newer := stampedUser("ns1", "svc-b", "prod", "svc-shared", dupLater())
	cl := dupFakeClient(t, s, older, newer, topicCluster())
	if err := cl.Delete(context.Background(), older); err != nil {
		t.Fatalf("mark winner deleting: %v", err)
	}
	k := kafkamock.NewWithScramCredentials(nil, nil,
		[]kafka.ScramCredential{{User: "svc-shared", Mechanism: "SCRAM-SHA-512", Iterations: 4096}})
	r := &KafkaUserReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "svc-a")); err != nil {
		t.Fatalf("deletion reconcile: %v", err)
	}
	var gone v1alpha1.KafkaUser
	err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "svc-a"}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("deleting winner must still finalize; get err = %v", err)
	}
	if callsContain(k.Calls(), "DeleteScramCredential ") {
		t.Fatalf("winner deletion must orphan the credential to the waiting loser; kafka calls = %v", k.Calls())
	}
	if creds, _ := k.ListScramCredentials(context.Background(), "svc-shared"); len(creds) != 1 {
		t.Fatalf("live credentials = %+v, want the shared credential retained", creds)
	}
}

// TestUserCoClaimantShield_DifferentMechanismDoesNotSkipCleanup pins the
// mechanism-aware side of the deletion-path scan: a claimant on a DIFFERENT
// mechanism manages a different broker credential, so it must NOT shield the
// deleting CR's cleanup — its own mechanism's credential has no other owner
// and must be deleted, while the other claimant's credential survives.
func TestUserCoClaimantShield_DifferentMechanismDoesNotSkipCleanup(t *testing.T) {
	s := topicScheme(t)
	doomed := stampedUser("ns1", "svc-a", "prod", "svc-shared", dupT0)
	doomed.Finalizers = []string{FinalizerName} // defaulted mechanism: SCRAM-SHA-512
	other := stampedUser("ns1", "svc-b", "prod", "svc-shared", dupLater())
	other.Spec.Mechanism = "SCRAM-SHA-256"
	cl := dupFakeClient(t, s, doomed, other, topicCluster())
	if err := cl.Delete(context.Background(), doomed); err != nil {
		t.Fatalf("mark deleting: %v", err)
	}
	k := kafkamock.NewWithScramCredentials(nil, nil, []kafka.ScramCredential{
		{User: "svc-shared", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
		{User: "svc-shared", Mechanism: "SCRAM-SHA-256", Iterations: 4096},
	})
	r := &KafkaUserReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "svc-a")); err != nil {
		t.Fatalf("deletion reconcile: %v", err)
	}
	if !callsContain(k.Calls(), "DeleteScramCredential ") {
		t.Fatalf("a different-mechanism claimant must not shield cleanup; kafka calls = %v", k.Calls())
	}
	creds, _ := k.ListScramCredentials(context.Background(), "svc-shared")
	if len(creds) != 1 || creds[0].Mechanism != "SCRAM-SHA-256" {
		t.Fatalf("live credentials = %+v, want only the other claimant's SCRAM-SHA-256 credential", creds)
	}
}

func TestUserDuplicateGate_NewerLosesOlderWins(t *testing.T) {
	s := topicScheme(t)
	older := stampedUser("ns1", "svc-a", "prod", "svc-shared", dupT0)
	newer := stampedUser("ns1", "svc-b", "prod", "svc-shared", dupLater())
	// The loser carries a pre-existing DriftDetected=True from an earlier,
	// pre-conflict reconcile (e.g. it won the identity once, drifted, then a
	// duplicate showed up and out-aged it). The gate must clear this, not just
	// forward-copy it — a stale True condition alongside a zeroed gauge would
	// contradict itself.
	newer.Status = &v1alpha1.KafkaUserStatus{}
	meta.SetStatusCondition(&newer.Status.Conditions, metav1.Condition{
		Type:               v1alpha1.CondDriftDetected,
		Status:             metav1.ConditionTrue,
		Reason:             "DriftDetected",
		Message:            "stale drift from a prior reconcile",
		ObservedGeneration: newer.Generation,
	})
	cl := dupFakeClient(t, s, older, newer, topicCluster())
	k := kafkamock.New(nil, nil)
	r := &KafkaUserReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "svc-b")); err != nil {
		t.Fatalf("loser reconcile: %v", err)
	}
	if calls := k.Calls(); len(calls) != 0 {
		t.Fatalf("loser must not touch the broker; kafka calls = %v", calls)
	}
	var lost v1alpha1.KafkaUser
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "svc-b"}, &lost); err != nil {
		t.Fatalf("re-get loser: %v", err)
	}
	vf := dupCond(lost.Status.Conditions, v1alpha1.CondValidationFailed)
	if lost.Status.Phase != v1alpha1.PhaseError || vf == nil || vf.Status != metav1.ConditionTrue ||
		vf.Reason != reasonDuplicateIdentity || !strings.Contains(vf.Message, "KafkaUser ns1/svc-a") {
		t.Fatalf("loser status = phase %v, ValidationFailed %+v", lost.Status.Phase, vf)
	}
	dd := dupCond(lost.Status.Conditions, v1alpha1.CondDriftDetected)
	if dd == nil || dd.Status != metav1.ConditionFalse {
		t.Fatalf("loser DriftDetected = %+v, want False (stale True must not survive the gate)", dd)
	}
	v, ok := gaugeSeries(t, "monedula_kafka_user_drift_detected",
		map[string]string{"namespace": "ns1", "name": "svc-b"})
	if !ok || v != 0 {
		t.Fatalf("loser drift gauge = (%v, ok=%v), want (0, true)", v, ok)
	}

	// The winner reconciles normally (never sees the DuplicateIdentity gate).
	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "svc-a")); err != nil {
		t.Fatalf("winner reconcile: %v", err)
	}
	var won v1alpha1.KafkaUser
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "svc-a"}, &won); err != nil {
		t.Fatalf("re-get winner: %v", err)
	}
	if vf := dupCond(won.Status.Conditions, v1alpha1.CondValidationFailed); vf != nil && vf.Status == metav1.ConditionTrue {
		t.Fatalf("winner must not be gated: %+v", vf)
	}
}

// --- KafkaRoleBinding ---

func stampedRoleBinding(ns, name, clusterRef string, ts metav1.Time) *v1alpha1.KafkaRoleBinding {
	rb := clusterScopedRoleBinding(ns, name, clusterRef)
	rb.CreationTimestamp = ts
	rb.UID = types.UID(ns + "/" + name)
	rb.Generation = 1
	return rb
}

// TestRoleBindingDuplicateGate_DeletingRivalDoesNotBlock mirrors the
// KafkaQuota case (TestQuotaDuplicateGate_DeletingRivalDoesNotBlock) for
// KafkaRoleBinding: a deleting rival must never block another CR's identity
// claim, so a copy-paste slip in this controller's DeletionTimestamp.IsZero()
// rival-skip guard would be caught.
func TestRoleBindingDuplicateGate_DeletingRivalDoesNotBlock(t *testing.T) {
	s := newScheme(t)
	older := stampedRoleBinding("ns1", "rb-a", "prod", dupT0)
	older.Finalizers = []string{FinalizerName} // so Delete only marks it
	newer := stampedRoleBinding("ns1", "rb-b", "prod", dupLater())
	c := mdsCluster("ns1", "prod")
	cl := dupFakeClient(t, s, older, newer, c)
	if err := cl.Delete(context.Background(), older); err != nil {
		t.Fatalf("mark older deleting: %v", err)
	}
	mock := mdsmock.New()
	r := &KafkaRoleBindingReconciler{Client: cl, Scheme: s, Clients: stubFactory{mds: mock}}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "rb-b")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got v1alpha1.KafkaRoleBinding
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "rb-b"}, &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("a deleting rival must not block; status = %+v", got.Status)
	}
}

// TestRoleBindingDuplicateGate_DeletingLoserStillFinalizes mirrors the
// KafkaQuota case (TestQuotaDuplicateGate_DeletingLoserStillFinalizes) for
// KafkaRoleBinding: the gate must never run on the deletion path, so a
// deleting loser still reaches its finalizer despite an older duplicate
// existing.
func TestRoleBindingDuplicateGate_DeletingLoserStillFinalizes(t *testing.T) {
	s := newScheme(t)
	older := stampedRoleBinding("ns1", "rb-a", "prod", dupT0)
	newer := stampedRoleBinding("ns1", "rb-b", "prod", dupLater())
	newer.Finalizers = []string{FinalizerName} // simulates a past winning reconcile
	// DeletionPolicy left unset: it resolves to "Orphan" (the controller's
	// default when spec.deletionPolicy is empty), so finalizing makes no MDS
	// call — keeping this test focused on the gate, not teardown.
	c := mdsCluster("ns1", "prod")
	cl := dupFakeClient(t, s, older, newer, c)
	if err := cl.Delete(context.Background(), newer); err != nil {
		t.Fatalf("mark loser deleting: %v", err)
	}
	mock := mdsmock.New()
	r := &KafkaRoleBindingReconciler{Client: cl, Scheme: s, Clients: stubFactory{mds: mock}}

	// The gate must NOT run on the deletion path: despite the older duplicate,
	// the deleting loser finalizes and is garbage-collected.
	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "rb-b")); err != nil {
		t.Fatalf("deletion reconcile: %v", err)
	}
	var gone v1alpha1.KafkaRoleBinding
	err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "rb-b"}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("deleting loser must finalize; get err = %v", err)
	}
}

func TestRoleBindingDuplicateGate_NewerLosesOlderWins(t *testing.T) {
	s := newScheme(t)
	older := stampedRoleBinding("ns1", "rb-a", "prod", dupT0)
	newer := stampedRoleBinding("ns1", "rb-b", "prod", dupLater())
	c := mdsCluster("ns1", "prod")
	cl := dupFakeClient(t, s, older, newer, c)
	mock := mdsmock.New()
	r := &KafkaRoleBindingReconciler{Client: cl, Scheme: s, Clients: stubFactory{mds: mock}}

	// Newer loses before the MDS client is even built.
	res, err := r.Reconcile(context.Background(), reqFor("ns1", "rb-b"))
	if err != nil {
		t.Fatalf("loser reconcile: %v", err)
	}
	if res.RequeueAfter != roleBindingRequeueAfter {
		t.Fatalf("RequeueAfter = %v, want %v", res.RequeueAfter, roleBindingRequeueAfter)
	}
	if calls := mock.Calls(); len(calls) != 0 {
		t.Fatalf("loser must not touch MDS; calls = %v", calls)
	}
	var lost v1alpha1.KafkaRoleBinding
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "rb-b"}, &lost); err != nil {
		t.Fatalf("re-get loser: %v", err)
	}
	vf := dupCond(lost.Status.Conditions, v1alpha1.CondValidationFailed)
	if lost.Status.Phase != v1alpha1.PhaseError || vf == nil || vf.Status != metav1.ConditionTrue ||
		vf.Reason != reasonDuplicateIdentity || !strings.Contains(vf.Message, "KafkaRoleBinding ns1/rb-a") {
		t.Fatalf("loser status = phase %v, ValidationFailed %+v", lost.Status.Phase, vf)
	}

	// Older wins: reconciles normally and writes its binding.
	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "rb-a")); err != nil {
		t.Fatalf("winner reconcile: %v", err)
	}
	var won v1alpha1.KafkaRoleBinding
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "rb-a"}, &won); err != nil {
		t.Fatalf("re-get winner: %v", err)
	}
	if won.Status == nil || won.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("winner status = %+v, want Ready", won.Status)
	}
}

func TestRoleBindingDuplicateGate_DisjointBindingsNotGated(t *testing.T) {
	s := newScheme(t)
	older := stampedRoleBinding("ns1", "rb-a", "prod", dupT0)
	newer := stampedRoleBinding("ns1", "rb-b", "prod", dupLater())
	newer.Spec.Principal = "User:bob" // different principal -> disjoint identity
	c := mdsCluster("ns1", "prod")
	cl := dupFakeClient(t, s, older, newer, c)
	mock := mdsmock.New()
	r := &KafkaRoleBindingReconciler{Client: cl, Scheme: s, Clients: stubFactory{mds: mock}}

	if _, err := r.Reconcile(context.Background(), reqFor("ns1", "rb-b")); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got v1alpha1.KafkaRoleBinding
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "rb-b"}, &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("disjoint bindings must both reconcile; status = %+v", got.Status)
	}
}

// reqFor builds a reconcile request for ns/name.
func reqFor(ns, name string) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}
}

// TestRoleBindingMDSNotConfiguredIncrementsTerminalCounter pins v0.36 Task 7's
// terminal-outcome counter for the MDSNotConfigured branch, which lives
// outside the reconcile engine (kafkarolebinding_controller.go, not
// duplicate.go) and so is instrumented at its own call site. A KafkaCluster
// with no authorization.mds makes stubFactory.MDSFor return (nil, nil),
// exercising the same "cluster has no authorization.mds configured" path
// production hits.
func TestRoleBindingMDSNotConfiguredIncrementsTerminalCounter(t *testing.T) {
	s := newScheme(t)
	rb := stampedRoleBinding("ns1", "rb-a", "prod", dupT0)
	noMDS := &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "prod"},
		Spec:       v1alpha1.KafkaClusterSpec{BootstrapServers: "localhost:9092"}, // no Authorization.MDS
	}
	cl := dupFakeClient(t, s, rb, noMDS)
	r := &KafkaRoleBindingReconciler{Client: cl, Scheme: s, Clients: stubFactory{}} // MDSFor returns (nil, nil)

	before := operator.ReconcileTerminalCount(controllerKafkaRoleBinding, "MDSNotConfigured")

	res, err := r.Reconcile(context.Background(), reqFor("ns1", "rb-a"))
	if err != nil {
		t.Fatalf("MDSNotConfigured is terminal, want nil error, got: %v", err)
	}
	if res.RequeueAfter != roleBindingRequeueAfter {
		t.Fatalf("RequeueAfter = %v, want resync cadence %v (the MDSNotConfigured fallback)", res.RequeueAfter, roleBindingRequeueAfter)
	}
	if got := operator.ReconcileTerminalCount(controllerKafkaRoleBinding, "MDSNotConfigured"); got != before+1 {
		t.Fatalf("terminal counter = %v, want %v (before %v + 1)", got, before+1, before)
	}

	var got v1alpha1.KafkaRoleBinding
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "rb-a"}, &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("status = %+v, want phase Error", got.Status)
	}
}
