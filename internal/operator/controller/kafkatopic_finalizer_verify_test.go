package controller

// KafkaTopic finalizer mirror-state guard tests (mirror-topics design v6,
// "Ordered cleanup path", "Authoritative final verification algorithm" and
// "Whole-attempt deadline and ACL lock impact"): the REST preflight outside
// the ACL substrate lock, and the single bounded final verification inside it.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
	restconfluent "github.com/monedula-dev/monedula-gitops/internal/kafkarest/confluent"
	restmock "github.com/monedula-dev/monedula-gitops/internal/kafkarest/mock"
	"github.com/monedula-dev/monedula-gitops/internal/mirror"
	"github.com/monedula-dev/monedula-gitops/internal/operator/locking"
	srmock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

const (
	fvClusterID = "lkc-test"
	fvLink      = "dr-link"
	fvTopic     = "payments.orders"
)

// fvDeletingTopic builds a fake-client world with a being-deleted, approved
// Delete-policy KafkaTopic for payments.orders that owns three ACL tuples
// (topic Read+Describe, group Read) and, when withSchema, a content-mode
// value schema whose managed subject is payments.orders-value.
func fvDeletingTopic(t *testing.T, withSchema bool) client.Client {
	t.Helper()
	tp := coOwnedTopic()
	tp.Spec.DeletionPolicy = "Delete"
	tp.Finalizers = []string{FinalizerName}
	tp.Annotations = allowDelete()
	if withSchema {
		tp.Spec.Schema = &v1alpha1.TopicSchema{
			Format:      "AVRO",
			ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Inline: avroBody}},
		}
	}
	cl := newTopicFakeClient(t, topicScheme(t), tp, topicCluster())
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	return cl
}

// fvGetTopic re-reads the deleting topic (DeletionTimestamp set) so the ACL
// co-ownership shield excludes it, exactly as on the real finalizer path.
func fvGetTopic(t *testing.T, cl client.Client) *v1alpha1.KafkaTopic {
	t.Helper()
	var tp v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &tp); err != nil {
		t.Fatalf("get topic: %v", err)
	}
	return &tp
}

// fvKafka returns a Kafka mock bound to fvClusterID holding the topic (when
// present) and the topic's three live ACL tuples.
func fvKafka(present bool) *kafkamock.Client {
	var topics []kafka.TopicState
	if present {
		topics = seededTopics(fvTopic)
	}
	k := kafkamock.New(topics, coOwnedLiveACLs())
	k.SetClusterID(fvClusterID)
	return k
}

// fvREST returns a REST mock bound to fvClusterID with one ACTIVE link and,
// when status is non-empty, a mirror entry for payments.orders.
func fvREST(status string) *restmock.Client {
	r := restmock.New(fvClusterID)
	r.AddLink(kafkarest.Link{Name: fvLink, State: "ACTIVE"}, nil)
	if status != "" {
		r.AddMirror(kafkarest.Mirror{LinkName: fvLink, MirrorTopicName: fvTopic, SourceTopicName: fvTopic, Status: status})
	}
	return r
}

// hookREST wraps the REST mock and counts ListMirrors calls. From the second
// call on it appends later (a mirror that appeared after the preflight), and
// it records, per call, whether the context was single-attempt and bounded
// plus a snapshot of the Kafka mutations issued so far (call ordering).
type hookREST struct {
	*restmock.Client
	k     *kafkamock.Client
	later []kafkarest.Mirror

	mu            sync.Mutex
	n             int
	noRetry       []bool
	hasDeadline   []bool
	kafkaCallsNow [][]string
}

func (h *hookREST) ListMirrors(ctx context.Context) ([]kafkarest.Mirror, error) {
	h.mu.Lock()
	h.n++
	n := h.n
	_, dl := ctx.Deadline()
	h.noRetry = append(h.noRetry, kafkarest.RetryDisabled(ctx))
	h.hasDeadline = append(h.hasDeadline, dl)
	h.kafkaCallsNow = append(h.kafkaCallsNow, h.k.Calls())
	h.mu.Unlock()
	ms, err := h.Client.ListMirrors(ctx)
	if err != nil {
		return nil, err
	}
	if n > 1 {
		ms = append(ms, h.later...)
	}
	return ms, nil
}

// appearingKafka reports the topic absent on the first GetTopic only (the
// preflight read), then delegates — a topic that appears after preflight.
type appearingKafka struct {
	*kafkamock.Client
	n atomic.Int32
}

func (a *appearingKafka) GetTopic(ctx context.Context, name string) (*kafka.TopicState, error) {
	if a.n.Add(1) == 1 {
		return nil, nil
	}
	return a.Client.GetTopic(ctx, name)
}

// assertACLLockFree proves the cluster's ACL substrate lock is not held: a
// fresh acquisition must succeed promptly.
func assertACLLockFree(t *testing.T, reg *locking.Registry, cluster *v1alpha1.KafkaCluster) {
	t.Helper()
	got := make(chan struct{})
	go func() {
		lockSubstrate(reg, cluster, locking.SubstrateACL)()
		close(got)
	}()
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("ACL substrate lock still held after deleteTopicState returned")
	}
}

func TestFinalizerPreflightBlocksLiveMirror(t *testing.T) {
	for _, status := range []string{"ACTIVE", "PENDING_STOPPED"} {
		t.Run(status, func(t *testing.T) {
			cl := fvDeletingTopic(t, false)
			k := fvKafka(true)
			rec := events.NewFakeRecorder(16)
			r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Recorder: rec,
				Clients: stubFactory{k: k, rest: fvREST(status)}}

			res, err := r.Reconcile(context.Background(), topicReq())
			if err != nil {
				t.Fatalf("a live mirror is a policy block, not an error: %v", err)
			}
			if res.RequeueAfter != resyncInterval(0) {
				t.Fatalf("RequeueAfter = %v, want the resync cadence %v", res.RequeueAfter, resyncInterval(0))
			}
			assertFinalizerKept(t, cl)
			if calls := k.Calls(); len(calls) != 0 {
				t.Fatalf("a live mirror must block every cleanup action, kafka calls = %v", calls)
			}
			if evs := drainEvents(rec); !findEvent(evs, "Warning", "MirrorDeleteBlocked", fvTopic) {
				t.Fatalf("expected a MirrorDeleteBlocked warning, got %v", evs)
			}
		})
	}
}

func TestFinalizerPreflightRESTDownBlocks(t *testing.T) {
	cl := fvDeletingTopic(t, false)
	k := fvKafka(true)
	rest := fvREST("")
	rest.Fail("ListMirrors", errors.New("injected: rest 503"))
	rec := events.NewFakeRecorder(16)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Recorder: rec,
		Clients: stubFactory{k: k, rest: rest}}

	_, err := r.Reconcile(context.Background(), topicReq())
	if err == nil || !strings.Contains(err.Error(), "injected: rest 503") {
		t.Fatalf("REST outage must block with its error, got %v", err)
	}
	assertFinalizerKept(t, cl)
	if calls := k.Calls(); len(calls) != 0 {
		t.Fatalf("REST outage must not fall back to unguarded deletion, kafka calls = %v", calls)
	}
	if evs := drainEvents(rec); !findEvent(evs, "Warning", "DeleteBlocked", "injected: rest 503") {
		t.Fatalf("expected a DeleteBlocked warning carrying the error, got %v", evs)
	}
}

// TestFinalizerPreflightUnknownNilErrorBlocks: VerifyTopic may return
// (Unknown, nil) on contradictory evidence — here a mirror entry whose Kafka
// topic is missing. That must block exactly like a read error.
func TestFinalizerPreflightUnknownNilErrorBlocks(t *testing.T) {
	cl := fvDeletingTopic(t, false)
	k := fvKafka(false)
	rec := events.NewFakeRecorder(16)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Recorder: rec,
		Clients: stubFactory{k: k, rest: fvREST("ACTIVE")}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err == nil {
		t.Fatal("an Unknown classification must block with an error")
	}
	assertFinalizerKept(t, cl)
	if calls := k.Calls(); len(calls) != 0 {
		t.Fatalf("Unknown must block every cleanup action, kafka calls = %v", calls)
	}
	if evs := drainEvents(rec); !findEvent(evs, "Warning", "DeleteBlocked", "Unknown") {
		t.Fatalf("expected a DeleteBlocked warning naming Unknown, got %v", evs)
	}
}

func TestFinalizerPreflightBindingFailureBlocks(t *testing.T) {
	cl := fvDeletingTopic(t, false)
	k := fvKafka(true)
	k.SetClusterID("lkc-other")
	rec := events.NewFakeRecorder(16)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Recorder: rec,
		Clients: stubFactory{k: k, rest: fvREST("")}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err == nil {
		t.Fatal("a cluster-ID binding failure must block with an error")
	}
	assertFinalizerKept(t, cl)
	if calls := k.Calls(); len(calls) != 0 {
		t.Fatalf("binding failure must block every cleanup action, kafka calls = %v", calls)
	}
}

// TestFinalizerNormalTopicDeletedWithVerification: the happy REST path. The
// preflight (retrying context, no deadline) runs before any mutation; the
// final verification runs once, single-attempt and bounded, after the ACL
// deletion and before DeleteTopic.
func TestFinalizerNormalTopicDeletedWithVerification(t *testing.T) {
	cl := fvDeletingTopic(t, false)
	k := fvKafka(true)
	rest := &hookREST{Client: fvREST(""), k: k}
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Clients: stubFactory{k: k, rest: rest}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertTopicGone(t, cl)
	if want := []string{"DeleteACLs 3", "DeleteTopic payments.orders"}; !reflect.DeepEqual(k.Calls(), want) {
		t.Fatalf("kafka calls = %v, want %v", k.Calls(), want)
	}
	if rest.n != 2 {
		t.Fatalf("ListMirrors calls = %d, want 2 (preflight + one final verification)", rest.n)
	}
	if rest.noRetry[0] || rest.hasDeadline[0] {
		t.Fatalf("preflight must use the ordinary retrying context, noRetry=%v deadline=%v", rest.noRetry[0], rest.hasDeadline[0])
	}
	if !rest.noRetry[1] || !rest.hasDeadline[1] {
		t.Fatalf("final verification must be single-attempt and bounded, noRetry=%v deadline=%v", rest.noRetry[1], rest.hasDeadline[1])
	}
	if len(rest.kafkaCallsNow[0]) != 0 {
		t.Fatalf("preflight must precede every mutation, saw %v", rest.kafkaCallsNow[0])
	}
	if want := []string{"DeleteACLs 3"}; !reflect.DeepEqual(rest.kafkaCallsNow[1], want) {
		t.Fatalf("final verification must run after ACL deletion and before DeleteTopic, saw %v", rest.kafkaCallsNow[1])
	}
}

func TestFinalizerFinalVerifyBlocksMirrorAppeared(t *testing.T) {
	cl := fvDeletingTopic(t, true)
	k := fvKafka(true)
	sr := srmock.New()
	sr.Seed("payments.orders-value", "AVRO", "", avroBody)
	rest := &hookREST{Client: fvREST(""), k: k, later: []kafkarest.Mirror{
		{LinkName: fvLink, MirrorTopicName: fvTopic, SourceTopicName: fvTopic, Status: "ACTIVE"},
	}}
	rec := events.NewFakeRecorder(16)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Recorder: rec,
		Clients: stubFactory{k: k, sr: sr, rest: rest}}

	_, err := r.Reconcile(context.Background(), topicReq())
	if err == nil || !strings.Contains(err.Error(), "before DeleteTopic") {
		t.Fatalf("a mirror found by the final verification must block with an error naming the stage, got %v", err)
	}
	assertFinalizerKept(t, cl)
	if callsContain(k.Calls(), "DeleteTopic") {
		t.Fatalf("no DeleteTopic after a blocked verification, calls %v", k.Calls())
	}
	if !callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("preflight Normal lets ACL cleanup run before the final verification, calls %v", k.Calls())
	}
	if calls := sr.Calls(); callsContain(calls, "DeleteSubject") {
		t.Fatalf("schema subjects must be untouched after a blocked stage, sr calls %v", calls)
	}
	evs := drainEvents(rec)
	if !findEvent(evs, "Warning", "PartialCleanup", "ACL", "DeleteTopic") {
		t.Fatalf("expected a PartialCleanup warning listing the completed and blocked stages, got %v", evs)
	}
	if !findEvent(evs, "Warning", "DeleteBlocked", "Mirror") {
		t.Fatalf("expected a DeleteBlocked warning naming the classification, got %v", evs)
	}
}

func TestFinalizerFinalVerifyTimeout(t *testing.T) {
	cl := fvDeletingTopic(t, true)
	tp := fvGetTopic(t, cl)
	k := fvKafka(true)
	sr := srmock.New()
	sr.Seed("payments.orders-value", "AVRO", "", avroBody)
	reg := &locking.Registry{}
	cluster := topicCluster()
	rec := events.NewFakeRecorder(16)
	const budget = 50 * time.Millisecond
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Recorder: rec, Locks: reg,
		FinalizerVerificationTimeout: budget}

	var calls atomic.Int32
	var noRetry atomic.Bool
	verify := func(ctx context.Context) (mirror.Classification, error) {
		calls.Add(1)
		noRetry.Store(kafkarest.RetryDisabled(ctx))
		<-ctx.Done()
		return mirror.Unknown, ctx.Err()
	}

	start := time.Now()
	err := r.deleteTopicState(context.Background(), tp, k, sr, nil, cluster, "ns1", verify, mirror.Normal)
	elapsed := time.Since(start)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout must block with the deadline error, got %v", err)
	}
	if elapsed > budget+time.Second {
		t.Fatalf("deleteTopicState took %v, want about the %v budget", elapsed, budget)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("verification attempts = %d, want exactly 1", n)
	}
	if !noRetry.Load() {
		t.Fatal("final verification context must carry kafkarest.WithoutRetry")
	}
	if callsContain(k.Calls(), "DeleteTopic") {
		t.Fatalf("no DeleteTopic after a verification timeout, calls %v", k.Calls())
	}
	if callsContain(sr.Calls(), "DeleteSubject") {
		t.Fatalf("schema subjects must be untouched after a timeout, sr calls %v", sr.Calls())
	}
	assertACLLockFree(t, reg, cluster)
	if evs := drainEvents(rec); !findEvent(evs, "Warning", "PartialCleanup") {
		t.Fatalf("expected a PartialCleanup warning after ACLs were deleted, got %v", evs)
	}
}

// TestFinalizerFinalVerifyLateResultRejected: a verifier that ignores
// cancellation and returns a "safe" class after the deadline produced no valid
// deletion evidence.
func TestFinalizerFinalVerifyLateResultRejected(t *testing.T) {
	cl := fvDeletingTopic(t, false)
	tp := fvGetTopic(t, cl)
	k := fvKafka(true)
	reg := &locking.Registry{}
	cluster := topicCluster()
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Locks: reg,
		FinalizerVerificationTimeout: 20 * time.Millisecond}
	verify := func(ctx context.Context) (mirror.Classification, error) {
		<-ctx.Done()
		return mirror.Normal, nil
	}

	if err := r.deleteTopicState(context.Background(), tp, k, nil, nil, cluster, "ns1", verify, mirror.Normal); err == nil {
		t.Fatal("a result completed after the deadline must not authorize DeleteTopic")
	}
	if callsContain(k.Calls(), "DeleteTopic") {
		t.Fatalf("no DeleteTopic on a late result, calls %v", k.Calls())
	}
	assertACLLockFree(t, reg, cluster)
}

// TestFinalizerFinalVerifyUnderACLLock: the verification runs while the ACL
// substrate lock is held; a waiting ACL writer proceeds once it returns.
func TestFinalizerFinalVerifyUnderACLLock(t *testing.T) {
	cl := fvDeletingTopic(t, false)
	tp := fvGetTopic(t, cl)
	k := fvKafka(true)
	reg := &locking.Registry{}
	cluster := topicCluster()
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Locks: reg}

	writerDone := make(chan struct{})
	var heldDuringVerify bool
	verify := func(ctx context.Context) (mirror.Classification, error) {
		go func() {
			lockSubstrate(reg, cluster, locking.SubstrateACL)()
			close(writerDone)
		}()
		select {
		case <-writerDone:
			heldDuringVerify = false
		case <-time.After(50 * time.Millisecond):
			heldDuringVerify = true
		}
		return mirror.Normal, nil
	}

	if err := r.deleteTopicState(context.Background(), tp, k, nil, nil, cluster, "ns1", verify, mirror.Normal); err != nil {
		t.Fatalf("deleteTopicState: %v", err)
	}
	if !heldDuringVerify {
		t.Fatal("final verification must run under the ACL substrate lock")
	}
	select {
	case <-writerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("waiting ACL writer must continue after the finalizer releases the lock")
	}
	if !callsContain(k.Calls(), "DeleteTopic payments.orders") {
		t.Fatalf("Normal verification must delete the topic, calls %v", k.Calls())
	}
}

// TestFinalizerFinalVerifyDefaultBudget: a zero FinalizerVerificationTimeout
// uses DefaultFinalizerVerificationTimeout.
func TestFinalizerFinalVerifyDefaultBudget(t *testing.T) {
	cl := fvDeletingTopic(t, false)
	tp := fvGetTopic(t, cl)
	k := fvKafka(true)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t)}
	var remaining time.Duration
	verify := func(ctx context.Context) (mirror.Classification, error) {
		dl, ok := ctx.Deadline()
		if !ok {
			return mirror.Unknown, errors.New("no deadline")
		}
		remaining = time.Until(dl)
		return mirror.Normal, nil
	}
	if err := r.deleteTopicState(context.Background(), tp, k, nil, nil, topicCluster(), "ns1", verify, mirror.Normal); err != nil {
		t.Fatalf("deleteTopicState: %v", err)
	}
	if remaining <= DefaultFinalizerVerificationTimeout-time.Second || remaining > DefaultFinalizerVerificationTimeout {
		t.Fatalf("remaining budget = %v, want about %v", remaining, DefaultFinalizerVerificationTimeout)
	}
}

func TestFinalizerFinalVerifySingleAttempt(t *testing.T) {
	var mirrorsReqs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/links/-/mirrors") {
			mirrorsReqs.Add(1)
		}
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	rc, err := restconfluent.New(srv.URL, fvClusterID, nil, srv.Client())
	if err != nil {
		t.Fatalf("confluent.New: %v", err)
	}

	cl := fvDeletingTopic(t, false)
	tp := fvGetTopic(t, cl)
	k := fvKafka(true)
	reg := &locking.Registry{}
	cluster := topicCluster()
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Locks: reg,
		FinalizerVerificationTimeout: 10 * time.Second}
	verify := func(ctx context.Context) (mirror.Classification, error) {
		return mirror.VerifyTopic(ctx, k, rc, fvTopic)
	}

	start := time.Now()
	err = r.deleteTopicState(context.Background(), tp, k, nil, nil, cluster, "ns1", verify, mirror.Normal)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a 503 during final verification must block")
	}
	if n := mirrorsReqs.Load(); n != 1 {
		t.Fatalf("ListMirrors requests = %d, want exactly 1 (no retry)", n)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("deleteTopicState took %v: Retry-After must not be honored under the lock", elapsed)
	}
	if callsContain(k.Calls(), "DeleteTopic") {
		t.Fatalf("no DeleteTopic after a failed verification, calls %v", k.Calls())
	}
	assertACLLockFree(t, reg, cluster)
}

// TestFinalizerAbsentRecoveryAfterRestart: the topic was already deleted (a
// restart before finalizer removal), no mirror entry contradicts it. The
// verification runs before the first ACL deletion, DeleteTopic is skipped,
// the residual ACL cleanup runs and the finalizer is removed.
func TestFinalizerAbsentRecoveryAfterRestart(t *testing.T) {
	cl := fvDeletingTopic(t, false)
	k := fvKafka(false)
	rest := &hookREST{Client: fvREST(""), k: k}
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Clients: stubFactory{k: k, rest: rest}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertTopicGone(t, cl)
	if want := []string{"DeleteACLs 3"}; !reflect.DeepEqual(k.Calls(), want) {
		t.Fatalf("kafka calls = %v, want %v (ACL cleanup, no DeleteTopic)", k.Calls(), want)
	}
	if rest.n != 2 {
		t.Fatalf("ListMirrors calls = %d, want 2 (preflight + one final verification)", rest.n)
	}
	if len(rest.kafkaCallsNow[1]) != 0 {
		t.Fatalf("for a preflight-Absent topic the final verification must precede ACL deletion, saw %v", rest.kafkaCallsNow[1])
	}
}

func TestFinalizerAbsentThenAppeared(t *testing.T) {
	cl := fvDeletingTopic(t, false)
	base := fvKafka(true)
	k := &appearingKafka{Client: base}
	rec := events.NewFakeRecorder(16)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Recorder: rec,
		Clients: stubFactory{k: k, rest: fvREST("")}}

	_, err := r.Reconcile(context.Background(), topicReq())
	if err == nil || !strings.Contains(err.Error(), "topic appeared during deletion; retrying from preflight") {
		t.Fatalf("a topic appearing after an Absent preflight must abort the attempt, got %v", err)
	}
	assertFinalizerKept(t, cl)
	if calls := base.Calls(); len(calls) != 0 {
		t.Fatalf("no cleanup may run when a topic appears after an Absent preflight, calls %v", calls)
	}
	if evs := drainEvents(rec); findEvent(evs, "PartialCleanup") {
		t.Fatalf("nothing was deleted, so no PartialCleanup event, got %v", evs)
	}
}

// TestFinalizerNoRESTLegacyPath: a cluster without spec.rest keeps today's
// behavior exactly: no verification, ACLs then DeleteTopic, then subjects.
func TestFinalizerNoRESTLegacyPath(t *testing.T) {
	cl := fvDeletingTopic(t, true)
	k := fvKafka(true)
	sr := srmock.New()
	sr.Seed("payments.orders-value", "AVRO", "", avroBody)
	rec := events.NewFakeRecorder(16)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Recorder: rec,
		Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertTopicGone(t, cl)
	if want := []string{"DeleteACLs 3", "DeleteTopic payments.orders"}; !reflect.DeepEqual(k.Calls(), want) {
		t.Fatalf("kafka calls = %v, want %v", k.Calls(), want)
	}
	if want := []string{"DeleteSubject payments.orders-value"}; !reflect.DeepEqual(sr.Calls(), want) {
		t.Fatalf("sr calls = %v, want %v", sr.Calls(), want)
	}
	if evs := drainEvents(rec); !findEvent(evs, "Normal", "Deleted") {
		t.Fatalf("expected a Deleted event, got %v", evs)
	}
}

// TestFinalizerNoRESTLegacyAbsent: without REST the legacy path still issues
// the idempotent DeleteTopic for an already-absent topic.
func TestFinalizerNoRESTLegacyAbsent(t *testing.T) {
	cl := fvDeletingTopic(t, false)
	k := fvKafka(false)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertTopicGone(t, cl)
	if want := []string{"DeleteACLs 3", "DeleteTopic payments.orders"}; !reflect.DeepEqual(k.Calls(), want) {
		t.Fatalf("kafka calls = %v, want %v", k.Calls(), want)
	}
}
