//go:build envtest

package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// TestEnvtestTopicDuplicateIdentity is the flagship duplicate-identity flow
// against a real apiserver: two KafkaTopics claiming the same (cluster,
// topicName) identity.
//
//  1. The OLDER topic reconciles to Ready (its path is entirely unaffected).
//  2. The NEWER topic goes terminal: Phase Error, ValidationFailed=True with
//     reason DuplicateIdentity naming the winner, Ready=False, and NO broker
//     mutation (the mock records nothing for the loser's reconcile).
//  3. Re-reconciling the winner while the duplicate exists keeps it Ready.
//  4. Deleting the winner (finalized via its own reconcile) and then forcing a
//     re-reconcile of the loser — standing in for the periodic resync, which
//     is the loser's real-world recovery trigger (there is no same-kind
//     cross-CR watch) — recovers the loser to Ready with ValidationFailed
//     cleared by the engine.
//
// The two topics are created back-to-back, so their creationTimestamps (1s
// apiserver granularity) may be equal; the names are chosen so the
// namespace/name tiebreak selects the same winner ("dup-a" < "dup-b") and the
// test is deterministic either way.
func TestEnvtestTopicDuplicateIdentity(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "dup-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	mkTopic := func(name string) *v1alpha1.KafkaTopic {
		return &v1alpha1.KafkaTopic{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: v1alpha1.KafkaTopicSpec{
				ClusterRef: v1alpha1.ClusterRef{Name: "dup-cluster"},
				TopicName:  "dup.orders",
				Partitions: 3,
			},
		}
	}
	// Create the winner FIRST so it is older by timestamp (and by the
	// namespace/name tiebreak if the timestamps land on the same second).
	if err := env.cl.Create(ctx, mkTopic("dup-a")); err != nil {
		t.Fatalf("create older topic: %v", err)
	}
	if err := env.cl.Create(ctx, mkTopic("dup-b")); err != nil {
		t.Fatalf("create newer topic: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := &KafkaTopicReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{k: mk, sr: schemamock.New()},
	}

	// 1. Older topic wins and reconciles normally.
	reconcileFor(t, r, testNamespace, "dup-a")
	var winner v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "dup-a"}, &winner); err != nil {
		t.Fatalf("re-get winner: %v", err)
	}
	if winner.Status == nil || winner.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("winner status = %+v, want phase Ready", winner.Status)
	}
	if !containsCall(mk.Calls(), "CreateTopic dup.orders") {
		t.Fatalf("winner must create the topic; kafka calls = %v", mk.Calls())
	}

	// 2. Newer topic goes terminal DuplicateIdentity, with zero broker calls.
	callsBefore := len(mk.Calls())
	reconcileFor(t, r, testNamespace, "dup-b")
	if callsAfter := len(mk.Calls()); callsAfter != callsBefore {
		t.Fatalf("loser reconcile touched the broker: calls %v -> %v: %v",
			callsBefore, callsAfter, mk.Calls()[callsBefore:])
	}
	var loser v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "dup-b"}, &loser); err != nil {
		t.Fatalf("re-get loser: %v", err)
	}
	if loser.Status == nil || loser.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("loser status = %+v, want phase Error", loser.Status)
	}
	var vf *metav1.Condition
	for i := range loser.Status.Conditions {
		if loser.Status.Conditions[i].Type == v1alpha1.CondValidationFailed {
			vf = &loser.Status.Conditions[i]
		}
	}
	if vf == nil || vf.Status != metav1.ConditionTrue || vf.Reason != "DuplicateIdentity" {
		t.Fatalf("ValidationFailed = %+v, want True/DuplicateIdentity", vf)
	}
	if !strings.Contains(vf.Message, "KafkaTopic "+testNamespace+"/dup-a") ||
		!strings.Contains(vf.Message, "(older)") {
		t.Fatalf("loser message must name the winner: %q", vf.Message)
	}
	if s := condStatus(loser.Status.Conditions, v1alpha1.CondReady); s != metav1.ConditionFalse {
		t.Fatalf("loser Ready = %q, want False", s)
	}
	if controllerutil.ContainsFinalizer(&loser, FinalizerName) {
		t.Fatal("loser must not gain a finalizer (it owns no broker state)")
	}

	// 3. The winner's path stays unaffected by the standing duplicate.
	reconcileFor(t, r, testNamespace, "dup-a")
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "dup-a"}, &winner); err != nil {
		t.Fatalf("re-get winner: %v", err)
	}
	if winner.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("winner phase after duplicate appeared = %q, want Ready", winner.Status.Phase)
	}

	// 4. Delete the winner (default deletionPolicy Orphan: its reconcile just
	// removes the finalizer), then force a loser re-reconcile — the resync
	// stand-in — and the loser recovers.
	if err := env.cl.Delete(ctx, &winner); err != nil {
		t.Fatalf("delete winner: %v", err)
	}
	reconcileFor(t, r, testNamespace, "dup-a") // finalize
	var gone v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "dup-a"}, &gone); !apierrors.IsNotFound(err) {
		t.Fatalf("winner should be gone after finalization; get err = %v", err)
	}

	reconcileFor(t, r, testNamespace, "dup-b")
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "dup-b"}, &loser); err != nil {
		t.Fatalf("re-get recovered loser: %v", err)
	}
	if loser.Status == nil || loser.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("recovered loser status = %+v, want phase Ready", loser.Status)
	}
	if s := condStatus(loser.Status.Conditions, v1alpha1.CondValidationFailed); s != metav1.ConditionFalse {
		t.Fatalf("recovered loser ValidationFailed = %q, want False (cleared by the engine)", s)
	}
	if !controllerutil.ContainsFinalizer(&loser, FinalizerName) {
		t.Fatal("recovered loser must now own broker state and carry the finalizer")
	}
}

// TestEnvtestTopicDuplicateQuorumRecheckConverges wires a REAL uncached
// apiserver reader through the duplicate gate (the mgr.GetAPIReader()
// equivalent for this manager-less harness: a second client.New against the
// same rest config) and drives a contested (cluster, topicName) pair to
// convergence: the loser goes terminal via a quorum-confirmed rival, the
// winner stays Ready, and after the winner's deletion the loser recovers —
// no flap, no broker double-claim. The rest of this file runs with a nil
// APIReader, pinning that the recheck is optional (skip) for existing suites.
func TestEnvtestTopicDuplicateQuorumRecheckConverges(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	apiReader, err := client.New(env.cfg, client.Options{Scheme: env.scheme})
	if err != nil {
		t.Fatalf("building quorum reader: %v", err)
	}

	if err := env.cl.Create(ctx, newCluster(testNamespace, "recheck-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	mkTopic := func(name string) *v1alpha1.KafkaTopic {
		return &v1alpha1.KafkaTopic{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Spec: v1alpha1.KafkaTopicSpec{
				ClusterRef: v1alpha1.ClusterRef{Name: "recheck-cluster"},
				TopicName:  "recheck.orders",
				Partitions: 3,
			},
		}
	}
	// Winner first (older; "recheck-a" also wins the equal-timestamp tiebreak).
	if err := env.cl.Create(ctx, mkTopic("recheck-a")); err != nil {
		t.Fatalf("create older topic: %v", err)
	}
	if err := env.cl.Create(ctx, mkTopic("recheck-b")); err != nil {
		t.Fatalf("create newer topic: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := &KafkaTopicReconciler{
		Client:    env.cl,
		Scheme:    env.scheme,
		Clients:   stubFactory{k: mk, sr: schemamock.New()},
		APIReader: apiReader,
	}

	// Both CRs are young (never Ready), so BOTH contested-path triggers fire
	// against the real apiserver: the winner rechecks via the
	// never-materialized trigger (and stays the winner), the loser via the
	// loser-path trigger (and the quorum read confirms the rival).
	reconcileFor(t, r, testNamespace, "recheck-a")
	var winner v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "recheck-a"}, &winner); err != nil {
		t.Fatalf("re-get winner: %v", err)
	}
	if winner.Status == nil || winner.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("winner status = %+v, want Ready", winner.Status)
	}

	callsBefore := len(mk.Calls())
	reconcileFor(t, r, testNamespace, "recheck-b")
	if callsAfter := len(mk.Calls()); callsAfter != callsBefore {
		t.Fatalf("loser touched the broker: %v", mk.Calls()[callsBefore:])
	}
	var loser v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "recheck-b"}, &loser); err != nil {
		t.Fatalf("re-get loser: %v", err)
	}
	if loser.Status == nil || loser.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("loser status = %+v, want Error (quorum-confirmed duplicate)", loser.Status)
	}
	if s := condStatus(loser.Status.Conditions, v1alpha1.CondValidationFailed); s != metav1.ConditionTrue {
		t.Fatalf("loser ValidationFailed = %q, want True", s)
	}

	// No flap: re-reconciling both leaves the outcomes stable.
	reconcileFor(t, r, testNamespace, "recheck-a")
	reconcileFor(t, r, testNamespace, "recheck-b")
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "recheck-a"}, &winner); err != nil {
		t.Fatalf("re-get winner: %v", err)
	}
	if winner.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("winner flapped to %q", winner.Status.Phase)
	}

	// Handover: delete + finalize the winner, then the loser converges to
	// Ready via its own quorum recheck (the rival is gone at quorum).
	if err := env.cl.Delete(ctx, &winner); err != nil {
		t.Fatalf("delete winner: %v", err)
	}
	reconcileFor(t, r, testNamespace, "recheck-a") // finalize
	reconcileFor(t, r, testNamespace, "recheck-b")
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "recheck-b"}, &loser); err != nil {
		t.Fatalf("re-get recovered loser: %v", err)
	}
	if loser.Status == nil || loser.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("recovered loser status = %+v, want Ready", loser.Status)
	}
}

// TestEnvtestUserDuplicateLoserDeletionOrphansCredential is the deletion-path
// co-claimant shield against a real apiserver: deleting the LOSER of a
// KafkaUser duplicate pair (the natural remediation for DuplicateIdentity)
// must remove its finalizer WITHOUT deleting the shared SCRAM credential the
// winner keeps managing — the exposed cohort is duplicate pairs that pre-date
// the gate, where both CRs already carry finalizers, so the loser is created
// here with the finalizer pre-set. Once the loser is gone, deleting the
// (now sole) winner must still delete the credential, guarding against
// over-skipping.
func TestEnvtestUserDuplicateLoserDeletionOrphansCredential(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "dup-user-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	pw := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "dup-user-pw", Namespace: testNamespace},
		Data:       map[string][]byte{"password": []byte("pw")},
	}
	if err := env.cl.Create(ctx, pw); err != nil {
		t.Fatalf("create secret: %v", err)
	}
	mkUser := func(name string, finalizers []string) *v1alpha1.KafkaUser {
		return &v1alpha1.KafkaUser{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, Finalizers: finalizers},
			Spec: v1alpha1.KafkaUserSpec{
				ClusterRef:     v1alpha1.ClusterRef{Name: "dup-user-cluster"},
				Username:       "dup-user",
				Password:       secretKeyRefPassword("dup-user-pw"),
				DeletionPolicy: "Delete",
			},
		}
	}
	// Winner first (older by timestamp; "dup-user-a" also wins the tiebreak),
	// then the loser with the finalizer pre-set (a pre-gate duplicate pair).
	if err := env.cl.Create(ctx, mkUser("dup-user-a", nil)); err != nil {
		t.Fatalf("create winner: %v", err)
	}
	if err := env.cl.Create(ctx, mkUser("dup-user-b", []string{FinalizerName})); err != nil {
		t.Fatalf("create loser: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := newUserReconciler(env, mk)

	// The winner reconciles normally and creates the shared credential.
	reconcileFor(t, r, testNamespace, "dup-user-a")
	if creds, _ := mk.ListScramCredentials(ctx, "dup-user"); len(creds) != 1 {
		t.Fatalf("live credentials = %+v, want the winner's credential", creds)
	}

	// Delete the loser and reconcile its deletion: the finalizer is removed
	// (object garbage-collected) but the credential is orphaned to the winner.
	loser := getUser(t, env, "dup-user-b")
	if err := env.cl.Delete(ctx, loser); err != nil {
		t.Fatalf("delete loser: %v", err)
	}
	reconcileFor(t, r, testNamespace, "dup-user-b")
	var gone v1alpha1.KafkaUser
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "dup-user-b"}, &gone); !apierrors.IsNotFound(err) {
		t.Fatalf("loser should be gone after finalization; get err = %v", err)
	}
	if hasCallPrefix(mk.Calls(), "DeleteScramCredential ") {
		t.Fatalf("loser deletion must not touch the credential; kafka calls = %v", mk.Calls())
	}
	if creds, _ := mk.ListScramCredentials(ctx, "dup-user"); len(creds) != 1 {
		t.Fatalf("live credentials = %+v, want the shared credential RETAINED", creds)
	}

	// Guard against over-skipping: with the loser gone, deleting the winner
	// (the identity's only remaining claimant) deletes the credential normally.
	winner := getUser(t, env, "dup-user-a")
	if err := env.cl.Delete(ctx, winner); err != nil {
		t.Fatalf("delete winner: %v", err)
	}
	reconcileFor(t, r, testNamespace, "dup-user-a")
	if creds, _ := mk.ListScramCredentials(ctx, "dup-user"); len(creds) != 0 {
		t.Fatalf("live credentials = %+v, want deleted once no claimant remains", creds)
	}
}

// TestEnvtestQuotaDuplicateLoserDeletionOrphansQuota is the KafkaQuota variant
// of the deletion-path co-claimant shield: deleting the loser of a duplicate
// (cluster, entity) pair removes its finalizer without deleting the entity's
// quota keys, which the winner keeps managing; deleting the last claimant
// still cleans up.
func TestEnvtestQuotaDuplicateLoserDeletionOrphansQuota(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "dup-quota-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	mkQuota := func(name string, finalizers []string) *v1alpha1.KafkaQuota {
		return &v1alpha1.KafkaQuota{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, Finalizers: finalizers},
			Spec: v1alpha1.KafkaQuotaSpec{
				ClusterRef: v1alpha1.ClusterRef{Name: "dup-quota-cluster"},
				Entity:     v1alpha1.QuotaEntity{User: "User:alice"},
				Limits:     v1alpha1.QuotaLimits{ProducerByteRate: ptr.To(1048576.0)},
			},
		}
	}
	if err := env.cl.Create(ctx, mkQuota("dup-quota-a", nil)); err != nil {
		t.Fatalf("create winner: %v", err)
	}
	if err := env.cl.Create(ctx, mkQuota("dup-quota-b", []string{FinalizerName})); err != nil {
		t.Fatalf("create loser: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := &KafkaQuotaReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{k: mk, sr: schemamock.New()},
	}

	// The winner reconciles normally and writes the entity's quota.
	reconcileFor(t, r, testNamespace, "dup-quota-a")
	if quotas, _ := mk.ListQuotas(ctx); len(quotas) != 1 {
		t.Fatalf("mock quotas = %+v, want the winner's quota", quotas)
	}

	// Delete the loser and reconcile its deletion: finalizer removed, quota
	// keys orphaned to the winner (no DeleteQuota call).
	var loser v1alpha1.KafkaQuota
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "dup-quota-b"}, &loser); err != nil {
		t.Fatalf("re-get loser: %v", err)
	}
	if err := env.cl.Delete(ctx, &loser); err != nil {
		t.Fatalf("delete loser: %v", err)
	}
	reconcileFor(t, r, testNamespace, "dup-quota-b")
	var gone v1alpha1.KafkaQuota
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "dup-quota-b"}, &gone); !apierrors.IsNotFound(err) {
		t.Fatalf("loser should be gone after finalization; get err = %v", err)
	}
	if hasCallPrefix(mk.Calls(), "DeleteQuota ") {
		t.Fatalf("loser deletion must not touch the quota; kafka calls = %v", mk.Calls())
	}
	quotas, _ := mk.ListQuotas(ctx)
	if len(quotas) != 1 || quotas[0].Limits["producer_byte_rate"] != 1048576.0 {
		t.Fatalf("mock quotas = %+v, want the shared quota RETAINED", quotas)
	}

	// Guard against over-skipping: deleting the last claimant cleans up.
	var winner v1alpha1.KafkaQuota
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "dup-quota-a"}, &winner); err != nil {
		t.Fatalf("re-get winner: %v", err)
	}
	if err := env.cl.Delete(ctx, &winner); err != nil {
		t.Fatalf("delete winner: %v", err)
	}
	reconcileFor(t, r, testNamespace, "dup-quota-a")
	if quotas, _ := mk.ListQuotas(ctx); len(quotas) != 0 {
		t.Fatalf("mock quotas = %+v, want deleted once no claimant remains", quotas)
	}
}
