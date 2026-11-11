package reconcile

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
	restmock "github.com/monedula-dev/monedula-gitops/internal/kafkarest/mock"
)

const (
	mirrorTestClusterID = "lkc-test"
	mirrorTestLink      = "link-a"
	mirrorTestTopic     = "orders"
	mirrorStallAfter    = 30 * time.Minute
)

var mirrorTestNow = time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

// mkMirror builds a KafkaMirrorTopic "orders" in team-a mirroring "orders" on
// link-a in the given mode ("" = default Enforce).
func mkMirror(mode string) *v1alpha1.KafkaMirrorTopic {
	m := &v1alpha1.KafkaMirrorTopic{
		Spec: v1alpha1.KafkaMirrorTopicSpec{
			ClusterRef:      v1alpha1.ClusterRef{Name: "c"},
			LinkName:        mirrorTestLink,
			SourceTopicName: mirrorTestTopic,
		},
	}
	m.Name = "orders-mirror"
	m.Namespace = "team-a"
	m.Generation = 1
	if mode != "" {
		m.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: mode}
	}
	return m
}

// mirrorFixture returns a healthy destination: matching cluster IDs and an
// ACTIVE link reporting topic.config.sync.include. When status is non-empty the
// topic exists in Kafka and REST reports it as a mirror with that status.
func mirrorFixture(status string) (*kafkamock.Client, *restmock.Client) {
	var topics []kafka.TopicState
	if status != "" {
		topics = []kafka.TopicState{{Name: mirrorTestTopic, Partitions: 3, ReplicationFactor: 3}}
	}
	k := kafkamock.New(topics, nil)
	k.SetClusterID(mirrorTestClusterID)
	r := restmock.New(mirrorTestClusterID)
	r.AddLink(kafkarest.Link{Name: mirrorTestLink, State: "ACTIVE"},
		map[string]string{"topic.config.sync.include": "cleanup.policy"})
	if status != "" {
		r.AddMirror(kafkarest.Mirror{
			LinkName:        mirrorTestLink,
			MirrorTopicName: mirrorTestTopic,
			SourceTopicName: mirrorTestTopic,
			Status:          status,
		})
	}
	return k, r
}

// withPrevStatus presets the status as written by a previous reconcile (or as
// read back after a controller restart).
func withPrevStatus(m *v1alpha1.KafkaMirrorTopic, observed string, since time.Time) {
	t := metav1.NewTime(since)
	m.Status = &v1alpha1.KafkaMirrorTopicStatus{
		ObservedMirrorStatus:         observed,
		LastObservedStatusChangeTime: &t,
	}
}

func reconcileMirror(t *testing.T, m *v1alpha1.KafkaMirrorTopic, cl *v1alpha1.KafkaCluster,
	k kafka.AdminClient, r kafkarest.Client, now time.Time) (v1alpha1.KafkaMirrorTopicStatus, error) {
	t.Helper()
	if cl == nil {
		cl = &v1alpha1.KafkaCluster{}
	}
	return ReconcileMirrorTopic(context.Background(), m, cl, k, r, now, mirrorStallAfter)
}

func mustCond(t *testing.T, conds []metav1.Condition, typ string) *metav1.Condition {
	t.Helper()
	c := meta.FindStatusCondition(conds, typ)
	if c == nil {
		t.Fatalf("condition %s missing; conditions: %+v", typ, conds)
	}
	return c
}

func driftHas(st v1alpha1.KafkaMirrorTopicStatus, action string) bool {
	if st.Drift == nil {
		return false
	}
	for _, f := range st.Drift.Fields {
		if strings.HasPrefix(f, action+" ") {
			return true
		}
	}
	return false
}

func TestReconcileMirrorCreates(t *testing.T) {
	k, r := mirrorFixture("")
	m := mkMirror("")

	st, err := reconcileMirror(t, m, nil, k, r, mirrorTestNow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	created := r.Created()
	if len(created) != 1 {
		t.Fatalf("want 1 created mirror, got %d", len(created))
	}
	if created[0].LinkName != mirrorTestLink || created[0].SourceTopicName != mirrorTestTopic ||
		created[0].MirrorTopicName != mirrorTestTopic {
		t.Fatalf("unexpected create request: %+v", created[0])
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if c := mustCond(t, st.Conditions, v1alpha1.CondReady); c.Status != metav1.ConditionTrue {
		t.Fatalf("Ready = %s (%s)", c.Status, c.Message)
	}
	if st.LinkName != mirrorTestLink || st.SourceTopicName != mirrorTestTopic {
		t.Fatalf("link/source not reported: %+v", st)
	}
	if st.LastAppliedTime == nil || !st.LastAppliedTime.Time.Equal(mirrorTestNow) {
		t.Fatalf("LastAppliedTime = %v, want now", st.LastAppliedTime)
	}
	if st.LastCheckedTime == nil || !st.LastCheckedTime.Time.Equal(mirrorTestNow) {
		t.Fatalf("LastCheckedTime = %v, want now", st.LastCheckedTime)
	}
	if st.ObservedGeneration != 1 {
		t.Fatalf("ObservedGeneration = %d", st.ObservedGeneration)
	}
}

func TestReconcileMirrorRESTNotConfigured(t *testing.T) {
	k, _ := mirrorFixture("")
	m := mkMirror("")

	st, err := ReconcileMirrorTopic(context.Background(), m, &v1alpha1.KafkaCluster{}, k, nil, mirrorTestNow, mirrorStallAfter)
	if err != nil {
		t.Fatalf("RESTNotConfigured is terminal, want nil error, got %v", err)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	vf := mustCond(t, st.Conditions, v1alpha1.CondValidationFailed)
	if vf.Status != metav1.ConditionTrue || vf.Reason != "RESTNotConfigured" {
		t.Fatalf("ValidationFailed = %s/%s", vf.Status, vf.Reason)
	}
	if c := mustCond(t, st.Conditions, v1alpha1.CondReady); c.Status != metav1.ConditionFalse {
		t.Fatalf("Ready = %s", c.Status)
	}
}

func TestReconcileMirrorUnhealthyIsDrift(t *testing.T) {
	k, r := mirrorFixture("FAILED")
	m := mkMirror("")

	st, err := reconcileMirror(t, m, nil, k, r, mirrorTestNow)
	if err != nil {
		t.Fatalf("unhealthy mirror is terminal drift, want nil error, got %v", err)
	}
	if st.Drift == nil || !st.Drift.Detected {
		t.Fatalf("want drift detected, got %+v", st.Drift)
	}
	if !driftHas(st, "MirrorTopicUnhealthy") {
		t.Fatalf("drift fields lack MirrorTopicUnhealthy: %v", st.Drift.Fields)
	}
	if st.Phase != v1alpha1.PhaseDrifted {
		t.Fatalf("phase = %q, want Drifted", st.Phase)
	}
	if st.ObservedMirrorStatus != "FAILED" {
		t.Fatalf("ObservedMirrorStatus = %q", st.ObservedMirrorStatus)
	}
	if c := mustCond(t, st.Conditions, v1alpha1.CondReady); c.Status != metav1.ConditionFalse {
		t.Fatalf("Ready = %s", c.Status)
	}
	if len(r.Created()) != 0 {
		t.Fatalf("no writes expected")
	}
	if st.LastAppliedTime != nil {
		t.Fatalf("LastAppliedTime set without a write: %v", st.LastAppliedTime)
	}
}

func TestReconcileMirrorStateReadError(t *testing.T) {
	for _, mode := range []string{ModeEnforce, ModeDetectOnly, ModeObserveOnly} {
		t.Run(mode, func(t *testing.T) {
			k, r := mirrorFixture("")
			r.Fail("ListMirrors", errors.New("boom"))
			m := mkMirror(mode)

			st, err := reconcileMirror(t, m, nil, k, r, mirrorTestNow)
			if err == nil {
				t.Fatalf("want a retryable error")
			}
			c := mustCond(t, st.Conditions, v1alpha1.CondReady)
			if c.Status != metav1.ConditionFalse || c.Reason != reasonLiveStateError {
				t.Fatalf("Ready = %s/%s", c.Status, c.Reason)
			}
			if st.Phase == v1alpha1.PhaseReady {
				t.Fatalf("phase must never be Ready on a state read error")
			}
			if len(r.Created()) != 0 {
				t.Fatalf("no writes expected on a state read error")
			}
		})
	}
}

func TestReconcileMirrorStatusChangeTime(t *testing.T) {
	k, r := mirrorFixture("PAUSED")
	m := mkMirror("")
	withPrevStatus(m, "ACTIVE", mirrorTestNow.Add(-2*time.Hour))

	st, _ := reconcileMirror(t, m, nil, k, r, mirrorTestNow)
	if st.ObservedMirrorStatus != "PAUSED" {
		t.Fatalf("ObservedMirrorStatus = %q", st.ObservedMirrorStatus)
	}
	if st.LastObservedStatusChangeTime == nil || !st.LastObservedStatusChangeTime.Time.Equal(mirrorTestNow) {
		t.Fatalf("change time = %v, want now", st.LastObservedStatusChangeTime)
	}

	// Same status on a later pass: the change time is kept.
	m.Status = &st
	later := mirrorTestNow.Add(5 * time.Minute)
	st2, _ := reconcileMirror(t, m, nil, k, r, later)
	if st2.LastObservedStatusChangeTime == nil || !st2.LastObservedStatusChangeTime.Time.Equal(mirrorTestNow) {
		t.Fatalf("change time = %v, want unchanged %v", st2.LastObservedStatusChangeTime, mirrorTestNow)
	}
	if !st2.LastCheckedTime.Time.Equal(later) {
		t.Fatalf("LastCheckedTime = %v, want %v", st2.LastCheckedTime, later)
	}
}

func TestReconcileMirrorStallAfterThreshold(t *testing.T) {
	k, r := mirrorFixture("PENDING_SYNCHRONIZE")
	m := mkMirror("")
	since := mirrorTestNow.Add(-31 * time.Minute)
	withPrevStatus(m, "PENDING_SYNCHRONIZE", since)

	st, err := reconcileMirror(t, m, nil, k, r, mirrorTestNow)
	if err != nil {
		t.Fatalf("stall is terminal drift, want nil error, got %v", err)
	}
	if st.Drift == nil || !st.Drift.Detected || !driftHas(st, "MirrorTopicStalled") {
		t.Fatalf("want MirrorTopicStalled drift, got %+v", st.Drift)
	}
	if driftHas(st, "MirrorTopicProgressing") {
		t.Fatalf("Progressing must be replaced by Stalled: %v", st.Drift.Fields)
	}
	if st.Phase != v1alpha1.PhaseDrifted {
		t.Fatalf("phase = %q, want Drifted", st.Phase)
	}
	ready := mustCond(t, st.Conditions, v1alpha1.CondReady)
	if !strings.Contains(ready.Message, "PENDING_SYNCHRONIZE") || !strings.Contains(ready.Message, "31m") {
		t.Fatalf("stall message lacks status/duration: %q", ready.Message)
	}
	if !st.LastObservedStatusChangeTime.Time.Equal(since) {
		t.Fatalf("change time moved: %v", st.LastObservedStatusChangeTime)
	}
}

func TestReconcileMirrorStallClockSurvivesRestart(t *testing.T) {
	k, r := mirrorFixture("PENDING_SYNCHRONIZE")
	m := mkMirror("")
	since := mirrorTestNow.Add(-10 * time.Minute)
	withPrevStatus(m, "PENDING_SYNCHRONIZE", since)

	st, err := reconcileMirror(t, m, nil, k, r, mirrorTestNow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !st.LastObservedStatusChangeTime.Time.Equal(since) {
		t.Fatalf("change time = %v, want unchanged %v", st.LastObservedStatusChangeTime, since)
	}
	if driftHas(st, "MirrorTopicStalled") || (st.Drift != nil && st.Drift.Detected) {
		t.Fatalf("within the window a progressing mirror is not drift: %+v", st.Drift)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if st.ObservedMirrorStatus != "PENDING_SYNCHRONIZE" {
		t.Fatalf("ObservedMirrorStatus = %q", st.ObservedMirrorStatus)
	}
}

func TestReconcileMirrorPendingStoppedImmediateDrift(t *testing.T) {
	k, r := mirrorFixture("PENDING_STOPPED")
	m := mkMirror("")
	// First observation of the status: no time has passed at all.
	withPrevStatus(m, "ACTIVE", mirrorTestNow.Add(-time.Hour))

	st, err := reconcileMirror(t, m, nil, k, r, mirrorTestNow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.Drift == nil || !st.Drift.Detected || !driftHas(st, "MirrorTopicStopping") {
		t.Fatalf("want immediate MirrorTopicStopping drift, got %+v", st.Drift)
	}
	if st.Phase == v1alpha1.PhaseReady {
		t.Fatalf("phase must not be Ready")
	}
	if !st.LastObservedStatusChangeTime.Time.Equal(mirrorTestNow) {
		t.Fatalf("change time = %v, want now", st.LastObservedStatusChangeTime)
	}
}

func TestReconcileMirrorDetectOnlyNoWrites(t *testing.T) {
	k, r := mirrorFixture("")
	m := mkMirror(ModeDetectOnly)

	st, err := reconcileMirror(t, m, nil, k, r, mirrorTestNow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(r.Created()) != 0 {
		t.Fatalf("DetectOnly must not write, created %v", r.Created())
	}
	if st.Drift == nil || !st.Drift.Detected || !driftHas(st, "CreateMirrorTopic") {
		t.Fatalf("want CreateMirrorTopic drift, got %+v", st.Drift)
	}
	if st.Phase != v1alpha1.PhaseDrifted {
		t.Fatalf("phase = %q, want Drifted", st.Phase)
	}
	if st.LastAppliedTime != nil {
		t.Fatalf("LastAppliedTime set in DetectOnly")
	}
}

func TestReconcileMirrorTenancyDenied(t *testing.T) {
	k, r := mirrorFixture("")
	m := mkMirror("")
	cl := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{Tenancy: &v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-a"},
		TopicPrefixes:     []v1alpha1.TopicPrefixRule{{Namespaces: []string{"team-a"}, Prefixes: []string{"finance."}}},
	}}}

	st, err := reconcileMirror(t, m, cl, k, r, mirrorTestNow)
	if err != nil {
		t.Fatalf("tenancy denial is terminal, want nil error, got %v", err)
	}
	vf := mustCond(t, st.Conditions, v1alpha1.CondValidationFailed)
	if vf.Status != metav1.ConditionTrue || vf.Reason != reasonTenancyDenied {
		t.Fatalf("ValidationFailed = %s/%s", vf.Status, vf.Reason)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q", st.Phase)
	}
	if len(r.Created()) != 0 {
		t.Fatalf("no writes expected")
	}
}
