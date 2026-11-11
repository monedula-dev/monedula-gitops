package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/tools/events"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
	restmock "github.com/monedula-dev/monedula-gitops/internal/kafkarest/mock"
	"github.com/monedula-dev/monedula-gitops/internal/operator/locking"
)

// listCountingAdmin counts the topic reads a KafkaTopic reconcile issues.
type listCountingAdmin struct {
	*kafkamock.Client
	mu                   sync.Mutex
	listTopics, getTopic int
}

func (a *listCountingAdmin) ListTopics(ctx context.Context) ([]kafka.TopicState, error) {
	a.mu.Lock()
	a.listTopics++
	a.mu.Unlock()
	return a.Client.ListTopics(ctx)
}

func (a *listCountingAdmin) GetTopic(ctx context.Context, name string) (*kafka.TopicState, error) {
	a.mu.Lock()
	a.getTopic++
	a.mu.Unlock()
	return a.Client.GetTopic(ctx, name)
}

// lockProbeREST records, on every ListMirrors, whether the cluster's ACL
// substrate lock could be taken at that moment (i.e. was not held).
type lockProbeREST struct {
	*restmock.Client
	reg     *locking.Registry
	cluster *v1alpha1.KafkaCluster
	aclFree []bool
}

func (p *lockProbeREST) ListMirrors(ctx context.Context) ([]kafkarest.Mirror, error) {
	got := make(chan func(), 1)
	go func() { got <- lockSubstrate(p.reg, p.cluster, locking.SubstrateACL) }()
	select {
	case unlock := <-got:
		unlock()
		p.aclFree = append(p.aclFree, true)
	case <-time.After(100 * time.Millisecond):
		p.aclFree = append(p.aclFree, false)
		go func() { (<-got)() }() // release once the reconcile lets go
	}
	return p.Client.ListMirrors(ctx)
}

// TestTopicReconcile_MirrorGuardReadsTargetedSnapshot: on a REST-configured
// cluster the mirror guard reads only the reconciled topic (GetTopic plus the
// full mirror list), never the whole topic list, and it does so before the
// substrate locks are taken.
func TestTopicReconcile_MirrorGuardReadsTargetedSnapshot(t *testing.T) {
	s := topicScheme(t)
	tp := topicObj("Enforce")
	tp.Spec.Config = map[string]string{"retention.ms": "5"}
	cluster := topicCluster()
	cl := newTopicFakeClient(t, s, tp, cluster)
	km := kafkamock.New(seededTopics("payments.orders"), nil)
	km.SetClusterID("lkc-1")
	k := &listCountingAdmin{Client: km}
	reg := &locking.Registry{}
	rc := &lockProbeREST{Client: restmock.New("lkc-1"), reg: reg, cluster: cluster}
	rc.AddMirror(kafkarest.Mirror{LinkName: "l", MirrorTopicName: "payments.orders", Status: "ACTIVE"})
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: events.NewFakeRecorder(8), Locks: reg,
		Clients: stubFactory{k: k, rest: rc}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if k.listTopics != 0 {
		t.Fatalf("ListTopics called %d times, want 0 (targeted snapshot only)", k.listTopics)
	}
	if calls := km.Calls(); len(calls) != 0 {
		t.Fatalf("live mirror topic mutated: %v", calls)
	}
	if len(rc.aclFree) != 1 {
		t.Fatalf("ListMirrors calls = %d, want 1", len(rc.aclFree))
	}
	if !rc.aclFree[0] {
		t.Fatal("the mirror snapshot must be read before the ACL substrate lock is taken")
	}
}
