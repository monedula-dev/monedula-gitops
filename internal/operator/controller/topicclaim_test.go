package controller

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// stampedMirror builds a KafkaMirrorTopic with an explicit creationTimestamp
// and UID (the fake client stamps neither) whose effective topic name is
// topicName.
func stampedMirror(ns, name, clusterRef, topicName string, ts metav1.Time) *v1alpha1.KafkaMirrorTopic {
	return &v1alpha1.KafkaMirrorTopic{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name, CreationTimestamp: ts, UID: types.UID("mirror:" + ns + "/" + name), Generation: 1,
		},
		Spec: v1alpha1.KafkaMirrorTopicSpec{
			ClusterRef:      v1alpha1.ClusterRef{Name: clusterRef},
			LinkName:        "prod-to-dr",
			SourceTopicName: topicName,
		},
	}
}

// claimKeys renders claims as "Kind ns/name" for order-insensitive assertions.
func claimKeys(claims []client.Object) map[string]bool {
	out := make(map[string]bool, len(claims))
	for _, c := range claims {
		out[topicClaimKind(c)+" "+c.GetNamespace()+"/"+c.GetName()] = true
	}
	return out
}

func TestTopicClaimName(t *testing.T) {
	topic := stampedTopic("ns1", "orders", "prod", "", dupT0)
	if cl, name, ok := topicClaimName(topic); !ok || cl != "prod" || name != "orders" {
		t.Fatalf("topic claim = (%q, %q, %v), want (prod, orders, true)", cl, name, ok)
	}
	m := stampedMirror("ns1", "m", "dr", "payments.orders", dupT0)
	m.Spec.TopicName = "dr.payments.orders"
	if cl, name, ok := topicClaimName(m); !ok || cl != "dr" || name != "dr.payments.orders" {
		t.Fatalf("mirror claim = (%q, %q, %v), want (dr, dr.payments.orders, true)", cl, name, ok)
	}
	if _, _, ok := topicClaimName(stampedUser("ns1", "u", "prod", "u", dupT0)); ok {
		t.Fatal("a KafkaUser is not a topic claimant")
	}
}

func TestOlderTopicClaimKindTieBreak(t *testing.T) {
	topic := stampedTopic("ns1", "same", "prod", "t", dupT0)
	mirror := stampedMirror("ns1", "same", "prod", "t", dupT0)
	if !olderTopicClaim(topic, mirror) {
		t.Fatal("equal timestamp and namespace/name: KafkaTopic must precede KafkaMirrorTopic")
	}
	if olderTopicClaim(mirror, topic) {
		t.Fatal("equal timestamp and namespace/name: KafkaMirrorTopic must not precede KafkaTopic")
	}
	// The kind tiebreak is the LAST criterion: timestamp and name still decide first.
	olderMirror := stampedMirror("ns1", "zzz", "prod", "t", dupT0)
	newerTopic := stampedTopic("ns1", "aaa", "prod", "t", dupLater())
	if !olderTopicClaim(olderMirror, newerTopic) || olderTopicClaim(newerTopic, olderMirror) {
		t.Fatal("an older mirror must precede a newer topic")
	}
	smallMirror := stampedMirror("ns1", "aaa", "prod", "t", dupT0)
	bigTopic := stampedTopic("ns1", "zzz", "prod", "t", dupT0)
	if !olderTopicClaim(smallMirror, bigTopic) || olderTopicClaim(bigTopic, smallMirror) {
		t.Fatal("equal timestamp: the smaller namespace/name must win before the kind tiebreak")
	}
}

func TestListTopicClaimsBothKinds(t *testing.T) {
	s := topicScheme(t)
	deleting := stampedMirror("ns1", "m-deleting", "prod", "orders", dupT0)
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	deleting.Finalizers = []string{"test/hold"} // the fake client refuses a deleting object without finalizers
	cl := dupFakeClient(t, s,
		stampedTopic("ns1", "t-orders", "prod", "orders", dupT0),
		stampedMirror("ns1", "m-orders", "prod", "orders", dupT0),
		stampedTopic("ns1", "t-other-cluster", "dr", "orders", dupT0),
		stampedMirror("ns1", "m-other-cluster", "dr", "orders", dupT0),
		stampedTopic("ns1", "t-other-topic", "prod", "payments", dupT0),
		deleting,
	)
	claims, err := listTopicClaims(context.Background(), cl, "prod", "orders", "ns1", true)
	if err != nil {
		t.Fatalf("listTopicClaims: %v", err)
	}
	got := claimKeys(claims)
	want := map[string]bool{"KafkaTopic ns1/t-orders": true, "KafkaMirrorTopic ns1/m-orders": true}
	if len(got) != len(want) {
		t.Fatalf("claims = %v, want %v", got, want)
	}
	for k := range want {
		if !got[k] {
			t.Fatalf("claims = %v, missing %q", got, k)
		}
	}
}

func TestListTopicClaimsNamespaceScope(t *testing.T) {
	s := topicScheme(t)
	cl := dupFakeClient(t, s,
		stampedTopic("a", "t", "prod", "orders", dupT0),
		stampedMirror("b", "m", "prod", "orders", dupT0),
	)
	local, err := listTopicClaims(context.Background(), cl, "prod", "orders", "a", true)
	if err != nil {
		t.Fatalf("listTopicClaims local: %v", err)
	}
	if got := claimKeys(local); len(got) != 1 || !got["KafkaTopic a/t"] {
		t.Fatalf("namespaceLocal claims = %v, want only KafkaTopic a/t", got)
	}
	global, err := listTopicClaims(context.Background(), cl, "prod", "orders", "a", false)
	if err != nil {
		t.Fatalf("listTopicClaims global: %v", err)
	}
	if got := claimKeys(global); len(got) != 2 || !got["KafkaTopic a/t"] || !got["KafkaMirrorTopic b/m"] {
		t.Fatalf("global claims = %v, want both namespaces", got)
	}
}

func TestFindOlderTopicClaimantCrossKind(t *testing.T) {
	s := topicScheme(t)
	olderMirror := stampedMirror("ns1", "m", "prod", "orders", dupT0)
	newerTopic := stampedTopic("ns1", "t", "prod", "orders", dupLater())
	cl := dupFakeClient(t, s, olderMirror, newerTopic)
	ctx := context.Background()

	w, err := findOlderTopicClaimant(ctx, cl, newerTopic, true)
	if err != nil {
		t.Fatalf("findOlderTopicClaimant(topic): %v", err)
	}
	if w == nil || topicClaimKind(w) != "KafkaMirrorTopic" || w.GetName() != "m" {
		t.Fatalf("topic's older claimant = %v, want KafkaMirrorTopic ns1/m", w)
	}
	w, err = findOlderTopicClaimant(ctx, cl, olderMirror, true)
	if err != nil {
		t.Fatalf("findOlderTopicClaimant(mirror): %v", err)
	}
	if w != nil {
		t.Fatalf("the older mirror must win; got older claimant %s/%s", w.GetNamespace(), w.GetName())
	}
}

func TestFindOlderTopicClaimantSameNameDifferentKind(t *testing.T) {
	// Same namespace/name and timestamp across kinds: the topic wins, the
	// mirror loses, and neither treats the other as "self".
	s := topicScheme(t)
	topic := stampedTopic("ns1", "same", "prod", "orders", dupT0)
	mirror := stampedMirror("ns1", "same", "prod", "orders", dupT0)
	cl := dupFakeClient(t, s, topic, mirror)
	ctx := context.Background()
	if w, err := findOlderTopicClaimant(ctx, cl, topic, true); err != nil || w != nil {
		t.Fatalf("topic older claimant = %v, %v; want nil", w, err)
	}
	w, err := findOlderTopicClaimant(ctx, cl, mirror, true)
	if err != nil || w == nil || topicClaimKind(w) != "KafkaTopic" {
		t.Fatalf("mirror older claimant = %v, %v; want the KafkaTopic", w, err)
	}
}

func TestFindLiveTopicCoClaimant(t *testing.T) {
	s := topicScheme(t)
	olderTopic := stampedTopic("ns1", "t", "prod", "orders", dupT0)
	newerMirror := stampedMirror("ns1", "m", "prod", "orders", dupLater())
	lone := stampedTopic("ns1", "lone", "prod", "payments", dupT0)
	cl := dupFakeClient(t, s, olderTopic, newerMirror, lone)
	ctx := context.Background()

	// Age-blind: the older topic still sees the newer mirror.
	w, err := findLiveTopicCoClaimant(ctx, cl, olderTopic, true)
	if err != nil || w == nil || topicClaimKind(w) != "KafkaMirrorTopic" {
		t.Fatalf("co-claimant of older topic = %v, %v; want the mirror", w, err)
	}
	w, err = findLiveTopicCoClaimant(ctx, cl, newerMirror, true)
	if err != nil || w == nil || topicClaimKind(w) != "KafkaTopic" {
		t.Fatalf("co-claimant of mirror = %v, %v; want the topic", w, err)
	}
	if w, err := findLiveTopicCoClaimant(ctx, cl, lone, true); err != nil || w != nil {
		t.Fatalf("co-claimant of lone topic = %v, %v; want nil", w, err)
	}
}

func TestTopicGateLosesToOlderMirror(t *testing.T) {
	s := topicScheme(t)
	mirror := stampedMirror("ns1", "m-orders", "prod", "dup.orders", dupT0)
	topic := stampedTopic("ns1", "t-orders", "prod", "dup.orders", dupLater())
	cl := dupFakeClient(t, s, mirror, topic, topicCluster())
	r := &KafkaTopicReconciler{Client: cl, Scheme: s}

	res, done, err := r.duplicateIdentityGate(context.Background(), topic)
	if err != nil || !done || res.RequeueAfter == 0 {
		t.Fatalf("gate = (%+v, %v, %v), want terminal done with resync requeue", res, done, err)
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: "ns1", Name: "t-orders"}, &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	vf := dupCond(got.Status.Conditions, v1alpha1.CondValidationFailed)
	if got.Status.Phase != v1alpha1.PhaseError || vf == nil || vf.Reason != reasonDuplicateIdentity {
		t.Fatalf("status = phase %v, ValidationFailed %+v; want Error/DuplicateIdentity", got.Status.Phase, vf)
	}
	if want := "KafkaMirrorTopic ns1/m-orders"; !strings.Contains(vf.Message, want) {
		t.Fatalf("message %q must name %q", vf.Message, want)
	}
}
