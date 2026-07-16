package importer

import (
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNamespaceStrategy_For_Prefix(t *testing.T) {
	s := NamespaceStrategy{Kind: "prefix"}
	if got, err := s.For("payments.orders"); err != nil || got != "payments" {
		t.Fatalf("prefix payments.orders = %q, %v; want payments, nil", got, err)
	}
	// no separator present -> fallback
	if got, err := s.For("orders"); err != nil || got != "default" {
		t.Fatalf("prefix orders = %q, %v; want default, nil", got, err)
	}
	// empty first segment -> fallback
	if got, err := s.For(".orders"); err != nil || got != "default" {
		t.Fatalf("prefix .orders = %q, %v; want default, nil", got, err)
	}
}

func TestNamespaceStrategy_For_PrefixCustomSeparator(t *testing.T) {
	s := NamespaceStrategy{Kind: "prefix", Separator: "_"}
	if got, err := s.For("team_topic"); err != nil || got != "team" {
		t.Fatalf("prefix _ team_topic = %q, %v; want team, nil", got, err)
	}
	// dot is not the separator now -> no separator present -> fallback
	if got, err := s.For("payments.orders"); err != nil || got != "default" {
		t.Fatalf("prefix _ payments.orders = %q, %v; want default, nil", got, err)
	}
}

func TestNamespaceStrategy_For_Single(t *testing.T) {
	s := NamespaceStrategy{Kind: "single", Single: "fixed-ns"}
	if got, err := s.For("anything"); err != nil || got != "fixed-ns" {
		t.Fatalf("single = %q, %v; want fixed-ns, nil", got, err)
	}
	// empty Single -> fallback
	s2 := NamespaceStrategy{Kind: "single"}
	if got, err := s2.For("anything"); err != nil || got != "default" {
		t.Fatalf("single empty = %q, %v; want default, nil", got, err)
	}
}

func TestNamespaceStrategy_For_EmptyKindDefaultsToSingle(t *testing.T) {
	// zero-value strategy is usable: empty Kind behaves as single+fallback.
	var s NamespaceStrategy
	if got, err := s.For("anything"); err != nil || got != "default" {
		t.Fatalf("empty kind = %q, %v; want default, nil", got, err)
	}
	s2 := NamespaceStrategy{Single: "ns"}
	if got, err := s2.For("anything"); err != nil || got != "ns" {
		t.Fatalf("empty kind w/ single = %q, %v; want ns, nil", got, err)
	}
}

func TestNamespaceStrategy_For_Regex(t *testing.T) {
	s := NamespaceStrategy{Kind: "regex", Pattern: `^([a-z]+)\.`}
	if got, err := s.For("payments.orders"); err != nil || got != "payments" {
		t.Fatalf("regex payments.orders = %q, %v; want payments, nil", got, err)
	}
	// non-match -> fallback
	if got, err := s.For("123.orders"); err != nil || got != "default" {
		t.Fatalf("regex 123.orders = %q, %v; want default, nil", got, err)
	}
}

func TestNamespaceStrategy_For_RegexBadPattern(t *testing.T) {
	// no capture group
	s := NamespaceStrategy{Kind: "regex", Pattern: `^abc$`}
	if _, err := s.For("abc"); err == nil {
		t.Fatal("regex with no capture group: want error, got nil")
	}
	// does not compile
	s2 := NamespaceStrategy{Kind: "regex", Pattern: `^(abc`}
	if _, err := s2.For("abc"); err == nil {
		t.Fatal("regex that does not compile: want error, got nil")
	}
}

func TestNamespaceStrategy_For_MappingFile(t *testing.T) {
	s := NamespaceStrategy{Kind: "mapping-file", Mapping: map[string]string{"payments.orders": "pay-ns"}}
	if got, err := s.For("payments.orders"); err != nil || got != "pay-ns" {
		t.Fatalf("mapping hit = %q, %v; want pay-ns, nil", got, err)
	}
	// miss -> fallback
	if got, err := s.For("search.idx"); err != nil || got != "default" {
		t.Fatalf("mapping miss = %q, %v; want default, nil", got, err)
	}
}

func TestNamespaceStrategy_For_CustomFallback(t *testing.T) {
	s := NamespaceStrategy{Kind: "prefix", Fallback: "limbo"}
	if got, err := s.For("orders"); err != nil || got != "limbo" {
		t.Fatalf("custom fallback = %q, %v; want limbo, nil", got, err)
	}
}

func TestNamespaceStrategy_For_UnknownKind(t *testing.T) {
	s := NamespaceStrategy{Kind: "bogus"}
	_, err := s.For("anything")
	if err == nil {
		t.Fatal("unknown kind: want error, got nil")
	}
	if !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("unknown kind error should mention kind; got %v", err)
	}
}

func topic(topicName string) *v1alpha1.KafkaTopic {
	return &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: topicName},
		Spec:       v1alpha1.KafkaTopicSpec{TopicName: topicName},
	}
}

func policyRefs(name string, resources ...v1alpha1.ACLResource) *v1alpha1.KafkaAccessPolicy {
	rules := make([]v1alpha1.ACLRule, 0, len(resources))
	for _, r := range resources {
		rules = append(rules, v1alpha1.ACLRule{Resource: r})
	}
	return &v1alpha1.KafkaAccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha1.KafkaAccessPolicySpec{Rules: rules},
	}
}

func TestAssignNamespaces_TopicsAndPolicies(t *testing.T) {
	res := &Result{
		Topics: []*v1alpha1.KafkaTopic{
			topic("payments.orders"),
			topic("search.idx"),
		},
		Policies: []*v1alpha1.KafkaAccessPolicy{
			// references only payments.orders -> payments
			policyRefs("p1", v1alpha1.ACLResource{Type: "topic", Name: "payments.orders"}),
			// references both topics -> fallback (ambiguous)
			policyRefs("p2",
				v1alpha1.ACLResource{Type: "topic", Name: "payments.orders"},
				v1alpha1.ACLResource{Type: "topic", Name: "search.idx"}),
			// references a non-topic resource only -> fallback
			policyRefs("p3", v1alpha1.ACLResource{Type: "cluster", Name: "kafka-cluster"}),
		},
	}

	if err := AssignNamespaces(res, NamespaceStrategy{Kind: "prefix"}); err != nil {
		t.Fatalf("AssignNamespaces error: %v", err)
	}

	if res.Topics[0].Namespace != "payments" {
		t.Errorf("topic payments.orders ns = %q; want payments", res.Topics[0].Namespace)
	}
	if res.Topics[1].Namespace != "search" {
		t.Errorf("topic search.idx ns = %q; want search", res.Topics[1].Namespace)
	}
	if res.Policies[0].Namespace != "payments" {
		t.Errorf("policy p1 ns = %q; want payments", res.Policies[0].Namespace)
	}
	if res.Policies[1].Namespace != "default" {
		t.Errorf("policy p2 ns = %q; want default (ambiguous)", res.Policies[1].Namespace)
	}
	if res.Policies[2].Namespace != "default" {
		t.Errorf("policy p3 ns = %q; want default (non-topic)", res.Policies[2].Namespace)
	}
}

func TestAssignNamespaces_TopicNameFallsBackToMetadataName(t *testing.T) {
	tp := &v1alpha1.KafkaTopic{ObjectMeta: metav1.ObjectMeta{Name: "team.t"}}
	res := &Result{Topics: []*v1alpha1.KafkaTopic{tp}}
	if err := AssignNamespaces(res, NamespaceStrategy{Kind: "prefix"}); err != nil {
		t.Fatalf("AssignNamespaces error: %v", err)
	}
	if tp.Namespace != "team" {
		t.Errorf("ns = %q; want team (from metadata.name)", tp.Namespace)
	}
}

func TestAssignNamespaces_StampsSchemaFilesViaMetaName(t *testing.T) {
	// A topic whose name itself ends in "-value" proves the link is by MetaName,
	// not by string-stripping the BaseName suffix.
	tp := topic("payments.my-value")
	res := &Result{
		Topics: []*v1alpha1.KafkaTopic{tp},
		SchemaFiles: []SchemaFile{
			{MetaName: "payments.my-value", BaseName: "payments.my-value-value", Ext: "avsc"},
			{MetaName: "payments.my-value", BaseName: "payments.my-value-key", Ext: "avsc"},
		},
	}
	if err := AssignNamespaces(res, NamespaceStrategy{Kind: "prefix"}); err != nil {
		t.Fatalf("AssignNamespaces error: %v", err)
	}
	for i, sf := range res.SchemaFiles {
		if sf.Namespace != "payments" {
			t.Errorf("schema file %d ns = %q; want payments", i, sf.Namespace)
		}
	}
}

func TestAssignNamespaces_SchemaFileUnknownMetaNameFallsBack(t *testing.T) {
	res := &Result{
		Topics: []*v1alpha1.KafkaTopic{topic("payments.orders")},
		SchemaFiles: []SchemaFile{
			{MetaName: "ghost.topic", BaseName: "ghost.topic-value", Ext: "avsc"},
		},
	}
	if err := AssignNamespaces(res, NamespaceStrategy{Kind: "prefix"}); err != nil {
		t.Fatalf("AssignNamespaces error: %v", err)
	}
	if res.SchemaFiles[0].Namespace != "default" {
		t.Errorf("schema file ns = %q; want default fallback", res.SchemaFiles[0].Namespace)
	}
}

func TestAssignNamespaces_PropagatesError(t *testing.T) {
	res := &Result{Topics: []*v1alpha1.KafkaTopic{topic("a.b")}}
	err := AssignNamespaces(res, NamespaceStrategy{Kind: "regex", Pattern: `^abc$`})
	if err == nil {
		t.Fatal("want error from bad regex, got nil")
	}
}

func TestAssignNamespacesStampsRoleBindings(t *testing.T) {
	r := Result{
		Quotas:       []*v1alpha1.KafkaQuota{{ObjectMeta: metav1.ObjectMeta{Name: "quota-x"}}},
		RoleBindings: []*v1alpha1.KafkaRoleBinding{{ObjectMeta: metav1.ObjectMeta{Name: "imported-rb-x"}}},
	}
	err := AssignNamespaces(&r, NamespaceStrategy{Kind: "single", Single: "team-a"})
	if err != nil {
		t.Fatalf("AssignNamespaces error: %v", err)
	}
	// Role bindings are not topic-scoped; they share the same fallback namespace as quotas.
	if r.RoleBindings[0].Namespace != r.Quotas[0].Namespace {
		t.Errorf("role binding ns = %q; want same as quota ns %q", r.RoleBindings[0].Namespace, r.Quotas[0].Namespace)
	}
	if r.RoleBindings[0].Namespace == "" {
		t.Error("role binding namespace must not be empty")
	}
}

func TestAssignNamespacesStampsUsers(t *testing.T) {
	r := Result{
		Quotas: []*v1alpha1.KafkaQuota{{ObjectMeta: metav1.ObjectMeta{Name: "quota-x"}}},
		Users:  []*v1alpha1.KafkaUser{{ObjectMeta: metav1.ObjectMeta{Name: "svc-checkout"}}},
	}
	err := AssignNamespaces(&r, NamespaceStrategy{Kind: "single", Single: "team-a", Fallback: "team-a"})
	if err != nil {
		t.Fatalf("AssignNamespaces error: %v", err)
	}
	// Users are not topic-scoped (a SCRAM credential is a cluster-wide
	// principal); they share the same fallback namespace as quotas.
	if r.Users[0].Namespace != r.Quotas[0].Namespace {
		t.Errorf("user ns = %q; want same as quota ns %q", r.Users[0].Namespace, r.Quotas[0].Namespace)
	}
	if r.Users[0].Namespace != "team-a" {
		t.Errorf("user namespace = %q; want %q", r.Users[0].Namespace, "team-a")
	}
}
