package tenancy

import (
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// makeConfig is a convenience builder for TenancyConfig in table tests.
func makeConfig(allowedNS []string, rules []v1alpha1.TopicPrefixRule) *v1alpha1.TenancyConfig {
	return &v1alpha1.TenancyConfig{
		AllowedNamespaces: allowedNS,
		TopicPrefixes:     rules,
	}
}

func prefixRule(namespaces, prefixes []string) v1alpha1.TopicPrefixRule {
	return v1alpha1.TopicPrefixRule{Namespaces: namespaces, Prefixes: prefixes}
}

func TestCheck(t *testing.T) {
	cases := []struct {
		name      string
		cfg       *v1alpha1.TenancyConfig
		namespace string
		topicName string
		wantOK    bool
		wantMsg   string // substring expected in error when !wantOK
	}{
		// Allow-all cases --------------------------------------------------
		{
			name:      "nil config: allow all",
			cfg:       nil,
			namespace: "team-a",
			topicName: "anything",
			wantOK:    true,
		},
		{
			name:      "empty config struct: allow all",
			cfg:       &v1alpha1.TenancyConfig{},
			namespace: "team-a",
			topicName: "anything",
			wantOK:    true,
		},
		{
			name:      "empty AllowedNamespaces + empty TopicPrefixes: allow all",
			cfg:       makeConfig(nil, nil),
			namespace: "team-b",
			topicName: "some.topic",
			wantOK:    true,
		},
		// Namespace allow-list: exact match --------------------------------
		{
			name:      "exact namespace match: allowed",
			cfg:       makeConfig([]string{"team-a", "team-b"}, nil),
			namespace: "team-a",
			topicName: "foo",
			wantOK:    true,
		},
		{
			name:      "exact namespace non-match: denied",
			cfg:       makeConfig([]string{"team-a"}, nil),
			namespace: "team-c",
			topicName: "foo",
			wantOK:    false,
			wantMsg:   "allowedNamespaces",
		},
		// Namespace allow-list: glob ---------------------------------------
		{
			name:      "glob team-* matches team-a",
			cfg:       makeConfig([]string{"team-*"}, nil),
			namespace: "team-a",
			topicName: "foo",
			wantOK:    true,
		},
		{
			name:      "glob team-* does not match platform",
			cfg:       makeConfig([]string{"team-*"}, nil),
			namespace: "platform",
			topicName: "foo",
			wantOK:    false,
			wantMsg:   "allowedNamespaces",
		},
		{
			name:      "multiple globs: second matches",
			cfg:       makeConfig([]string{"team-*", "platform"}, nil),
			namespace: "platform",
			topicName: "foo",
			wantOK:    true,
		},
		// Prefix allow/deny ------------------------------------------------
		{
			name: "namespace matched by rule, topic starts with allowed prefix",
			cfg: makeConfig(nil, []v1alpha1.TopicPrefixRule{
				prefixRule([]string{"team-a"}, []string{"payments."}),
			}),
			namespace: "team-a",
			topicName: "payments.orders",
			wantOK:    true,
		},
		{
			name: "namespace matched by rule, topic does not start with allowed prefix",
			cfg: makeConfig(nil, []v1alpha1.TopicPrefixRule{
				prefixRule([]string{"team-a"}, []string{"payments."}),
			}),
			namespace: "team-a",
			topicName: "infra.logs",
			wantOK:    false,
			wantMsg:   "allowed prefix",
		},
		{
			name: "namespace NOT matched by any rule: unrestricted",
			cfg: makeConfig(nil, []v1alpha1.TopicPrefixRule{
				prefixRule([]string{"team-a"}, []string{"payments."}),
			}),
			namespace: "team-b", // no rule matches team-b
			topicName: "anything.goes",
			wantOK:    true,
		},
		// Multiple matching rules: union of prefixes (any-prefix-satisfies) -
		{
			name: "two rules match namespace: topic satisfies first rule prefix",
			cfg: makeConfig(nil, []v1alpha1.TopicPrefixRule{
				prefixRule([]string{"team-*"}, []string{"payments."}),
				prefixRule([]string{"team-a"}, []string{"finance."}),
			}),
			namespace: "team-a",
			topicName: "payments.orders",
			wantOK:    true,
		},
		{
			name: "two rules match namespace: topic satisfies second rule prefix",
			cfg: makeConfig(nil, []v1alpha1.TopicPrefixRule{
				prefixRule([]string{"team-*"}, []string{"payments."}),
				prefixRule([]string{"team-a"}, []string{"finance."}),
			}),
			namespace: "team-a",
			topicName: "finance.reports",
			wantOK:    true,
		},
		{
			name: "two rules match namespace: topic satisfies neither prefix",
			cfg: makeConfig(nil, []v1alpha1.TopicPrefixRule{
				prefixRule([]string{"team-*"}, []string{"payments."}),
				prefixRule([]string{"team-a"}, []string{"finance."}),
			}),
			namespace: "team-a",
			topicName: "infra.logs",
			wantOK:    false,
			wantMsg:   "allowed prefix",
		},
		// Combined namespace allow-list + prefix rule ----------------------
		{
			name: "namespace denied by allow-list: no prefix check needed",
			cfg: makeConfig([]string{"team-a"}, []v1alpha1.TopicPrefixRule{
				prefixRule([]string{"team-a"}, []string{"payments."}),
			}),
			namespace: "team-b",
			topicName: "payments.orders",
			wantOK:    false,
			wantMsg:   "allowedNamespaces",
		},
		// Bad glob patterns: non-match, no panic ----------------------------
		{
			name:      "bad glob pattern in AllowedNamespaces: treated as non-match",
			cfg:       makeConfig([]string{"["}, nil),
			namespace: "team-a",
			topicName: "foo",
			wantOK:    false,
			wantMsg:   "allowedNamespaces",
		},
		{
			name: "bad glob pattern in TopicPrefixRule.Namespaces: treated as non-match",
			cfg: makeConfig(nil, []v1alpha1.TopicPrefixRule{
				prefixRule([]string{"["}, []string{"payments."}),
			}),
			namespace: "team-a",
			topicName: "foo",
			wantOK:    true, // bad pattern → rule not matched → unrestricted
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Check(tc.cfg, tc.namespace, tc.topicName)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("Check() = %v, want nil", err)
				}
			} else {
				if err == nil {
					t.Fatalf("Check() = nil, want error containing %q", tc.wantMsg)
				}
				if tc.wantMsg != "" && !strings.Contains(err.Error(), tc.wantMsg) {
					t.Fatalf("Check() error = %q, want substring %q", err.Error(), tc.wantMsg)
				}
			}
		})
	}
}

func TestCheckResource(t *testing.T) {
	cfg := makeConfig(
		[]string{"team-*"},
		[]v1alpha1.TopicPrefixRule{
			prefixRule([]string{"team-a"}, []string{"payments."}),
		},
	)

	cases := []struct {
		name         string
		cfg          *v1alpha1.TenancyConfig
		namespace    string
		resourceType string
		resourceName string
		patternType  string
		wantOK       bool
		wantMsg      string
	}{
		// Namespace allow-list applies to all resource types ---------------
		{
			name:         "namespace denied: topic resource",
			cfg:          cfg,
			namespace:    "platform", // not team-*
			resourceType: "topic",
			resourceName: "payments.orders",
			patternType:  "literal",
			wantOK:       false,
			wantMsg:      "allowedNamespaces",
		},
		{
			name:         "namespace denied: group resource",
			cfg:          cfg,
			namespace:    "platform",
			resourceType: "group",
			resourceName: "my-group",
			patternType:  "literal",
			wantOK:       false,
			wantMsg:      "allowedNamespaces",
		},
		// Prefix rules apply only to topic resources -----------------------
		{
			name:         "topic resource with literal name: prefix checked",
			cfg:          cfg,
			namespace:    "team-a",
			resourceType: "topic",
			resourceName: "payments.orders",
			patternType:  "literal",
			wantOK:       true,
		},
		{
			name:         "prefixed ACL topic resource: name outside allowed prefix -> denied",
			cfg:          cfg,
			namespace:    "team-a",
			resourceType: "topic",
			resourceName: "infra.",
			patternType:  "prefixed",
			wantOK:       false,
			wantMsg:      "allowed prefix",
		},
		{
			name:         "prefixed ACL topic resource: name starts with allowed prefix -> allowed",
			cfg:          cfg,
			namespace:    "team-a",
			resourceType: "topic",
			resourceName: "payments.",
			patternType:  "prefixed",
			wantOK:       true,
		},
		// Group resources reuse topic prefixes -----------------------------
		{
			name:         "group resource outside allowed prefix -> denied",
			cfg:          cfg,
			namespace:    "team-a",
			resourceType: "group",
			resourceName: "infra.consumer-group",
			patternType:  "literal",
			wantOK:       false,
			wantMsg:      "allowed prefix",
		},
		{
			name:         "group resource inside allowed prefix -> allowed",
			cfg:          cfg,
			namespace:    "team-a",
			resourceType: "group",
			resourceName: "payments.consumer-group",
			patternType:  "literal",
			wantOK:       true,
		},
		{
			name:         "group resource in non-prefix-restricted namespace: unrestricted",
			cfg:          cfg,
			namespace:    "team-b", // matches allow-list team-*, no prefix rule
			resourceType: "group",
			resourceName: "anything",
			patternType:  "literal",
			wantOK:       true,
		},
		// Unscopeable resource types: denied for prefix-restricted namespaces
		{
			name:         "cluster resource in prefix-restricted namespace -> denied",
			cfg:          cfg,
			namespace:    "team-a",
			resourceType: "cluster",
			resourceName: "kafka-cluster",
			patternType:  "literal",
			wantOK:       false,
			wantMsg:      `"cluster" resource cannot be scoped`,
		},
		{
			name:         "transactionalId resource in prefix-restricted namespace -> denied",
			cfg:          cfg,
			namespace:    "team-a",
			resourceType: "transactionalId",
			resourceName: "infra.txid",
			patternType:  "literal",
			wantOK:       false,
			wantMsg:      `"transactionalId" resource cannot be scoped`,
		},
		{
			name:         "delegationToken resource in prefix-restricted namespace -> denied",
			cfg:          cfg,
			namespace:    "team-a",
			resourceType: "delegationToken",
			resourceName: "tok",
			patternType:  "literal",
			wantOK:       false,
			wantMsg:      `"delegationToken" resource cannot be scoped`,
		},
		// Unscopeable resource types pass for non-prefix-restricted namespaces
		{
			name:         "cluster resource in non-prefix-restricted namespace: unrestricted",
			cfg:          cfg,
			namespace:    "team-b",
			resourceType: "cluster",
			resourceName: "kafka-cluster",
			patternType:  "literal",
			wantOK:       true,
		},
		{
			name:         "transactionalId resource in non-prefix-restricted namespace: unrestricted",
			cfg:          cfg,
			namespace:    "team-b",
			resourceType: "transactionalId",
			resourceName: "infra.txid",
			patternType:  "literal",
			wantOK:       true,
		},
		// Nil config: allow all --------------------------------------------
		{
			name:         "nil config: allow all resource types",
			cfg:          nil,
			namespace:    "any",
			resourceType: "topic",
			resourceName: "any.topic",
			patternType:  "literal",
			wantOK:       true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckResource(tc.cfg, tc.namespace, tc.resourceType, tc.resourceName, tc.patternType)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("CheckResource() = %v, want nil", err)
				}
			} else {
				if err == nil {
					t.Fatalf("CheckResource() = nil, want error containing %q", tc.wantMsg)
				}
				if tc.wantMsg != "" && !strings.Contains(err.Error(), tc.wantMsg) {
					t.Fatalf("CheckResource() error = %q, want substring %q", err.Error(), tc.wantMsg)
				}
			}
		})
	}
}

func TestCheckNamespace(t *testing.T) {
	cases := []struct {
		name      string
		cfg       *v1alpha1.TenancyConfig
		namespace string
		wantOK    bool
		wantMsg   string
	}{
		{
			name:      "nil config: allow all",
			cfg:       nil,
			namespace: "anywhere",
			wantOK:    true,
		},
		{
			name:      "empty allow-list: allow all",
			cfg:       makeConfig(nil, nil),
			namespace: "anywhere",
			wantOK:    true,
		},
		{
			name:      "namespace in allow-list: allowed",
			cfg:       makeConfig([]string{"team-*"}, nil),
			namespace: "team-a",
			wantOK:    true,
		},
		{
			name:      "namespace outside allow-list: denied",
			cfg:       makeConfig([]string{"team-*"}, nil),
			namespace: "platform",
			wantOK:    false,
			wantMsg:   "allowedNamespaces",
		},
		{
			name: "prefix rules do NOT restrict CheckNamespace (allow-list only)",
			cfg: makeConfig(nil, []v1alpha1.TopicPrefixRule{
				prefixRule([]string{"team-a"}, []string{"payments."}),
			}),
			namespace: "team-a",
			wantOK:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckNamespace(tc.cfg, tc.namespace)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("CheckNamespace() = %v, want nil", err)
				}
			} else {
				if err == nil {
					t.Fatalf("CheckNamespace() = nil, want error containing %q", tc.wantMsg)
				}
				if tc.wantMsg != "" && !strings.Contains(err.Error(), tc.wantMsg) {
					t.Fatalf("CheckNamespace() error = %q, want substring %q", err.Error(), tc.wantMsg)
				}
			}
		})
	}
}

func TestIsPrefixRestricted(t *testing.T) {
	cases := []struct {
		name      string
		cfg       *v1alpha1.TenancyConfig
		namespace string
		want      bool
	}{
		{
			name:      "nil config: not restricted",
			cfg:       nil,
			namespace: "team-a",
			want:      false,
		},
		{
			name:      "no prefix rules: not restricted",
			cfg:       makeConfig([]string{"team-*"}, nil),
			namespace: "team-a",
			want:      false,
		},
		{
			name: "namespace matches a rule glob: restricted",
			cfg: makeConfig(nil, []v1alpha1.TopicPrefixRule{
				prefixRule([]string{"team-*"}, []string{"payments."}),
			}),
			namespace: "team-a",
			want:      true,
		},
		{
			name: "namespace matches no rule glob: not restricted",
			cfg: makeConfig(nil, []v1alpha1.TopicPrefixRule{
				prefixRule([]string{"team-*"}, []string{"payments."}),
			}),
			namespace: "platform",
			want:      false,
		},
		{
			name: "bad glob in rule: treated as non-match",
			cfg: makeConfig(nil, []v1alpha1.TopicPrefixRule{
				prefixRule([]string{"["}, []string{"payments."}),
			}),
			namespace: "team-a",
			want:      false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPrefixRestricted(tc.cfg, tc.namespace); got != tc.want {
				t.Fatalf("IsPrefixRestricted() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCheckRoleBinding(t *testing.T) {
	// team-a is prefix-restricted to "payments."; team-b passes the allow-list
	// but is NOT prefix-restricted; platform fails the allow-list.
	cfg := makeConfig(
		[]string{"team-*"},
		[]v1alpha1.TopicPrefixRule{
			prefixRule([]string{"team-a"}, []string{"payments."}),
		},
	)

	res := func(typ, name, pt string) v1alpha1.RoleResource {
		return v1alpha1.RoleResource{Type: typ, Name: name, PatternType: pt}
	}

	cases := []struct {
		name      string
		cfg       *v1alpha1.TenancyConfig
		namespace string
		resources []v1alpha1.RoleResource
		wantOK    bool
		wantMsg   string
	}{
		// Nil config / allow-list ------------------------------------------
		{
			name:      "nil config: cluster-scoped binding allowed",
			cfg:       nil,
			namespace: "anywhere",
			resources: nil,
			wantOK:    true,
		},
		{
			name:      "namespace outside allow-list: denied regardless of resources",
			cfg:       cfg,
			namespace: "platform",
			resources: []v1alpha1.RoleResource{res("Topic", "payments.orders", "literal")},
			wantOK:    false,
			wantMsg:   "allowedNamespaces",
		},
		// Non-prefix-restricted namespaces: allow-list only ----------------
		{
			name:      "non-restricted namespace: cluster-scoped binding allowed",
			cfg:       cfg,
			namespace: "team-b",
			resources: nil,
			wantOK:    true,
		},
		{
			name:      "non-restricted namespace: Cluster resource allowed",
			cfg:       cfg,
			namespace: "team-b",
			resources: []v1alpha1.RoleResource{res("Cluster", "kafka-cluster", "literal")},
			wantOK:    true,
		},
		// Prefix-restricted namespaces --------------------------------------
		{
			name:      "restricted namespace: cluster-scoped binding denied",
			cfg:       cfg,
			namespace: "team-a",
			resources: nil,
			wantOK:    false,
			wantMsg:   "cluster-scoped role binding",
		},
		{
			name:      "restricted namespace: Topic resource inside prefix allowed",
			cfg:       cfg,
			namespace: "team-a",
			resources: []v1alpha1.RoleResource{res("Topic", "payments.orders", "literal")},
			wantOK:    true,
		},
		{
			name:      "restricted namespace: prefixed-patternType Topic resource inside prefix allowed",
			cfg:       cfg,
			namespace: "team-a",
			resources: []v1alpha1.RoleResource{res("Topic", "payments.", "prefixed")},
			wantOK:    true,
		},
		{
			name:      "restricted namespace: Topic resource outside prefix denied",
			cfg:       cfg,
			namespace: "team-a",
			resources: []v1alpha1.RoleResource{res("Topic", "infra.logs", "literal")},
			wantOK:    false,
			wantMsg:   "allowed prefix",
		},
		{
			name:      "restricted namespace: prefixed-patternType Topic resource outside prefix denied",
			cfg:       cfg,
			namespace: "team-a",
			resources: []v1alpha1.RoleResource{res("Topic", "infra.", "prefixed")},
			wantOK:    false,
			wantMsg:   "allowed prefix",
		},
		{
			name:      "restricted namespace: Group resource inside prefix allowed",
			cfg:       cfg,
			namespace: "team-a",
			resources: []v1alpha1.RoleResource{res("Group", "payments.cg", "literal")},
			wantOK:    true,
		},
		{
			name:      "restricted namespace: Group resource outside prefix denied",
			cfg:       cfg,
			namespace: "team-a",
			resources: []v1alpha1.RoleResource{res("Group", "infra.cg", "literal")},
			wantOK:    false,
			wantMsg:   "allowed prefix",
		},
		{
			name:      "restricted namespace: Cluster resource denied",
			cfg:       cfg,
			namespace: "team-a",
			resources: []v1alpha1.RoleResource{res("Cluster", "kafka-cluster", "literal")},
			wantOK:    false,
			wantMsg:   `type "Cluster"`,
		},
		{
			name:      "restricted namespace: TransactionalId resource denied",
			cfg:       cfg,
			namespace: "team-a",
			resources: []v1alpha1.RoleResource{res("TransactionalId", "payments.tx", "literal")},
			wantOK:    false,
			wantMsg:   `type "TransactionalId"`,
		},
		{
			name:      "restricted namespace: one bad resource among good ones denies the binding",
			cfg:       cfg,
			namespace: "team-a",
			resources: []v1alpha1.RoleResource{
				res("Topic", "payments.orders", "literal"),
				res("Group", "infra.cg", "literal"),
			},
			wantOK:  false,
			wantMsg: "allowed prefix",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckRoleBinding(tc.cfg, tc.namespace, tc.resources)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("CheckRoleBinding() = %v, want nil", err)
				}
			} else {
				if err == nil {
					t.Fatalf("CheckRoleBinding() = nil, want error containing %q", tc.wantMsg)
				}
				if tc.wantMsg != "" && !strings.Contains(err.Error(), tc.wantMsg) {
					t.Fatalf("CheckRoleBinding() error = %q, want substring %q", err.Error(), tc.wantMsg)
				}
			}
		})
	}
}
