// Package tenancy enforces the cluster-owner tenancy policy defined in
// KafkaCluster.spec.tenancy (spec §20.2). The operator calls the Check*
// functions before any live-state mutation; the CLI does NOT enforce tenancy
// (it runs as the cluster admin; tenancy is a multi-team operator-mode control
// only).
//
// # Enforcement model
//
// The namespace allow-list (AllowedNamespaces, when non-empty) applies to ALL
// data-plane kinds: KafkaTopic, KafkaAccessPolicy, KafkaQuota, KafkaRoleBinding
// and KafkaUser. A nil TenancyConfig disables every check.
//
// A namespace is PREFIX-RESTRICTED iff it matches at least one
// TopicPrefixRule.Namespaces glob (see IsPrefixRestricted). Prefix-restricted
// namespaces are additionally constrained to resources whose names carry one
// of the namespace's allowed prefixes:
//
//   - topic names (KafkaTopic.topicName, policy topic rules, role-binding
//     Topic resources) must start with an allowed prefix;
//   - GROUP names reuse the same topic prefixes by design (policy group
//     rules, role-binding Group resources, consumer group names in a
//     KafkaTopic access block): a team that owns the "payments." prefix owns
//     "payments."-prefixed consumer groups too;
//   - resources that CANNOT be scoped by a name prefix are denied outright:
//     policy rules on cluster / transactionalId / delegationToken resources,
//     and cluster-scoped role bindings (empty spec.resources) or role-binding
//     resources of any type other than Topic/Group. Allowing them would let a
//     tenant escalate past its prefix (e.g. Alter on the cluster resource
//     grants the power to create arbitrary ACLs).
//
// Namespaces that are NOT prefix-restricted are only subject to the
// allow-list; their non-topic policy rules and role-binding resources pass
// unchecked (the cluster owner opted them out of prefix scoping).
//
// # Multi-rule prefix semantics
//
// When a namespace matches multiple TopicPrefixRules, the allowed-prefix set is
// the UNION of all matching rules' Prefixes: the topicName (or resource name)
// must start with at least one prefix collected from all matching rules.
// "Satisfying any rule" and "union of all rules" are equivalent when a match
// requires a single prefix, and the union interpretation is the simplest
// defensible semantic — it extends a namespace's permissions when multiple
// rules overlap, which is the least-surprise behaviour for cluster owners who
// compose rules additively (each rule grants an additional allowed prefix set).
package tenancy

import (
	"fmt"
	"path"
	"strings"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// Check enforces the namespace allow-list and topic-prefix rules for a
// KafkaTopic reconcile. topicName must be the RESOLVED topic name (after
// defaulting: spec.topicName if set, metadata.name otherwise).
//
// Returns nil if:
//   - t is nil or its AllowedNamespaces and TopicPrefixes are both empty, or
//   - the namespace passes the allow-list AND the topicName passes any
//     applicable prefix rules.
//
// Returns a non-nil error with a message naming the namespace/topic and the
// failed constraint. These errors end up in CR conditions.
func Check(t *v1alpha1.TenancyConfig, namespace, topicName string) error {
	if t == nil {
		return nil
	}
	if err := checkNamespace(t, namespace); err != nil {
		return err
	}
	return checkPrefix(t, namespace, "topic", topicName)
}

// CheckNamespace enforces ONLY the namespace allow-list. It is the tenancy
// gate for kinds whose spec carries no name that prefix rules could scope —
// today KafkaQuota (a quota targets a principal/client-id entity, so
// prefix-restricted namespaces get no additional entity-level check; this is a
// documented limitation).
func CheckNamespace(t *v1alpha1.TenancyConfig, namespace string) error {
	if t == nil {
		return nil
	}
	return checkNamespace(t, namespace)
}

// IsPrefixRestricted reports whether namespace matches at least one
// TopicPrefixRule.Namespaces glob, i.e. whether the cluster owner scoped this
// namespace to a set of name prefixes. Nil config → false.
func IsPrefixRestricted(t *v1alpha1.TenancyConfig, namespace string) bool {
	if t == nil {
		return false
	}
	return allowedPrefixes(t, namespace) != nil
}

// allowedPrefixes returns the union of Prefixes from every TopicPrefixRule
// whose Namespaces globs match namespace. A non-nil empty slice is never
// returned: nil means "no rule matches" (namespace is not prefix-restricted).
func allowedPrefixes(t *v1alpha1.TenancyConfig, namespace string) []string {
	var allowed []string
	for _, rule := range t.TopicPrefixes {
		if namespaceMatchesRule(rule.Namespaces, namespace) {
			allowed = append(allowed, rule.Prefixes...)
		}
	}
	return allowed
}

// CheckResource enforces tenancy for a KafkaAccessPolicy rule resource (and
// for a KafkaTopic access-block consumer group, via resourceType "group").
// The namespace allow-list is checked the same as in Check. Then:
//
//   - resourceType "topic": the name is prefix-checked (both literal and
//     prefixed pattern types — the prefix check on the resource name is
//     identical for both, which is why patternType is accepted only for
//     call-site symmetry with ACLResource and otherwise unused);
//   - resourceType "group": the name is prefix-checked against the SAME
//     TopicPrefixes (groups reuse topic prefixes by design);
//   - any other resourceType (cluster, transactionalId, delegationToken):
//     DENIED when the namespace is prefix-restricted — these resources cannot
//     be scoped by a name prefix, and allowing them would let a tenant
//     escalate past its prefix (e.g. Alter on the cluster resource grants the
//     power to create arbitrary ACLs). Non-prefix-restricted namespaces pass
//     unchecked.
func CheckResource(t *v1alpha1.TenancyConfig, namespace, resourceType, resourceName, _ string) error {
	if t == nil {
		return nil
	}
	if err := checkNamespace(t, namespace); err != nil {
		return err
	}
	switch resourceType {
	case "topic":
		return checkPrefix(t, namespace, "topic", resourceName)
	case "group":
		return checkPrefix(t, namespace, "group", resourceName)
	default:
		// cluster, transactionalId, delegationToken (and anything future):
		// unscopeable by prefix — fail closed for prefix-restricted namespaces.
		if allowed := allowedPrefixes(t, namespace); allowed != nil {
			return fmt.Errorf("tenancy: namespace %q is restricted to name prefixes, but a %q resource cannot be scoped by a name prefix and is denied (only topic and group resources are allowed; allowed prefixes %v)",
				namespace, resourceType, allowed)
		}
		return nil
	}
}

// CheckRoleBinding enforces tenancy for a KafkaRoleBinding reconcile. The
// namespace allow-list is checked the same as in Check. For PREFIX-RESTRICTED
// namespaces the binding is additionally constrained:
//
//   - a cluster-scoped binding (empty resources — e.g. SystemAdmin) is
//     DENIED: it cannot be scoped by a name prefix;
//   - every resource entry must be of type Topic or Group with a name that
//     starts with one of the namespace's allowed prefixes (both literal and
//     prefixed patternTypes are prefix-checked the same way, like topics);
//   - a resource of any other type (Cluster, TransactionalId, Subject,
//     Connector, KsqlCluster, ...) is DENIED.
//
// Namespaces that are not prefix-restricted pass with the allow-list alone.
func CheckRoleBinding(t *v1alpha1.TenancyConfig, namespace string, resources []v1alpha1.RoleResource) error {
	if t == nil {
		return nil
	}
	if err := checkNamespace(t, namespace); err != nil {
		return err
	}
	allowed := allowedPrefixes(t, namespace)
	if allowed == nil {
		return nil
	}
	if len(resources) == 0 {
		return fmt.Errorf("tenancy: namespace %q is restricted to name prefixes, but a cluster-scoped role binding (no spec.resources) cannot be scoped by a name prefix and is denied (allowed prefixes %v)",
			namespace, allowed)
	}
	for i, res := range resources {
		switch res.Type {
		case "Topic":
			// The checkPrefixAllowed error already names the kind, name,
			// namespace and allowed prefixes; no extra wrapping needed.
			if err := checkPrefixAllowed(allowed, namespace, "topic", res.Name); err != nil {
				return err
			}
		case "Group":
			if err := checkPrefixAllowed(allowed, namespace, "group", res.Name); err != nil {
				return err
			}
		default:
			return fmt.Errorf("tenancy: namespace %q is restricted to name prefixes, but role-binding resource %d has type %q which cannot be scoped by a name prefix and is denied (only Topic and Group resources are allowed; allowed prefixes %v)",
				namespace, i, res.Type, allowed)
		}
	}
	return nil
}

// checkNamespace enforces the AllowedNamespaces list. Empty list = allow all.
func checkNamespace(t *v1alpha1.TenancyConfig, namespace string) error {
	if len(t.AllowedNamespaces) == 0 {
		return nil
	}
	for _, pattern := range t.AllowedNamespaces {
		// A pattern-compile error is treated as non-match; validation (which runs
		// before tenancy enforcement) catches bad glob patterns and rejects them
		// with a clear error, so reaching here with a bad pattern is abnormal.
		matched, err := path.Match(pattern, namespace)
		if err != nil {
			continue // bad pattern: non-match, no panic
		}
		if matched {
			return nil
		}
	}
	return fmt.Errorf("tenancy: namespace %q is not in the cluster's allowedNamespaces %v", namespace, t.AllowedNamespaces)
}

// checkPrefix enforces the TopicPrefixes rules on a name of the given kind
// ("topic" or "group" — groups reuse topic prefixes by design). A namespace
// matched by no rule is unrestricted. A namespace matched by one or more rules
// must have a name that starts with at least one prefix collected from ALL
// matching rules (union of prefixes across all matching rules).
func checkPrefix(t *v1alpha1.TenancyConfig, namespace, kind, name string) error {
	if len(t.TopicPrefixes) == 0 {
		return nil
	}
	return checkPrefixAllowed(allowedPrefixes(t, namespace), namespace, kind, name)
}

// checkPrefixAllowed enforces name against a precomputed allowed-prefix list
// (see allowedPrefixes). A nil allowed means no rule matched the namespace,
// i.e. unrestricted. Callers that need to check multiple names against the
// same namespace (e.g. CheckRoleBinding's resource loop) should compute the
// list once via allowedPrefixes and call this directly instead of
// re-deriving it per name via checkPrefix.
func checkPrefixAllowed(allowed []string, namespace, kind, name string) error {
	if len(allowed) == 0 {
		// No rule matched this namespace: unrestricted.
		return nil
	}

	for _, prefix := range allowed {
		if strings.HasPrefix(name, prefix) {
			return nil
		}
	}

	return fmt.Errorf("tenancy: %s %q in namespace %q does not start with any allowed prefix %v",
		kind, name, namespace, allowed)
}

// namespaceMatchesRule reports whether namespace matches at least one glob in
// patterns. A pattern-compile error is treated as non-match (see note in
// checkNamespace).
func namespaceMatchesRule(patterns []string, namespace string) bool {
	for _, pattern := range patterns {
		matched, err := path.Match(pattern, namespace)
		if err != nil {
			continue // bad pattern: non-match, no panic
		}
		if matched {
			return true
		}
	}
	return false
}
