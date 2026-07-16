package importer

import (
	"fmt"
	"regexp"
	"strings"
)

// NamespaceStrategy describes how to derive a Kubernetes namespace from a Kafka
// topic name. It supports four kinds; a zero-value strategy is usable and
// behaves as "single" resolving to the fallback (see For).
type NamespaceStrategy struct {
	Kind      string            // "single" | "prefix" | "regex" | "mapping-file"
	Single    string            // for single (the fixed namespace)
	Separator string            // for prefix (default "." if empty)
	Pattern   string            // for regex (must have at least one capture group)
	Mapping   map[string]string // for mapping-file: topicName -> namespace
	Fallback  string            // namespace when a strategy can't resolve (default "default" if empty)
}

// fallback returns the configured fallback namespace, defaulting to "default".
func (s NamespaceStrategy) fallback() string {
	if s.Fallback != "" {
		return s.Fallback
	}
	return "default"
}

// For returns the namespace for a topic name under this strategy.
//
// Semantics by Kind:
//   - "single": s.Single if non-empty, else the fallback.
//   - "prefix": the first segment when topicName contains the separator
//     (default ".") and that first segment is non-empty; otherwise the fallback.
//   - "regex": capture group 1 of s.Pattern applied to topicName when it matches
//     and the group is non-empty; otherwise the fallback. Returns an error if the
//     pattern does not compile or has no capture group.
//   - "mapping-file": s.Mapping[topicName] if present, else the fallback.
//   - "" (empty): treated as "single" so a zero-value strategy is usable.
//   - anything else: an error.
//
// For compiles the regex on each call; AssignNamespaces precompiles once and
// reuses it for loops.
func (s NamespaceStrategy) For(topicName string) (string, error) {
	kind := s.Kind
	if kind == "" {
		kind = "single"
	}
	switch kind {
	case "single":
		if s.Single != "" {
			return s.Single, nil
		}
		return s.fallback(), nil
	case "prefix":
		return s.prefixFor(topicName), nil
	case "regex":
		re, err := s.compileRegex()
		if err != nil {
			return "", err
		}
		return matchRegex(re, topicName, s.fallback()), nil
	case "mapping-file":
		if ns, ok := s.Mapping[topicName]; ok {
			return ns, nil
		}
		return s.fallback(), nil
	default:
		return "", fmt.Errorf("unknown namespace strategy %q", s.Kind)
	}
}

// prefixFor returns the first separator-delimited segment, or the fallback when
// the separator is absent or the first segment is empty.
func (s NamespaceStrategy) prefixFor(topicName string) string {
	sep := s.Separator
	if sep == "" {
		sep = "."
	}
	idx := strings.Index(topicName, sep)
	if idx <= 0 {
		// idx < 0: separator not present. idx == 0: empty first segment.
		return s.fallback()
	}
	return topicName[:idx]
}

// compileRegex compiles s.Pattern and verifies it has at least one capture group.
func (s NamespaceStrategy) compileRegex() (*regexp.Regexp, error) {
	re, err := regexp.Compile(s.Pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid namespace regex %q: %w", s.Pattern, err)
	}
	if re.NumSubexp() < 1 {
		return nil, fmt.Errorf("namespace regex %q must have at least one capture group", s.Pattern)
	}
	return re, nil
}

// matchRegex returns capture group 1 if the pattern matches and the group is
// non-empty; otherwise the fallback.
func matchRegex(re *regexp.Regexp, topicName, fallback string) string {
	m := re.FindStringSubmatch(topicName)
	if len(m) >= 2 && m[1] != "" {
		return m[1]
	}
	return fallback
}

// resolver wraps a strategy with a precompiled regex (when applicable) so it can
// be reused across many topic names without recompiling.
type resolver struct {
	s  NamespaceStrategy
	re *regexp.Regexp // non-nil only for the regex kind
}

func newResolver(s NamespaceStrategy) (resolver, error) {
	r := resolver{s: s}
	kind := s.Kind
	if kind == "" {
		kind = "single"
	}
	switch kind {
	case "single", "prefix", "mapping-file":
		return r, nil
	case "regex":
		re, err := s.compileRegex()
		if err != nil {
			return resolver{}, err
		}
		r.re = re
		return r, nil
	default:
		return resolver{}, fmt.Errorf("unknown namespace strategy %q", s.Kind)
	}
}

// for resolves a topic name using the precompiled state.
func (r resolver) for_(topicName string) string {
	if r.re != nil {
		return matchRegex(r.re, topicName, r.s.fallback())
	}
	// single / prefix / mapping-file never error after newResolver succeeds.
	ns, _ := r.s.For(topicName)
	return ns
}

// AssignNamespaces sets ObjectMeta.Namespace on every object in res: topics via
// the strategy, policies following their governed topic(s), schema files
// following their owning topic (via MetaName), and quotas + role bindings + users
// (which are not topic-scoped) receiving the fallback namespace.
//
// Topics: namespace = strategy applied to Spec.TopicName (falling back to
// Metadata.Name when TopicName is empty).
//
// Policies: a policy's namespace follows the topic(s) it governs. We collect the
// distinct namespaces of the topic-type rule resources that name an imported
// topic. If the policy references exactly ONE such namespace, that namespace is
// used; otherwise (zero imported-topic references, or references spanning
// multiple namespaces) the fallback is used. This keeps a policy in the same
// namespace as its single governed topic and avoids guessing when ambiguous.
func AssignNamespaces(res *Result, s NamespaceStrategy) error {
	if res == nil {
		return nil
	}
	r, err := newResolver(s)
	if err != nil {
		return err
	}

	// Assign topic namespaces and build topicName -> namespace lookup.
	nsByTopic := make(map[string]string, len(res.Topics))
	for _, tp := range res.Topics {
		key := tp.ResolvedTopicName()
		ns := r.for_(key)
		tp.Namespace = ns
		if key != "" {
			nsByTopic[key] = ns
		}
	}

	fallback := s.fallback()
	for _, pol := range res.Policies {
		seen := map[string]bool{}
		for _, rule := range pol.Spec.Rules {
			if rule.Resource.Type != "topic" {
				continue
			}
			if ns, ok := nsByTopic[rule.Resource.Name]; ok {
				seen[ns] = true
			}
		}
		if len(seen) == 1 {
			for ns := range seen {
				pol.Namespace = ns
			}
		} else {
			pol.Namespace = fallback
		}
	}

	// Stamp each schema file with its owning topic's namespace, looked up by the
	// explicit MetaName link (set at construction in applySchemas).
	nsByMeta := make(map[string]string, len(res.Topics))
	for _, tp := range res.Topics {
		nsByMeta[tp.Name] = tp.Namespace
	}
	for i := range res.SchemaFiles {
		if ns, ok := nsByMeta[res.SchemaFiles[i].MetaName]; ok {
			res.SchemaFiles[i].Namespace = ns
		} else {
			res.SchemaFiles[i].Namespace = fallback
		}
	}

	// Quotas are not topic-scoped; assign the fallback namespace.
	for _, q := range res.Quotas {
		q.Namespace = fallback
	}

	// Users are not topic-scoped (a SCRAM credential is a cluster-wide
	// principal, not owned by any single topic); assign the fallback
	// namespace, like quotas and role bindings.
	for _, u := range res.Users {
		u.Namespace = fallback
	}

	// Role bindings are not topic-scoped (cluster-scoped roles, ResourceOwner,
	// cross-scope grants); assign the fallback namespace like quotas.
	for _, rb := range res.RoleBindings {
		rb.Namespace = fallback
	}

	return nil
}
