package rbac

import (
	"fmt"
	"sort"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// Topic-access auto-map role assignments (spec §40):
//   - producer            → DeveloperWrite on the topic
//   - consumer (topic)    → DeveloperRead  on the topic
//   - consumer (group)    → DeveloperRead  on the consumer group
//
// The default ACL operation bundles these roles stand in for (used to detect a
// custom — and therefore coarsened — operation list). They mirror
// access.CompileTopic's orDefault bundles exactly.
var (
	producerOpBundle      = []string{"Write", "Describe"}
	consumerTopicOpBundle = []string{"Read", "Describe"}
	consumerGroupOpBundle = []string{"Read"}
)

// CompileTopicAccess maps a KafkaTopic.spec.access block to MDS role bindings
// (spec §40), the RBAC analogue of access.CompileTopic. It is used only on
// clusters whose authorization.accessBackends includes "rbac".
//
// Every binding is kafka-scoped and carries SourceKind="KafkaTopic" with the
// topic's namespace/name, so it is attributed to the topic (and is treated as a
// topic-access-derived binding — not an explicit KafkaRoleBinding — by
// BuildDesiredSet's cross-source dedup rule).
//
// RBAC has no host concept and its roles are coarse bundles, so an entry with a
// non-"*" host or a custom operation list that differs from the role's default
// bundle is still mapped (host + operation refinement dropped) but produces a
// human-readable warning. Returns an error only when the MDS scope cannot be
// resolved (missing kafka-cluster id), mirroring Compile.
func CompileTopicAccess(tp *v1alpha1.KafkaTopic, mds *v1alpha1.MDSConfig) ([]RoleBinding, []string, error) {
	scope, err := resolveScope("kafka", mds.Clusters)
	if err != nil {
		return nil, nil, err
	}

	topicName := tp.ResolvedTopicName()

	var out []RoleBinding
	var warns []string

	mk := func(principal, role, resType, resName string) RoleBinding {
		return RoleBinding{
			Principal:       principal,
			Role:            role,
			Scope:           scope,
			Resource:        &ResourcePattern{Type: resType, Name: resName, PatternType: "literal"},
			SourceKind:      "KafkaTopic",
			SourceNamespace: tp.Namespace,
			SourceName:      tp.Name,
		}
	}

	for _, p := range tp.Spec.Access.Producers {
		if w := hostCoarsenWarning(tp, p.Principal, "producer", p.Host); w != "" {
			warns = append(warns, w)
		}
		if w := opsCoarsenWarning(tp, p.Principal, "producer", p.Operations, producerOpBundle); w != "" {
			warns = append(warns, w)
		}
		out = append(out, mk(p.Principal, "DeveloperWrite", "Topic", topicName))
	}

	for _, c := range tp.Spec.Access.Consumers {
		// host applies to the whole consumer entry — warn at most once.
		if w := hostCoarsenWarning(tp, c.Principal, "consumer", c.Host); w != "" {
			warns = append(warns, w)
		}
		out = append(out, mk(c.Principal, "DeveloperRead", "Topic", topicName))
		if w := opsCoarsenWarning(tp, c.Principal, "consumer topic", c.TopicOperations, consumerTopicOpBundle); w != "" {
			warns = append(warns, w)
		}
		out = append(out, mk(c.Principal, "DeveloperRead", "Group", c.Group))
		if w := opsCoarsenWarning(tp, c.Principal, "consumer group", c.GroupOperations, consumerGroupOpBundle); w != "" {
			warns = append(warns, w)
		}
	}

	return out, warns, nil
}

// hostCoarsenWarning returns a non-empty message when an access entry's host
// cannot be represented in RBAC (non-"*"). "" means no loss.
func hostCoarsenWarning(tp *v1alpha1.KafkaTopic, principal, context, host string) string {
	if host == "" || host == "*" {
		return ""
	}
	return fmt.Sprintf("KafkaTopic %s/%s: %s access for %s mapped to a coarser RBAC role (dropped host %q); RBAC cannot represent it",
		tp.Namespace, tp.Name, context, principal, host)
}

// opsCoarsenWarning returns a non-empty message when an access entry's operation
// list differs from the role's default bundle (which RBAC cannot honor). "" means no loss.
func opsCoarsenWarning(tp *v1alpha1.KafkaTopic, principal, context string, ops, bundle []string) string {
	if len(ops) == 0 || sameStringSet(ops, bundle) {
		return ""
	}
	return fmt.Sprintf("KafkaTopic %s/%s: %s access for %s mapped to a coarser RBAC role (dropped custom operations %v); RBAC cannot represent it",
		tp.Namespace, tp.Name, context, principal, ops)
}

// sameStringSet reports whether a and b contain the same elements (set
// equality, order- and duplicate-insensitive).
func sameStringSet(a, b []string) bool {
	as := map[string]struct{}{}
	for _, x := range a {
		as[x] = struct{}{}
	}
	bs := map[string]struct{}{}
	for _, x := range b {
		bs[x] = struct{}{}
	}
	if len(as) != len(bs) {
		return false
	}
	for x := range as {
		if _, ok := bs[x]; !ok {
			return false
		}
	}
	return true
}

// SortedWarnings returns warnings in a deterministic (sorted) order so callers
// surfacing them in logs / status conditions produce reproducible output.
func SortedWarnings(warns []string) []string {
	out := append([]string(nil), warns...)
	sort.Strings(out)
	return out
}
