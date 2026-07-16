package validation

import (
	"fmt"
	"net"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/quota"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
	"github.com/monedula-dev/monedula-gitops/internal/user"
)

type Input struct {
	Topics       []*v1alpha1.KafkaTopic
	Policies     []*v1alpha1.KafkaAccessPolicy
	Clusters     map[string]*v1alpha1.KafkaCluster // nil => skip cluster-ref/identity checks
	Quotas       []*v1alpha1.KafkaQuota
	RoleBindings []*v1alpha1.KafkaRoleBinding // shape, clusterRef, MDS-required, and identity checks run in Validate
	Users        []*v1alpha1.KafkaUser        // shape and (clusterRef, username) identity checks run in Validate
}

type Error struct{ Msg string }

func (e Error) Error() string { return e.Msg }

func errf(format string, a ...any) Error { return Error{Msg: fmt.Sprintf(format, a...)} }

var validModes = map[string]bool{"Enforce": true, "DetectOnly": true, "ObserveOnly": true}

// validIgnoreField is the spec §16 drift.ignoreFields entry syntax: the two
// scalar field names, or "config." followed by a non-empty config key (keys
// contain dots, e.g. "config.retention.ms"). Anything else — typos like
// "configs.retention.ms" or a bare "retention.ms" — is rejected up-front so it
// cannot silently fail to suppress drift.
var validIgnoreField = regexp.MustCompile(`^(partitions|replicationFactor|config\..+)$`)
var validResourceTypes = map[string]bool{"topic": true, "group": true, "cluster": true, "transactionalId": true, "delegationToken": true}
var validPatternTypes = map[string]bool{"literal": true, "prefixed": true}
var validPermissions = map[string]bool{"Allow": true, "Deny": true}
var validOps = map[string]bool{"Read": true, "Write": true, "Create": true, "Delete": true, "Alter": true, "Describe": true, "ClusterAction": true, "DescribeConfigs": true, "AlterConfigs": true, "IdempotentWrite": true, "All": true}
var validSchemaFormats = map[string]bool{"AVRO": true, "JSON": true, "PROTOBUF": true}
var validSubjectStrategies = map[string]bool{"TopicName": true, "RecordName": true, "TopicRecordName": true, "Custom": true}
var validCompatibilities = map[string]bool{"NONE": true, "BACKWARD": true, "BACKWARD_TRANSITIVE": true, "FORWARD": true, "FORWARD_TRANSITIVE": true, "FULL": true, "FULL_TRANSITIVE": true}
var validUserMechanisms = map[string]bool{"SCRAM-SHA-256": true, "SCRAM-SHA-512": true}

// userUsernameForbidden matches whitespace, control characters, NUL, '=', and
// ',' — the characters that are sensitive in SASL/JAAS config strings and ACL
// principal serialization. Kafka principals are otherwise permissive, so
// spec.username is deliberately NOT charset-restricted beyond this: anything
// that would corrupt a config line or a "User:<name>" ACL principal is
// rejected; everything else (dots, colons, unicode, etc.) is allowed.
var userUsernameForbidden = regexp.MustCompile(`[\x00-\x20\x7f,=]`)

// canonicalOps maps the lowercase form of every valid ACL operation to its
// canonical (exact-case) spelling, so error messages for case variants like
// "WRITE" can point at "Write". Case matters: the broker parses operations
// case-insensitively on create, but live state is reported canonicalized, so a
// non-canonical desired op never matches live state and would be re-created
// forever.
var canonicalOps = func() map[string]string {
	m := make(map[string]string, len(validOps))
	for op := range validOps {
		m[strings.ToLower(op)] = op
	}
	return m
}()

// opError builds the invalid-operation error, suggesting the canonical form
// when the op is a case variant of a valid operation.
func opError(prefix, op string) Error {
	if canon, ok := canonicalOps[strings.ToLower(op)]; ok {
		return errf("%s: invalid operation %q (operations are case-sensitive; use %q)", prefix, op, canon)
	}
	return errf("%s: invalid operation %q", prefix, op)
}

// placementConstraintsKey is the Confluent topic config that drives replica
// placement. It is mutually exclusive with an explicit replication factor.
const placementConstraintsKey = "confluent.placement.constraints"

// ValidateAccessPolicyShape checks the per-resource shape of a KafkaAccessPolicy
// (spec §20.3). It does NOT check clusterRef resolution or cross-policy ACL
// conflicts — those require cross-resource context and are enforced by the
// top-level Validate function. This function is exported so the admission
// webhook can reuse it without pulling in cross-resource dependencies.
func ValidateAccessPolicyShape(pol *v1alpha1.KafkaAccessPolicy) []error {
	var errs []error
	name := pol.Name

	if pol.APIVersion != v1alpha1.APIVersion {
		errs = append(errs, errf("KafkaAccessPolicy %s: apiVersion must be %s", name, v1alpha1.APIVersion))
	}
	if len(pol.Spec.Rules) == 0 {
		errs = append(errs, errf("KafkaAccessPolicy %s: spec.rules must not be empty", name))
	}
	if pol.Spec.DeletionPolicy != "" && pol.Spec.DeletionPolicy != "Orphan" && pol.Spec.DeletionPolicy != "Delete" {
		errs = append(errs, errf("KafkaAccessPolicy %s: invalid deletionPolicy %q", name, pol.Spec.DeletionPolicy))
	}
	if pol.Spec.Reconciliation != nil && pol.Spec.Reconciliation.Mode != "" && !validModes[pol.Spec.Reconciliation.Mode] {
		errs = append(errs, errf("KafkaAccessPolicy %s: invalid reconciliation.mode %q", name, pol.Spec.Reconciliation.Mode))
	}
	for i, r := range pol.Spec.Rules {
		if r.Principal == "" {
			errs = append(errs, errf("KafkaAccessPolicy %s rule %d: principal required", name, i))
		}
		if r.Resource.Type == "" || !validResourceTypes[r.Resource.Type] {
			errs = append(errs, errf("KafkaAccessPolicy %s rule %d: invalid resource type %q", name, i, r.Resource.Type))
		}
		if r.Resource.Name == "" {
			errs = append(errs, errf("KafkaAccessPolicy %s rule %d: resource name required", name, i))
		}
		if r.Resource.PatternType != "" && !validPatternTypes[r.Resource.PatternType] {
			errs = append(errs, errf("KafkaAccessPolicy %s rule %d: invalid patternType %q", name, i, r.Resource.PatternType))
		}
		if r.Permission != "" && !validPermissions[r.Permission] {
			errs = append(errs, errf("KafkaAccessPolicy %s rule %d: invalid permission %q", name, i, r.Permission))
		}
		if len(r.Operations) == 0 {
			errs = append(errs, errf("KafkaAccessPolicy %s rule %d: at least one operation required", name, i))
		}
		for _, op := range r.Operations {
			if !validOps[op] {
				errs = append(errs, opError(fmt.Sprintf("KafkaAccessPolicy %s rule %d", name, i), op))
			}
		}
	}
	return errs
}

// ValidateQuotaShape checks the per-resource entity-shape and limits rules for
// a KafkaQuota (spec §39.5). It does NOT check clusterRef resolution or
// identity uniqueness — those require cross-resource context and are enforced
// by the top-level Validate function. This function is exported so the
// admission webhook (Task 9) can reuse it without pulling in cross-resource
// dependencies.
func ValidateQuotaShape(q *v1alpha1.KafkaQuota) []error {
	var errs []error
	name := q.Name
	e := q.Spec.Entity

	// At least one entity component required.
	hasUser := e.User != "" || e.UserDefault
	hasClientId := e.ClientId != "" || e.ClientIdDefault
	hasIp := e.Ip != "" || e.IpDefault
	if !hasUser && !hasClientId && !hasIp {
		errs = append(errs, errf("KafkaQuota %s: spec.entity must have at least one of user, clientId, userDefault, clientIdDefault, ip, ipDefault", name))
	}

	// ip is a separate dimension: never combined with user/client-id.
	if hasIp && (hasUser || hasClientId) {
		errs = append(errs, errf("KafkaQuota %s: spec.entity.ip/ipDefault is a separate quota dimension and cannot be combined with user/clientId components", name))
	}
	// ip and ipDefault are mutually exclusive.
	if e.Ip != "" && e.IpDefault {
		errs = append(errs, errf("KafkaQuota %s: spec.entity.ip and spec.entity.ipDefault are mutually exclusive", name))
	}
	// ip must be a valid IPv4/IPv6 literal.
	if e.Ip != "" && net.ParseIP(e.Ip) == nil {
		errs = append(errs, errf("KafkaQuota %s: spec.entity.ip must be a valid IPv4 or IPv6 address, got %q", name, e.Ip))
	}

	// user and userDefault are mutually exclusive.
	if e.User != "" && e.UserDefault {
		errs = append(errs, errf("KafkaQuota %s: spec.entity.user and spec.entity.userDefault are mutually exclusive", name))
	}

	// clientId and clientIdDefault are mutually exclusive.
	if e.ClientId != "" && e.ClientIdDefault {
		errs = append(errs, errf("KafkaQuota %s: spec.entity.clientId and spec.entity.clientIdDefault are mutually exclusive", name))
	}

	// user must be in "User:<name>" form with a non-empty name after the prefix.
	if e.User != "" {
		if !strings.HasPrefix(e.User, "User:") {
			errs = append(errs, errf(`KafkaQuota %s: spec.entity.user must be in "User:" form (e.g. "User:alice"), got %q`, name, e.User))
		} else if strings.TrimPrefix(e.User, "User:") == "" {
			errs = append(errs, errf(`KafkaQuota %s: spec.entity.user has "User:" prefix but requires a non-empty name after it`, name))
		}
	}

	// clientId must be non-blank when set.
	if e.ClientId != "" && strings.TrimSpace(e.ClientId) == "" {
		errs = append(errs, errf("KafkaQuota %s: spec.entity.clientId must not be blank (whitespace-only)", name))
	}

	// At least one limit must be set.
	l := q.Spec.Limits
	if l.ProducerByteRate == nil && l.ConsumerByteRate == nil && l.RequestPercentage == nil &&
		l.ControllerMutationRate == nil && l.ConnectionCreationRate == nil {
		errs = append(errs, errf("KafkaQuota %s: spec.limits must have at least one of producerByteRate, consumerByteRate, requestPercentage, controllerMutationRate, connectionCreationRate", name))
	}

	// Each set limit must be >= 0.
	if l.ProducerByteRate != nil && *l.ProducerByteRate < 0 {
		errs = append(errs, errf("KafkaQuota %s: spec.limits.producerByteRate must be >= 0", name))
	}
	if l.ConsumerByteRate != nil && *l.ConsumerByteRate < 0 {
		errs = append(errs, errf("KafkaQuota %s: spec.limits.consumerByteRate must be >= 0", name))
	}
	if l.RequestPercentage != nil && *l.RequestPercentage < 0 {
		errs = append(errs, errf("KafkaQuota %s: spec.limits.requestPercentage must be >= 0", name))
	}
	if l.ControllerMutationRate != nil && *l.ControllerMutationRate < 0 {
		errs = append(errs, errf("KafkaQuota %s: spec.limits.controllerMutationRate must be >= 0", name))
	}
	if l.ConnectionCreationRate != nil && *l.ConnectionCreationRate < 0 {
		errs = append(errs, errf("KafkaQuota %s: spec.limits.connectionCreationRate must be >= 0", name))
	}

	// Limit partitioning (Kafka model): connectionCreationRate is valid ONLY on
	// an ip entity; the four throughput limits are invalid on an ip entity.
	throughputSet := l.ProducerByteRate != nil || l.ConsumerByteRate != nil ||
		l.RequestPercentage != nil || l.ControllerMutationRate != nil
	if hasIp {
		if throughputSet {
			errs = append(errs, errf("KafkaQuota %s: an ip entity may set only connectionCreationRate (producerByteRate/consumerByteRate/requestPercentage/controllerMutationRate are not valid on ip)", name))
		}
	} else if l.ConnectionCreationRate != nil {
		errs = append(errs, errf("KafkaQuota %s: spec.limits.connectionCreationRate is valid only on an ip entity", name))
	}

	return errs
}

// rbResourceTypesByScopeType maps each scope type to its valid resource types.
// kafka: Topic, Group, Cluster, TransactionalId
// schema-registry: Subject, Cluster
// connect: Connector, Cluster
// ksql: KsqlCluster, Cluster
var rbResourceTypesByScopeType = map[string]map[string]bool{
	"kafka": {
		"Topic":           true,
		"Group":           true,
		"Cluster":         true,
		"TransactionalId": true,
	},
	"schema-registry": {
		"Subject": true,
		"Cluster": true,
	},
	"connect": {
		"Connector": true,
		"Cluster":   true,
	},
	"ksql": {
		"KsqlCluster": true,
		"Cluster":     true,
	},
}

// ValidateClusterAuthorization checks a KafkaCluster's authorization config:
// every accessBackends entry must be "acl" or "rbac", and "rbac" requires
// authorization.mds to be configured (spec §40). A nil cluster or nil
// authorization yields no errors (the default is ACL-only).
func ValidateClusterAuthorization(c *v1alpha1.KafkaCluster) []error {
	if c == nil || c.Spec.Authorization == nil {
		return nil
	}
	var errs []error
	for _, b := range c.Spec.Authorization.AccessBackends {
		if b != "acl" && b != "rbac" {
			errs = append(errs, errf(
				"KafkaCluster %s: authorization.accessBackends entry %q is invalid (must be \"acl\" or \"rbac\")", c.Name, b))
		}
	}
	if v1alpha1.HasAccessBackend(c, "rbac") && c.Spec.Authorization.MDS == nil {
		errs = append(errs, errf(
			"KafkaCluster %s: authorization.accessBackends includes \"rbac\" but authorization.mds is not configured", c.Name))
	}
	return errs
}

// ValidateRoleBindingShape checks the per-resource shape of a KafkaRoleBinding
// (spec §40). It does NOT check clusterRef resolution, MDS configuration, or
// identity uniqueness — those require cross-resource context and are enforced by
// the top-level Validate function. This function is exported so the admission
// webhook can reuse it without pulling in cross-resource dependencies.
//
// Unknown roles (rbac.RoleUnknown) are treated as a WARNING, not an error.
// Since this codebase has no warnings return channel, unknown-role emits no
// error and skips resource-presence enforcement while still validating the shape
// of any resources that ARE present (spec §40 decision 18: future-proof).
func ValidateRoleBindingShape(rb *v1alpha1.KafkaRoleBinding) []error {
	var errs []error
	name := rb.Name

	if rb.APIVersion != v1alpha1.APIVersion {
		errs = append(errs, errf("KafkaRoleBinding %s: apiVersion must be %s", name, v1alpha1.APIVersion))
	}

	// principal: non-empty, in "User:<name>" or "Group:<name>" form with non-empty name.
	switch {
	case rb.Spec.Principal == "":
		errs = append(errs, errf("KafkaRoleBinding %s: spec.principal must be non-empty", name))
	case strings.HasPrefix(rb.Spec.Principal, "User:"):
		if strings.TrimPrefix(rb.Spec.Principal, "User:") == "" {
			errs = append(errs, errf(`KafkaRoleBinding %s: spec.principal has "User:" prefix but requires a non-empty name after it`, name))
		}
	case strings.HasPrefix(rb.Spec.Principal, "Group:"):
		if strings.TrimPrefix(rb.Spec.Principal, "Group:") == "" {
			errs = append(errs, errf(`KafkaRoleBinding %s: spec.principal has "Group:" prefix but requires a non-empty name after it`, name))
		}
	default:
		errs = append(errs, errf(`KafkaRoleBinding %s: spec.principal must be in "User:<name>" or "Group:<name>" form, got %q`, name, rb.Spec.Principal))
	}

	if rb.Spec.Role == "" {
		errs = append(errs, errf("KafkaRoleBinding %s: spec.role must be non-empty", name))
	}

	if _, ok := rbResourceTypesByScopeType[rb.Spec.Scope.Type]; !ok {
		errs = append(errs, errf("KafkaRoleBinding %s: invalid scope.type %q (must be kafka, schema-registry, connect, or ksql)", name, rb.Spec.Scope.Type))
	}

	if rb.Spec.DeletionPolicy != "" && rb.Spec.DeletionPolicy != "Orphan" && rb.Spec.DeletionPolicy != "Delete" {
		errs = append(errs, errf("KafkaRoleBinding %s: invalid deletionPolicy %q", name, rb.Spec.DeletionPolicy))
	}
	if rb.Spec.Reconciliation != nil && rb.Spec.Reconciliation.Mode != "" && !validModes[rb.Spec.Reconciliation.Mode] {
		errs = append(errs, errf("KafkaRoleBinding %s: invalid reconciliation.mode %q", name, rb.Spec.Reconciliation.Mode))
	}

	// Role classification: cluster-scoped roles must have no resources;
	// resource-scoped roles must have at least one resource.
	// Unknown roles: skip resource-presence check (warning, not error — no
	// warnings channel in this codebase, so unknown-role is silently accepted).
	kind, known := rbac.ClassifyRole(rb.Spec.Role)
	if known {
		switch kind {
		case rbac.RoleClusterScoped:
			if len(rb.Spec.Resources) > 0 {
				errs = append(errs, errf("KafkaRoleBinding %s: role %q is cluster-scoped and must not have spec.resources", name, rb.Spec.Role))
			}
		case rbac.RoleResourceScoped:
			if len(rb.Spec.Resources) == 0 {
				errs = append(errs, errf("KafkaRoleBinding %s: role %q is resource-scoped and requires at least one entry in spec.resources", name, rb.Spec.Role))
			}
		}
	}
	// RoleUnknown: no error, no resource-presence enforcement (decision 18).

	// Per-resource shape checks (also run for unknown-role bindings that have resources).
	validTypesForScope := rbResourceTypesByScopeType[rb.Spec.Scope.Type] // may be nil for unknown scope
	for i, res := range rb.Spec.Resources {
		if res.Name == "" {
			errs = append(errs, errf("KafkaRoleBinding %s resource %d: name must be non-empty", name, i))
		}
		if res.PatternType != "" && !validPatternTypes[res.PatternType] {
			errs = append(errs, errf("KafkaRoleBinding %s resource %d: invalid patternType %q", name, i, res.PatternType))
		}
		if res.Type == "" {
			errs = append(errs, errf("KafkaRoleBinding %s resource %d: type must be non-empty", name, i))
		} else if validTypesForScope != nil && !validTypesForScope[res.Type] {
			errs = append(errs, errf("KafkaRoleBinding %s resource %d: invalid resource type %q for scope.type %q", name, i, res.Type, rb.Spec.Scope.Type))
		}
	}

	return errs
}

// ValidateUserShape checks the per-resource shape rules for a KafkaUser
// (v0.35 §T2). It does NOT check clusterRef resolution, CLI-vs-operator
// password-source legality, or (clusterRef, username) identity uniqueness —
// clusterRef resolution and identity uniqueness require cross-resource
// context and are enforced by the top-level Validate function; the
// CLI-vs-operator distinction for password.generate is enforced by the CLI
// pipeline layer (see internal/pipeline), since core validation has no
// notion of "mode" and the CRD/webhook path must accept generate. It DOES
// reject inline and configMapKeyRef password sources outright, regardless of
// mode: both are plaintext-in-non-secret-storage footguns that neither
// runtime accepts (the CLI's FileEnvResolver and the operator's
// sourceReferencedPassword both error on them), so rejecting them here
// surfaces the failure at the earliest common point. This function is
// exported so the admission webhook can reuse it without pulling in
// cross-resource dependencies.
func ValidateUserShape(u *v1alpha1.KafkaUser) []error {
	var errs []error
	name := u.Name

	if u.APIVersion != v1alpha1.APIVersion {
		errs = append(errs, errf("KafkaUser %s: apiVersion must be %s", name, v1alpha1.APIVersion))
	}

	// username: non-empty after defaulting (defaulting.User resolves it from
	// metadata.name, so empty here means metadata.name was also empty).
	// Charset: permissive by design (Kafka principals are permissive) — only
	// whitespace/control chars and the SASL/config-sensitive NUL, '=', ','
	// are rejected.
	if u.Spec.Username == "" {
		errs = append(errs, errf("KafkaUser %s: spec.username must be non-empty", name))
	} else if userUsernameForbidden.MatchString(u.Spec.Username) {
		errs = append(errs, errf("KafkaUser %s: spec.username %q contains a forbidden character (whitespace, control characters, NUL, '=', and ',' are not allowed)", name, u.Spec.Username))
	}

	// mechanism: CRD-enforced enum, but the CLI has no apiserver, so check
	// here too.
	if u.Spec.Mechanism != "" && !validUserMechanisms[u.Spec.Mechanism] {
		errs = append(errs, errf("KafkaUser %s: invalid mechanism %q (must be SCRAM-SHA-256 or SCRAM-SHA-512)", name, u.Spec.Mechanism))
	}

	// iterations: CRD-enforced bounds, checked again for the same reason.
	if u.Spec.Iterations != nil {
		if it := *u.Spec.Iterations; it < 4096 || it > 16384 {
			errs = append(errs, errf("KafkaUser %s: spec.iterations must be between 4096 and 16384, got %d", name, it))
		}
	}

	// password: required; exactly one of valueFrom/generate.
	if u.Spec.Password == nil {
		errs = append(errs, errf("KafkaUser %s: spec.password is required (set spec.password.valueFrom or spec.password.generate)", name))
	} else {
		p := u.Spec.Password
		hasValueFrom := p.ValueFrom != nil
		hasGenerate := p.Generate != nil
		switch {
		case !hasValueFrom && !hasGenerate:
			errs = append(errs, errf("KafkaUser %s: spec.password must set exactly one of valueFrom or generate; neither set", name))
		case hasValueFrom && hasGenerate:
			errs = append(errs, errf("KafkaUser %s: spec.password must set exactly one of valueFrom or generate; both set", name))
		case hasValueFrom:
			// inline and configMapKeyRef are both plaintext-password-in-non-secret-storage
			// footguns: reject outright, regardless of CLI/operator mode. Both
			// runtimes independently reject them too (the CLI's FileEnvResolver,
			// and the operator's sourceReferencedPassword), so catching them here
			// just surfaces the same terminal outcome earlier. secretKeyRef is
			// fine for the operator; env/file are fine for the CLI.
			if p.ValueFrom.Inline != "" {
				errs = append(errs, errf("KafkaUser %s: spec.password.valueFrom.inline is not allowed for a password (plaintext password would be committed to git); use env, file, or secretKeyRef instead", name))
			}
			if p.ValueFrom.ConfigMapKeyRef != nil {
				errs = append(errs, errf("KafkaUser %s: spec.password.valueFrom.configMapKeyRef is not allowed for passwords (ConfigMaps are not secret storage); use secretKeyRef (operator) or env/file (CLI)", name))
			}
			if n := countValueSources(*p.ValueFrom); n == 0 {
				errs = append(errs, errf("KafkaUser %s: spec.password.valueFrom must specify exactly one source (env, file, or secretKeyRef); none set", name))
			} else if n > 1 {
				errs = append(errs, errf("KafkaUser %s: spec.password.valueFrom must specify exactly one source (env, file, or secretKeyRef); %d set", name, n))
			}
		}
		// hasGenerate alone: shape-valid here. generate is operator-only; the
		// CLI pipeline rejects it (see pipeline.go / secrets resolution).
	}

	if u.Spec.ClusterRef.Name == "" {
		errs = append(errs, errf("KafkaUser %s: spec.clusterRef.name is required", name))
	}

	if u.Spec.DeletionPolicy != "" && u.Spec.DeletionPolicy != "Orphan" && u.Spec.DeletionPolicy != "Delete" {
		errs = append(errs, errf("KafkaUser %s: invalid deletionPolicy %q", name, u.Spec.DeletionPolicy))
	}

	return errs
}

func Validate(in Input) []error {
	var errs []error
	seen := map[string]string{} // "cluster\x00topicName" -> resource name

	for _, tp := range in.Topics {
		name := tp.Name
		if name == "" {
			errs = append(errs, errf("KafkaTopic: metadata.name must be non-empty"))
		}
		if tp.APIVersion != v1alpha1.APIVersion {
			errs = append(errs, errf("KafkaTopic %s: apiVersion must be %s", name, v1alpha1.APIVersion))
		}
		if tp.Spec.ClusterRef.Name == "" {
			errs = append(errs, errf("KafkaTopic %s: spec.clusterRef.name is required", name))
		}
		if tp.Spec.Partitions < 1 {
			errs = append(errs, errf("KafkaTopic %s: spec.partitions must be >= 1", name))
		}
		if tp.Spec.ReplicationFactor != nil && *tp.Spec.ReplicationFactor < 1 {
			errs = append(errs, errf("KafkaTopic %s: spec.replicationFactor must be >= 1", name))
		}
		// Replication factor and Confluent replica-placement constraints are
		// mutually exclusive: when a placement constraint is configured the broker
		// derives replication from it, so an explicit replication factor must be
		// omitted. Reject the combination early with a clear message.
		if tp.Spec.ReplicationFactor != nil {
			if _, hasPlacement := tp.Spec.Config[placementConstraintsKey]; hasPlacement {
				errs = append(errs, errf("KafkaTopic %s: spec.replicationFactor and config %q are mutually exclusive; omit replicationFactor and let the placement constraint determine replication", name, placementConstraintsKey))
			}
		}
		topicName := tp.ResolvedTopicName()
		if tp.Spec.Reconciliation != nil && tp.Spec.Reconciliation.Mode != "" && !validModes[tp.Spec.Reconciliation.Mode] {
			errs = append(errs, errf("KafkaTopic %s: invalid reconciliation.mode %q", name, tp.Spec.Reconciliation.Mode))
		}
		if tp.Spec.DeletionPolicy != "" && tp.Spec.DeletionPolicy != "Orphan" && tp.Spec.DeletionPolicy != "Delete" {
			errs = append(errs, errf("KafkaTopic %s: invalid deletionPolicy %q", name, tp.Spec.DeletionPolicy))
		}
		if tp.Spec.Drift != nil {
			for _, f := range tp.Spec.Drift.IgnoreFields {
				if !validIgnoreField.MatchString(f) {
					errs = append(errs, errf("KafkaTopic %s: invalid drift.ignoreFields entry %q (must be \"partitions\", \"replicationFactor\", or \"config.<key>\")", name, f))
				}
			}
		}
		for i, p := range tp.Spec.Access.Producers {
			if p.Principal == "" {
				errs = append(errs, errf("KafkaTopic %s: producer principal must be non-empty", name))
			}
			if p.Host != "" && strings.TrimSpace(p.Host) == "" {
				errs = append(errs, errf("KafkaTopic %s: access producer host must not be blank", name))
			}
			for _, op := range p.Operations {
				if !validOps[op] {
					errs = append(errs, opError(fmt.Sprintf("KafkaTopic %s producer %d", name, i), op))
				}
			}
		}
		for i, c := range tp.Spec.Access.Consumers {
			if c.Principal == "" {
				errs = append(errs, errf("KafkaTopic %s: consumer principal must be non-empty", name))
			}
			if c.Group == "" {
				errs = append(errs, errf("KafkaTopic %s: consumer group must be non-empty", name))
			}
			if c.Host != "" && strings.TrimSpace(c.Host) == "" {
				errs = append(errs, errf("KafkaTopic %s: access consumer host must not be blank", name))
			}
			for _, op := range c.TopicOperations {
				if !validOps[op] {
					errs = append(errs, opError(fmt.Sprintf("KafkaTopic %s consumer %d topicOperations", name, i), op))
				}
			}
			for _, op := range c.GroupOperations {
				if !validOps[op] {
					errs = append(errs, opError(fmt.Sprintf("KafkaTopic %s consumer %d groupOperations", name, i), op))
				}
			}
		}
		if tp.Spec.Schema != nil {
			sc := tp.Spec.Schema
			if !validSchemaFormats[sc.Format] {
				errs = append(errs, errf("KafkaTopic %s: invalid schema format %q (must be one of AVRO, JSON, PROTOBUF)", name, sc.Format))
			}
			// Subject-strategy validation (spec §11). A zero-value strategy is
			// TopicName; an unknown value is rejected (the CRD enum covers operator
			// input, this covers raw YAML the CLI lints). Each strategy has its own
			// content/subject prerequisites enforced below.
			strategy := sc.SubjectStrategy
			if strategy == "" {
				strategy = "TopicName"
			}
			strategyKnown := validSubjectStrategies[strategy]
			if !strategyKnown {
				errs = append(errs, errf("KafkaTopic %s: invalid subjectStrategy %q", name, sc.SubjectStrategy))
			}
			if sc.Compatibility != "" && !validCompatibilities[sc.Compatibility] {
				errs = append(errs, errf("KafkaTopic %s: invalid compatibility %q", name, sc.Compatibility))
			}
			if strategyKnown {
				switch strategy {
				case "RecordName", "TopicRecordName":
					// The subject is the record full name extracted from the schema
					// BODY, so these strategies are illegal in governance mode (no
					// content). valueSchema is required; keySchema is optional.
					if sc.ValueSchema == nil {
						errs = append(errs, errf("KafkaTopic %s: subjectStrategy %q requires valueSchema (the subject is derived from the schema body; governance mode has no body to extract from)", name, strategy))
					}
				case "Custom":
					// The subjects are named verbatim, so an explicit valueSubject is
					// required, and a keySubject is required whenever a key schema is
					// present. This is also how an arbitrary subject is governed
					// (no body needed).
					if sc.ValueSubject == "" {
						errs = append(errs, errf("KafkaTopic %s: subjectStrategy Custom requires spec.schema.valueSubject", name))
					}
					if sc.KeySchema != nil && sc.KeySubject == "" {
						errs = append(errs, errf("KafkaTopic %s: subjectStrategy Custom with a keySchema requires spec.schema.keySubject", name))
					}
					// Both subjects are literal strings known at validate time; detect
					// a collision immediately rather than deferring to build time.
					if sc.ValueSubject != "" && sc.KeySubject != "" && sc.ValueSubject == sc.KeySubject {
						errs = append(errs, errf("KafkaTopic %s: subjectStrategy Custom: valueSubject and keySubject must differ (both resolve to %q)", name, sc.ValueSubject))
					}
				case "TopicName":
					// Subjects are <topic>-value / <topic>-key; no extra inputs.
				}
				// valueSubject/keySubject only mean anything under Custom; setting
				// them with another strategy would be silently ignored, so reject.
				if strategy != "Custom" && (sc.ValueSubject != "" || sc.KeySubject != "") {
					errs = append(errs, errf("KafkaTopic %s: spec.schema.valueSubject/keySubject are only valid with subjectStrategy Custom", name))
				}
			}
			// Two supported modes (spec §12.2). CONTENT mode: a valueSchema (and
			// optionally a keySchema) body is supplied and monedula registers
			// versions. GOVERNANCE mode: no body at all — monedula manages only
			// the subject compatibility level and producers register versions
			// out-of-band. keySchema without valueSchema stays legal (content
			// mode for the key subject).
			if sc.ValueSchema == nil && sc.KeySchema == nil {
				if sc.Compatibility == "" {
					errs = append(errs, errf("KafkaTopic %s: spec.schema without valueSchema/keySchema manages compatibility only (governance mode) and requires spec.schema.compatibility", name))
				}
			} else {
				if sc.ValueSchema != nil {
					validateValueSource(&errs, fmt.Sprintf("KafkaTopic %s: spec.schema.valueSchema", name), *sc.ValueSchema)
				}
				if sc.KeySchema != nil {
					validateValueSource(&errs, fmt.Sprintf("KafkaTopic %s: spec.schema.keySchema", name), *sc.KeySchema)
				}
			}
		}
		// Identity collisions are keyed on (clusterRef.name, resolved topicName)
		// alone, so they are checked even without loaded cluster configs (the
		// typical `validate -f` PR lint runs without --cluster-config-file).
		key := tp.Spec.ClusterRef.Name + "\x00" + topicName
		if prev, dup := seen[key]; dup {
			errs = append(errs, errf("KafkaTopic %s collides with %s: both resolve to topicName %q on cluster %q", name, prev, topicName, tp.Spec.ClusterRef.Name))
		} else {
			seen[key] = name
		}
		if in.Clusters != nil {
			cl, ok := in.Clusters[tp.Spec.ClusterRef.Name]
			if !ok {
				errs = append(errs, errf("KafkaTopic %s references cluster %q, but no KafkaCluster config with that name was provided", name, tp.Spec.ClusterRef.Name))
			} else if tp.Spec.Schema != nil && (cl.Spec.SchemaRegistry == nil || cl.Spec.SchemaRegistry.Endpoint == "") {
				errs = append(errs, errf("KafkaTopic %s: spec.schema requires KafkaCluster %q to configure schemaRegistry", name, tp.Spec.ClusterRef.Name))
			}
		}
	}

	for _, pol := range in.Policies {
		name := pol.Name
		if name == "" {
			errs = append(errs, errf("KafkaAccessPolicy: metadata.name must be non-empty"))
		}
		if pol.Spec.ClusterRef.Name == "" {
			errs = append(errs, errf("KafkaAccessPolicy %s: spec.clusterRef.name is required", name))
		}

		// Per-resource shape checks (reused by the webhook).
		errs = append(errs, ValidateAccessPolicyShape(pol)...)

		if in.Clusters != nil {
			if _, ok := in.Clusters[pol.Spec.ClusterRef.Name]; !ok {
				errs = append(errs, errf("KafkaAccessPolicy %s references cluster %q, but no KafkaCluster config with that name was provided", name, pol.Spec.ClusterRef.Name))
			}
		}
	}

	// KafkaQuota validation (spec §39.5).
	seenQuotas := map[string]string{} // "clusterName\x00entityKey" -> resource name
	for _, q := range in.Quotas {
		name := q.Name

		// Per-resource shape checks (reused by the webhook).
		errs = append(errs, ValidateQuotaShape(q)...)

		// Identity collision: keyed on (clusterRef.name, entity key). Checked
		// even without cluster config (mirrors topic identity-collision behaviour).
		compiled := quota.Compile(q)
		key := q.Spec.ClusterRef.Name + "\x00" + compiled.Entity.Key()
		if prev, dup := seenQuotas[key]; dup {
			errs = append(errs, errf("KafkaQuota %s collides with %s: both resolve to the same entity on cluster %q", name, prev, q.Spec.ClusterRef.Name))
		} else {
			seenQuotas[key] = name
		}

		// ClusterRef resolution (only when cluster config was provided).
		if in.Clusters != nil {
			if _, ok := in.Clusters[q.Spec.ClusterRef.Name]; !ok {
				errs = append(errs, errf("KafkaQuota %s references cluster %q, but no KafkaCluster config with that name was provided", name, q.Spec.ClusterRef.Name))
			}
		}
	}

	// KafkaRoleBinding validation (spec §40).
	seenRBKeys := map[string]string{} // "clusterName\x00fullKey" -> resource name
	for _, rb := range in.RoleBindings {
		name := rb.Name

		// Per-resource shape checks (reused by the webhook).
		errs = append(errs, ValidateRoleBindingShape(rb)...)

		// ClusterRef resolution (only when cluster config was provided).
		if in.Clusters != nil {
			cl, ok := in.Clusters[rb.Spec.ClusterRef.Name]
			if !ok {
				errs = append(errs, errf("KafkaRoleBinding %s references cluster %q, but no KafkaCluster config with that name was provided", name, rb.Spec.ClusterRef.Name))
				continue // cannot check MDS or scope-id without the cluster
			}
			// MDS required: role bindings can't work without MDS on the cluster.
			if cl.Spec.Authorization == nil || cl.Spec.Authorization.MDS == nil {
				errs = append(errs, errf("KafkaRoleBinding %s: cluster %q does not have authorization.mds configured (required for role bindings)", name, rb.Spec.ClusterRef.Name))
				continue // cannot check scope-id or compile identities without MDS
			}
			mds := cl.Spec.Authorization.MDS
			// KafkaCluster id is always required.
			if mds.Clusters.KafkaCluster == "" {
				errs = append(errs, errf("KafkaRoleBinding %s: cluster %q authorization.mds.clusters.kafkaCluster must be set", name, rb.Spec.ClusterRef.Name))
			}
			// Scope-type-specific cluster id must be set.
			switch rb.Spec.Scope.Type {
			case "schema-registry":
				if mds.Clusters.SchemaRegistryCluster == "" {
					errs = append(errs, errf("KafkaRoleBinding %s: scope.type %q requires cluster %q authorization.mds.clusters.schemaRegistryCluster to be set", name, rb.Spec.Scope.Type, rb.Spec.ClusterRef.Name))
				}
			case "connect":
				if mds.Clusters.ConnectCluster == "" {
					errs = append(errs, errf("KafkaRoleBinding %s: scope.type %q requires cluster %q authorization.mds.clusters.connectCluster to be set", name, rb.Spec.Scope.Type, rb.Spec.ClusterRef.Name))
				}
			case "ksql":
				if mds.Clusters.KsqlCluster == "" {
					errs = append(errs, errf("KafkaRoleBinding %s: scope.type %q requires cluster %q authorization.mds.clusters.ksqlCluster to be set", name, rb.Spec.Scope.Type, rb.Spec.ClusterRef.Name))
				}
			}
			// Identity uniqueness: compile the binding's per-resource identities and
			// detect collisions across all loaded role bindings.
			// rbac.Compile errors if kafkaCluster is empty OR the scope's sub-cluster
			// id is unresolvable; in either case we've already appended the scope-id
			// error above, so skip collision tracking for this binding.
			// The `if compErr == nil` guard inside is load-bearing, not dead —
			// do not remove it.
			if mds.Clusters.KafkaCluster != "" {
				compiled, compErr := rbac.Compile(rb, mds)
				if compErr == nil {
					for _, b := range compiled {
						key := rb.Spec.ClusterRef.Name + "\x00" + b.FullKey()
						if prev, dup := seenRBKeys[key]; dup {
							errs = append(errs, errf("KafkaRoleBinding %s collides with %s: both produce the same role binding identity on cluster %q", name, prev, rb.Spec.ClusterRef.Name))
						} else {
							seenRBKeys[key] = name
						}
					}
				}
			}
		}
	}

	// KafkaUser validation (v0.35 §T2).
	seenUsers := map[string]string{} // "clusterName\x00username" -> resource name
	for _, u := range in.Users {
		name := u.Name

		// Per-resource shape checks (reused by the webhook).
		errs = append(errs, ValidateUserShape(u)...)

		// Identity collision: keyed on (clusterRef.name, username) alone —
		// regardless of mechanism, two CRs for the same username on the same
		// cluster fight over the same credential set. Checked even without
		// cluster config (mirrors topic/quota identity-collision behaviour).
		key := user.Key(u.Spec.ClusterRef.Name, u.Spec.Username)
		if prev, dup := seenUsers[key]; dup {
			errs = append(errs, errf("KafkaUser %s collides with %s: both resolve to username %q on cluster %q", name, prev, u.Spec.Username, u.Spec.ClusterRef.Name))
		} else {
			seenUsers[key] = name
		}

		// ClusterRef resolution (only when cluster config was provided).
		if in.Clusters != nil {
			if _, ok := in.Clusters[u.Spec.ClusterRef.Name]; !ok {
				errs = append(errs, errf("KafkaUser %s references cluster %q, but no KafkaCluster config with that name was provided", name, u.Spec.ClusterRef.Name))
			}
		}
	}

	// Cluster configs themselves (sorted by map key for deterministic output).
	keys := make([]string, 0, len(in.Clusters))
	for k := range in.Clusters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		cl := in.Clusters[k]
		if cl == nil {
			continue
		}
		if cl.Name == "" {
			errs = append(errs, errf("KafkaCluster %s: metadata.name must be non-empty", k))
		}
		if cl.APIVersion != v1alpha1.APIVersion {
			errs = append(errs, errf("KafkaCluster %s: apiVersion must be %s", k, v1alpha1.APIVersion))
		}
		// schemaRegistry.auth.type: only "basic" (or empty = no auth) is
		// implemented. internal/cluster ignores unknown types, which would
		// silently connect WITHOUT auth — reject them here instead.
		if cl.Spec.SchemaRegistry != nil && cl.Spec.SchemaRegistry.Auth != nil {
			if at := cl.Spec.SchemaRegistry.Auth.Type; at != "" && at != "basic" {
				errs = append(errs, errf("KafkaCluster %s: unknown schemaRegistry.auth.type %q (only \"basic\" is supported)", k, at))
			}
		}
		// defaults.topicDeletionPolicy must be Orphan, Delete, or absent.
		if cl.Spec.Defaults != nil {
			if p := cl.Spec.Defaults.TopicDeletionPolicy; p != "" && p != "Orphan" && p != "Delete" {
				errs = append(errs, errf("KafkaCluster %s: invalid defaults.topicDeletionPolicy %q (must be Orphan or Delete)", k, p))
			}
		}
		// Independent TLS rules: (a) clientCert and clientKey must be set together;
		// (b) both certs require tls.enabled: true (otherwise they are silently ignored
		//     at build time, which would be a confusing silent failure).
		// The same shape rules apply to schemaRegistry.tls.
		errs = append(errs, validateTLSShape(k, "tls", cl.Spec.TLS)...)
		if cl.Spec.SchemaRegistry != nil {
			errs = append(errs, validateTLSShape(k, "schemaRegistry.tls", cl.Spec.SchemaRegistry.TLS)...)
		}
		if cl.Spec.Authorization != nil && cl.Spec.Authorization.MDS != nil {
			errs = append(errs, validateTLSShape(k, "authorization.mds.tls", cl.Spec.Authorization.MDS.TLS)...)
		}
		// auth.mechanism validation (TLS config passed for mTLS checks).
		if cl.Spec.Auth != nil {
			errs = append(errs, validateAuthConfig(k, cl.Spec.Auth, cl.Spec.TLS)...)
		}
		// Tenancy shape checks (spec §20.2): each TopicPrefixRule must have at
		// least one namespace and at least one prefix pattern; every glob in
		// AllowedNamespaces and TopicPrefixes.Namespaces must compile cleanly
		// (path.Match returns ErrBadPattern for structurally invalid patterns —
		// smoke-test each against a probe string to catch them early).
		if cl.Spec.Tenancy != nil {
			errs = append(errs, validateTenancyConfig(k, cl.Spec.Tenancy)...)
		}
		// Authorization backend validation (spec §40): each accessBackends entry
		// must be "acl" or "rbac", and "rbac" requires authorization.mds.
		errs = append(errs, ValidateClusterAuthorization(cl)...)
	}
	return errs
}

// validateTLSShape checks the independent shape rules of a TLS block:
// (a) clientCert and clientKey must be set together; (b) either requires
// enabled: true (otherwise they are silently ignored at build time).
// fieldPath is the spec path of the block ("tls", "schemaRegistry.tls") used
// in error messages. Nil tls is valid (no TLS configured).
func validateTLSShape(clusterName, fieldPath string, tls *v1alpha1.TLSConfig) []error {
	if tls == nil {
		return nil
	}
	var errs []error
	hasCert := tls.ClientCert != nil
	hasKey := tls.ClientKey != nil
	if hasCert != hasKey {
		errs = append(errs, errf("KafkaCluster %s: %s.clientCert and %s.clientKey must be set together (one is missing)", clusterName, fieldPath, fieldPath))
	}
	if (hasCert || hasKey) && !tls.Enabled {
		errs = append(errs, errf("KafkaCluster %s: %s.clientCert/%s.clientKey require %s.enabled: true", clusterName, fieldPath, fieldPath, fieldPath))
	}
	return errs
}

// validateAuthConfig checks auth mechanism rules for a KafkaCluster.
// tls is the cluster's TLS config (may be nil); it is used for mTLS-specific checks.
func validateAuthConfig(clusterName string, a *v1alpha1.AuthConfig, tls *v1alpha1.TLSConfig) []error {
	var errs []error
	switch a.Mechanism {
	case "", "None", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		// These mechanisms use auth.scram; oauth must be absent.
		if a.OAuth != nil {
			errs = append(errs, errf("KafkaCluster %s: auth.oauth must not be set for mechanism %q", clusterName, a.Mechanism))
		}
	case "OAUTHBEARER":
		// auth.scram must be absent.
		if a.SCRAM != nil {
			errs = append(errs, errf("KafkaCluster %s: auth.scram is not allowed with mechanism OAUTHBEARER (use auth.oauth)", clusterName))
		}
		// auth.oauth must be present and well-formed.
		if a.OAuth == nil {
			errs = append(errs, errf("KafkaCluster %s: mechanism OAUTHBEARER requires auth.oauth to be set", clusterName))
		} else {
			if a.OAuth.TokenEndpoint == "" {
				errs = append(errs, errf("KafkaCluster %s: auth.oauth.tokenEndpoint must be non-empty", clusterName))
			}
			if isEmptyValueFrom(a.OAuth.ClientID) {
				errs = append(errs, errf("KafkaCluster %s: auth.oauth.clientId must specify a secret source", clusterName))
			}
			if isEmptyValueFrom(a.OAuth.ClientSecret) {
				errs = append(errs, errf("KafkaCluster %s: auth.oauth.clientSecret must specify a secret source", clusterName))
			}
		}
	case "mTLS":
		// mTLS = TLS client-certificate authentication (spec §4.5). No SASL is
		// used; the client certificate is the authentication credential.
		// Requires tls.enabled and both tls.clientCert and tls.clientKey.
		// NOTE: the tls.enabled and cert/key-present checks below intentionally
		// overlap with the independent TLS rules in Validate(); for mTLS the
		// auth-rule errors are the primary user-facing message.
		if tls == nil || !tls.Enabled {
			errs = append(errs, errf("KafkaCluster %s: mechanism mTLS requires tls.enabled: true", clusterName))
		}
		if tls == nil || tls.ClientCert == nil {
			errs = append(errs, errf("KafkaCluster %s: mechanism mTLS requires tls.clientCert to be set", clusterName))
		}
		if tls == nil || tls.ClientKey == nil {
			errs = append(errs, errf("KafkaCluster %s: mechanism mTLS requires tls.clientKey to be set", clusterName))
		}
		// auth.scram and auth.oauth are incompatible with mTLS (no SASL).
		if a.SCRAM != nil {
			errs = append(errs, errf("KafkaCluster %s: auth.scram must not be set for mechanism mTLS", clusterName))
		}
		if a.OAuth != nil {
			errs = append(errs, errf("KafkaCluster %s: auth.oauth must not be set for mechanism mTLS", clusterName))
		}
	default:
		errs = append(errs, errf("KafkaCluster %s: unknown auth mechanism %q", clusterName, a.Mechanism))
	}
	return errs
}

// isEmptyValueFrom reports whether a ValueFrom has no secret source set.
func isEmptyValueFrom(vf v1alpha1.ValueFrom) bool {
	return countValueSources(vf.ValueFrom) == 0
}

// countValueSources returns the number of non-zero source fields in a ValueSource.
func countValueSources(src v1alpha1.ValueSource) int {
	n := 0
	if src.Env != "" {
		n++
	}
	if src.File != "" {
		n++
	}
	if src.SecretKeyRef != nil {
		n++
	}
	if src.Inline != "" {
		n++
	}
	if src.ConfigMapKeyRef != nil {
		n++
	}
	return n
}

// validateValueSource enforces that exactly one of env/file/secretKeyRef/inline/
// configMapKeyRef is set. It appends an error using fieldPath (e.g.
// "KafkaTopic orders: spec.schema.valueSchema") for diagnostics.
func validateValueSource(errs *[]error, fieldPath string, vf v1alpha1.ValueFrom) {
	n := countValueSources(vf.ValueFrom)
	switch {
	case n == 0:
		*errs = append(*errs, errf("%s: valueFrom must specify exactly one source (env, file, secretKeyRef, inline, or configMapKeyRef); none set", fieldPath))
	case n > 1:
		*errs = append(*errs, errf("%s: valueFrom must specify exactly one source (env, file, secretKeyRef, inline, or configMapKeyRef); %d set", fieldPath, n))
	}
}

// validateTenancyConfig checks the shape of a TenancyConfig (spec §20.2):
//   - Each glob pattern in AllowedNamespaces must compile (path.Match with a
//     probe string catches ErrBadPattern).
//   - Each TopicPrefixRule must have at least one namespace and at least one
//     prefix; glob patterns in TopicPrefixRule.Namespaces must also compile.
//
// These are shape checks only. The tenancy package enforces the policy at
// reconcile time; validation rejects configs that can never be enforced.
func validateTenancyConfig(clusterName string, t *v1alpha1.TenancyConfig) []error {
	var errs []error

	for i, pattern := range t.AllowedNamespaces {
		if _, err := path.Match(pattern, "probe"); err != nil {
			errs = append(errs, errf("KafkaCluster %s: tenancy.allowedNamespaces[%d]: invalid glob pattern %q: %v", clusterName, i, pattern, err))
		}
	}

	for i, rule := range t.TopicPrefixes {
		if len(rule.Namespaces) == 0 {
			errs = append(errs, errf("KafkaCluster %s: tenancy.topicPrefixes[%d]: namespaces must be non-empty", clusterName, i))
		}
		if len(rule.Prefixes) == 0 {
			errs = append(errs, errf("KafkaCluster %s: tenancy.topicPrefixes[%d]: prefixes must be non-empty", clusterName, i))
		}
		for j, pattern := range rule.Namespaces {
			if _, err := path.Match(pattern, "probe"); err != nil {
				errs = append(errs, errf("KafkaCluster %s: tenancy.topicPrefixes[%d].namespaces[%d]: invalid glob pattern %q: %v", clusterName, i, j, pattern, err))
			}
		}
	}

	return errs
}
