package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MAINTAINER NOTE — doc comments in this file feed controller-gen markers
// (e.g. +kubebuilder:validation:XValidation CEL rules). Starting with Go 1.26,
// gofmt's doc-comment reformatter rewrites paired ASCII quote sequences in
// comments — two adjacent apostrophes, or two adjacent backquotes — into a
// single typographer (curly) quote glyph, which silently corrupts CEL
// expressions and their messages. Never write a CEL empty-string literal
// (two apostrophes) in a rule here — compare with field.size() == 0 instead —
// and avoid such paired quote sequences anywhere in this file's comments.
// Single apostrophes (e.g. "cluster's") are safe. CI greps this file for the
// reintroduction of CEL empty-string comparisons.
const APIVersion = "gitops.monedula.dev/v1alpha1"

// Phase constants reported in .status.phase.
const (
	PhasePending  = "Pending"
	PhaseReady    = "Ready"
	PhaseError    = "Error"
	PhaseDrifted  = "Drifted"
	PhaseDeleting = "Deleting"
)

// Condition type constants used in .status.conditions.
const (
	CondReady                   = "Ready"
	CondClusterReachable        = "ClusterReachable"
	CondAuthenticated           = "Authenticated"
	CondSchemaRegistryReachable = "SchemaRegistryReachable"
	CondTopicSynced             = "TopicSynced"
	CondTopicAccessSynced       = "TopicAccessSynced"
	CondSchemaSynced            = "SchemaSynced"
	CondDriftDetected           = "DriftDetected"
	CondAccessPolicySynced      = "AccessPolicySynced"
	CondValidationFailed        = "ValidationFailed"
	CondQuotaSynced             = "QuotaSynced"
	// CondUserSynced is set on a KafkaUser after SCRAM credential operations
	// are applied: True when the declared credential converges, False when one
	// or more operations could not be applied (v0.35).
	CondUserSynced = "UserSynced"
	// CondACLConflict is set when two resources express opposing Allow/Deny
	// permissions for the same ACL subject (spec §9).
	CondACLConflict = "ACLConflict"
	// CondSchemaSourceUnwatched is set on a KafkaTopic when a referenced schema
	// ConfigMap lacks the gitops.monedula.dev/schema-source watch label, so its
	// edits reconcile only at the periodic resync, not promptly (§11.3).
	// Informational; does not fail the reconcile.
	CondSchemaSourceUnwatched = "SchemaSourceUnwatched"
	// CondRoleBindingSynced is set on a KafkaRoleBinding — and on a KafkaTopic
	// whose cluster has the "rbac" accessBackend (its access-derived role
	// bindings) — after MDS role-binding operations are applied: True when the
	// desired set converges, False when one or more operations could not be
	// applied (spec §40).
	CondRoleBindingSynced = "RoleBindingSynced"
	// CondMDSReachable is set on a KafkaRoleBinding — and on a KafkaTopic whose
	// cluster has the "rbac" accessBackend — after the live-state read from the
	// Confluent Metadata Service: True when ListRoleBindings succeeds, False on a
	// transient MDS connectivity failure (spec §40).
	CondMDSReachable = "MDSReachable"
	// CondRBACCoarsened is set on a KafkaTopic when its access block is
	// auto-mapped to a coarser RBAC role binding because an entry's non-"*"
	// host or custom operation list cannot be faithfully represented in RBAC
	// (spec §40). Informational; does not fail the reconcile.
	CondRBACCoarsened = "RBACCoarsened"
	// CondCredentialSourceUnwatched is set on a KafkaCluster when a referenced
	// credential/TLS Secret lacks the gitops.monedula.dev/credential-source watch
	// label, so a rotation reconciles only at the periodic resync, not promptly
	// (§11.4). Informational; does not fail the reconcile.
	CondCredentialSourceUnwatched = "CredentialSourceUnwatched"
	// CondSchemaRegistryDegraded is set on a KafkaTopic when the Schema
	// Registry's GLOBAL compatibility level (GET /config) could not be read
	// during this reconcile: the level falls back to "" (unknown), so the diff
	// classifies a first-time subject-level compatibility set using the legacy
	// any-initial-set-is-a-Raise rule instead of comparing against the true
	// global default (spec §17.1). Informational; does not fail the reconcile.
	CondSchemaRegistryDegraded = "SchemaRegistryDegraded"
)

type ClusterRef struct {
	Name string `json:"name"`
}

type ValueSource struct {
	Env          string        `json:"env,omitempty"`
	File         string        `json:"file,omitempty"`
	SecretKeyRef *SecretKeyRef `json:"secretKeyRef,omitempty"`
	// Inline is the literal value verbatim (intended for schema bodies).
	Inline string `json:"inline,omitempty"`
	// ConfigMapKeyRef reads a ConfigMap key (operator mode only).
	// Uses the same {Name, Key} shape as SecretKeyRef; no Secret is involved.
	ConfigMapKeyRef *SecretKeyRef `json:"configMapKeyRef,omitempty"`
}

type SecretKeyRef struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

type ValueFrom struct {
	ValueFrom ValueSource `json:"valueFrom"`
}

// ---- KafkaCluster ----

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

type KafkaCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              KafkaClusterSpec    `json:"spec,omitempty"`
	Status            *KafkaClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type KafkaClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaCluster `json:"items"`
}

type KafkaClusterSpec struct {
	BootstrapServers string               `json:"bootstrapServers"`
	TLS              *TLSConfig           `json:"tls,omitempty"`
	Auth             *AuthConfig          `json:"auth,omitempty"`
	SchemaRegistry   *SchemaRegistryConf  `json:"schemaRegistry,omitempty"`
	Defaults         *ClusterDefaults     `json:"defaults,omitempty"`
	Tenancy          *TenancyConfig       `json:"tenancy,omitempty"`
	Authorization    *AuthorizationConfig `json:"authorization,omitempty"`
}

// AuthorizationConfig configures authorization backends beyond plain ACLs. MDS
// enables Confluent RBAC role bindings (spec §40). AccessBackends selects how a
// KafkaTopic.spec.access block is realized on this cluster (spec §40):
//   - unset/empty → ["acl"] (back-compat: ACLs only);
//   - ["rbac"]     → MDS role bindings only;
//   - ["acl","rbac"] → both (dual-emit).
//
// "rbac" requires MDS to be configured.
type AuthorizationConfig struct {
	MDS *MDSConfig `json:"mds,omitempty"`
	// +kubebuilder:validation:items:Enum=acl;rbac
	AccessBackends []string `json:"accessBackends,omitempty"`
}

// EffectiveAccessBackends returns the normalized authorization backends for a
// cluster: unset/empty → ["acl"] (back-compat). Duplicates collapse; first-seen
// order is preserved. Safe on a nil cluster.
func EffectiveAccessBackends(c *KafkaCluster) []string {
	if c == nil || c.Spec.Authorization == nil || len(c.Spec.Authorization.AccessBackends) == 0 {
		return []string{"acl"}
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(c.Spec.Authorization.AccessBackends))
	for _, b := range c.Spec.Authorization.AccessBackends {
		if seen[b] {
			continue
		}
		seen[b] = true
		out = append(out, b)
	}
	return out
}

// HasAccessBackend reports whether the cluster's effective backends include the
// given backend ("acl" or "rbac").
func HasAccessBackend(c *KafkaCluster, backend string) bool {
	for _, b := range EffectiveAccessBackends(c) {
		if b == backend {
			return true
		}
	}
	return false
}

// MDSConfig is the Confluent Metadata Service (RBAC) connection. Scope cluster
// IDs are configured explicitly (auto-discovery deferred). KafkaCluster id is
// required; the others are needed only for their respective scope types (§40).
type MDSConfig struct {
	Endpoint string      `json:"endpoint"`
	Auth     *MDSAuth    `json:"auth,omitempty"`
	TLS      *TLSConfig  `json:"tls,omitempty"`
	Clusters MDSClusters `json:"clusters"`
}

// MDSAuth authenticates to MDS. Type selects the method; credentials resolve via
// the secret machinery (basic: Username/Password; bearer: Token; mtls: TLS client cert).
type MDSAuth struct {
	// +kubebuilder:validation:Enum=basic;bearer;mtls
	Type     string     `json:"type"`
	Username *ValueFrom `json:"username,omitempty"`
	Password *ValueFrom `json:"password,omitempty"`
	Token    *ValueFrom `json:"token,omitempty"`
}

// MDSClusters holds the scope cluster IDs for MDS role bindings. kafkaCluster is the
// base scope id required on every binding; the sub-cluster ids (schemaRegistryCluster,
// connectCluster, ksqlCluster) are required only for their respective scope.type values.
type MDSClusters struct {
	KafkaCluster          string `json:"kafkaCluster"`
	SchemaRegistryCluster string `json:"schemaRegistryCluster,omitempty"`
	ConnectCluster        string `json:"connectCluster,omitempty"`
	KsqlCluster           string `json:"ksqlCluster,omitempty"`
}

type KafkaClusterStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	LastCheckedTime    *metav1.Time       `json:"lastCheckedTime,omitempty"`
}

type TLSConfig struct {
	Enabled bool `json:"enabled"`
	// CACert is the PEM of the CA (bundle) to trust for server verification,
	// resolved like any other secret value: file/env/inline in CLI mode,
	// secretKeyRef in operator mode. Nil = system trust store.
	CACert             *ValueFrom `json:"caCert,omitempty"`
	ClientCert         *ValueFrom `json:"clientCert,omitempty"`
	ClientKey          *ValueFrom `json:"clientKey,omitempty"`
	InsecureSkipVerify bool       `json:"insecureSkipVerify,omitempty"`
}

type AuthConfig struct {
	// +kubebuilder:validation:Enum=None;PLAIN;SCRAM-SHA-256;SCRAM-SHA-512;OAUTHBEARER;mTLS
	Mechanism string       `json:"mechanism"`
	SCRAM     *SCRAMAuth   `json:"scram,omitempty"`
	OAuth     *OAuthConfig `json:"oauth,omitempty"`
}

// OAuthConfig is the OIDC client-credentials flow (spec §4.5): the client
// fetches bearer tokens from TokenEndpoint with ClientID/ClientSecret.
type OAuthConfig struct {
	TokenEndpoint string    `json:"tokenEndpoint"`
	ClientID      ValueFrom `json:"clientId"`
	ClientSecret  ValueFrom `json:"clientSecret"`
	Scope         string    `json:"scope,omitempty"`
	// TokenEndpointCA is the PEM CA (bundle) to trust when connecting to the
	// IdP's TokenEndpoint over TLS, resolved like any other secret value:
	// file/env/inline in CLI mode, secretKeyRef in operator mode. The IdP is a
	// DIFFERENT trust domain than the Kafka brokers: this is intentionally
	// separate from (and never falls back to) spec.tls.caCert, since a
	// private-CA broker cluster and a private-CA IdP are not necessarily
	// signed by the same authority. Nil = system trust store (the default
	// http transport), matching today's behavior.
	TokenEndpointCA *ValueFrom `json:"tokenEndpointCA,omitempty"`
}

type SCRAMAuth struct {
	Username ValueFrom `json:"username"`
	Password ValueFrom `json:"password"`
}

type SchemaRegistryConf struct {
	Endpoint string              `json:"endpoint"`
	Auth     *SchemaRegistryAuth `json:"auth,omitempty"`
	// TLS configures the HTTPS connection to the Schema Registry (custom CA,
	// optional client cert, dev-only insecureSkipVerify). Same shape as
	// spec.tls and authorization.mds.tls.
	TLS *TLSConfig `json:"tls,omitempty"`
}

type SchemaRegistryAuth struct {
	Type     string    `json:"type,omitempty"` // e.g. "basic"
	Username ValueFrom `json:"username,omitempty"`
	Password ValueFrom `json:"password,omitempty"`
}

type ClusterDefaults struct {
	ReplicationFactor   *int   `json:"replicationFactor,omitempty"`
	TopicDeletionPolicy string `json:"topicDeletionPolicy,omitempty"`
}

// TenancyConfig restricts which namespaces may reference this cluster and
// which topic-name prefixes each namespace may manage (spec §20.2).
type TenancyConfig struct {
	// AllowedNamespaces: glob patterns (path.Match syntax); empty = any
	// namespace may reference this cluster.
	AllowedNamespaces []string          `json:"allowedNamespaces,omitempty"`
	TopicPrefixes     []TopicPrefixRule `json:"topicPrefixes,omitempty"`
}

// TopicPrefixRule: namespaces matching any glob in Namespaces may only manage
// topic names starting with one of Prefixes. Namespaces matched by no rule
// are unrestricted.
type TopicPrefixRule struct {
	Namespaces []string `json:"namespaces"`
	Prefixes   []string `json:"prefixes"`
}

// ---- KafkaTopic ----

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=".spec.clusterRef.name"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

type KafkaTopic struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              KafkaTopicSpec    `json:"spec,omitempty"`
	Status            *KafkaTopicStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type KafkaTopicList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaTopic `json:"items"`
}

// KafkaTopicSpec carries CEL transition rules (x-kubernetes-validations) that
// enforce identity immutability AT THE APISERVER, so the default install
// (webhooks disabled) cannot silently orphan broker state by renaming identity
// fields. The rules reference oldSelf, so they run only on UPDATE — create is
// unrestricted. topicName is immutable ONCE SET: an update from unset/empty to
// set is allowed (defaulting resolves topicName from metadata.name in-memory,
// so making the default explicit later must not be rejected). Note the CEL rule
// cannot compare against metadata.name from spec scope, so unset→set admits any
// value; the webhook (when enabled) additionally rejects an unset→set update
// whose value differs from metadata.name (resolved-name comparison). The
// !has() disjunct is defensive for a future pointer/optional refactor of
// TopicName; today the field is a plain string, so the .size() == 0 disjunct
// is what fires for never-set values. CEL messages are static (no old/new
// value interpolation) — the webhook supplies the richer old -> new messages.
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.topicName) || oldSelf.topicName.size() == 0 || (has(self.topicName) && self.topicName == oldSelf.topicName)",message="spec.topicName is immutable once set (a rename is a delete + create of a different Kafka topic)"
// +kubebuilder:validation:XValidation:rule="self.clusterRef.name == oldSelf.clusterRef.name",message="spec.clusterRef.name is immutable (repointing a topic orphans state on the previous cluster)"
type KafkaTopicSpec struct {
	ClusterRef ClusterRef `json:"clusterRef"`
	TopicName  string     `json:"topicName,omitempty"`
	// +kubebuilder:validation:Minimum=1
	Partitions int `json:"partitions"`
	// +kubebuilder:validation:Minimum=1
	ReplicationFactor *int              `json:"replicationFactor,omitempty"`
	Config            map[string]string `json:"config,omitempty"`
	Access            TopicAccess       `json:"access,omitempty"`
	Schema            *TopicSchema      `json:"schema,omitempty"`
	Reconciliation    *Reconciliation   `json:"reconciliation,omitempty"`
	Drift             *DriftConfig      `json:"drift,omitempty"`
	// Prune opts this resource's managed ACL scope into pruning (spec §10.3):
	// in-scope live ACLs no longer desired are deleted only when every
	// resource whose scope covers them sets prune: true. Default false —
	// prune candidates are reported as drift but never deleted.
	Prune bool `json:"prune,omitempty"`
	// +kubebuilder:validation:Enum=Orphan;Delete
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
}

type KafkaTopicStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	ObservedTopic      *ObservedTopic     `json:"observedTopic,omitempty"`
	ObservedAccess     *ObservedAccess    `json:"observedAccess,omitempty"`
	Schema             *ObservedSchema    `json:"schema,omitempty"`
	Drift              *DriftStatus       `json:"drift,omitempty"`
	LastCheckedTime    *metav1.Time       `json:"lastCheckedTime,omitempty"`
	LastAppliedTime    *metav1.Time       `json:"lastAppliedTime,omitempty"`
}

type ObservedTopic struct {
	TopicName         string            `json:"topicName,omitempty"`
	Partitions        int               `json:"partitions,omitempty"`
	ReplicationFactor int               `json:"replicationFactor,omitempty"`
	Config            map[string]string `json:"config,omitempty"`
}

type ObservedAccess struct {
	Producers []ProducerAccess `json:"producers,omitempty"`
	Consumers []ConsumerAccess `json:"consumers,omitempty"`
}

type ObservedSchema struct {
	ValueSubject  string `json:"valueSubject,omitempty"`
	ValueSchemaID int    `json:"valueSchemaId,omitempty"`
	Compatibility string `json:"compatibility,omitempty"`
}

type DriftStatus struct {
	Detected bool     `json:"detected"`
	Fields   []string `json:"fields,omitempty"`
}

type TopicAccess struct {
	Producers []ProducerAccess `json:"producers,omitempty"`
	Consumers []ConsumerAccess `json:"consumers,omitempty"`
}

type ProducerAccess struct {
	Principal string `json:"principal"`
	// Host restricts the ACL to a source host (default "*" = all hosts). Spec §8.4.
	Host       string   `json:"host,omitempty"`
	Operations []string `json:"operations,omitempty"`
}

type ConsumerAccess struct {
	Principal string `json:"principal"`
	// Host restricts the topic+group ACLs to a source host (default "*" = all hosts). Spec §8.4.
	Host            string   `json:"host,omitempty"`
	Group           string   `json:"group"`
	TopicOperations []string `json:"topicOperations,omitempty"`
	GroupOperations []string `json:"groupOperations,omitempty"`
}

type TopicSchema struct {
	// +kubebuilder:validation:Enum=AVRO;JSON;PROTOBUF
	Format string `json:"format"`
	// +kubebuilder:validation:Enum=TopicName;RecordName;TopicRecordName;Custom
	SubjectStrategy string `json:"subjectStrategy,omitempty"`
	// +kubebuilder:validation:Enum=NONE;BACKWARD;BACKWARD_TRANSITIVE;FORWARD;FORWARD_TRANSITIVE;FULL;FULL_TRANSITIVE
	Compatibility string     `json:"compatibility,omitempty"`
	ValueSchema   *ValueFrom `json:"valueSchema,omitempty"`
	KeySchema     *ValueFrom `json:"keySchema,omitempty"`
	// ValueSubject names the value schema subject verbatim when SubjectStrategy is Custom.
	ValueSubject string `json:"valueSubject,omitempty"`
	// KeySubject names the key schema subject verbatim when SubjectStrategy is Custom.
	KeySubject string `json:"keySubject,omitempty"`
}

type Reconciliation struct {
	// +kubebuilder:validation:Enum=Enforce;DetectOnly;ObserveOnly
	Mode string `json:"mode,omitempty"`
}

type DriftConfig struct {
	IgnoreFields []string `json:"ignoreFields,omitempty"`
}

// ---- KafkaAccessPolicy ----

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=".spec.clusterRef.name"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

type KafkaAccessPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              KafkaAccessPolicySpec    `json:"spec,omitempty"`
	Status            *KafkaAccessPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type KafkaAccessPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaAccessPolicy `json:"items"`
}

// KafkaAccessPolicySpec carries a CEL transition rule enforcing clusterRef
// immutability at the apiserver (always on, webhooks or not): repointing a
// policy to a different cluster orphans the ACLs applied on the previous one.
// +kubebuilder:validation:XValidation:rule="self.clusterRef.name == oldSelf.clusterRef.name",message="spec.clusterRef.name is immutable (repointing a policy orphans ACLs on the previous cluster)"
type KafkaAccessPolicySpec struct {
	ClusterRef     ClusterRef      `json:"clusterRef"`
	Rules          []ACLRule       `json:"rules"`
	Reconciliation *Reconciliation `json:"reconciliation,omitempty"`
	// Prune opts this policy's managed ACL scope into pruning (spec §10.3):
	// in-scope live ACLs no longer desired are deleted only when every
	// resource whose scope covers them sets prune: true. Default false —
	// prune candidates are reported as drift but never deleted.
	Prune bool `json:"prune,omitempty"`
	// +kubebuilder:validation:Enum=Orphan;Delete
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
}

type KafkaAccessPolicyStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	ObservedRules      []ACLRule          `json:"observedRules,omitempty"`
	Drift              *DriftStatus       `json:"drift,omitempty"`
	LastCheckedTime    *metav1.Time       `json:"lastCheckedTime,omitempty"`
	LastAppliedTime    *metav1.Time       `json:"lastAppliedTime,omitempty"`
}

type ACLRule struct {
	Principal string `json:"principal"`
	// +kubebuilder:validation:Enum=Allow;Deny
	Permission string      `json:"permission,omitempty"`
	Host       string      `json:"host,omitempty"`
	Resource   ACLResource `json:"resource"`
	Operations []string    `json:"operations"`
}

type ACLResource struct {
	// +kubebuilder:validation:Enum=topic;group;cluster;transactionalId;delegationToken
	Type string `json:"type"`
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=literal;prefixed
	PatternType string `json:"patternType,omitempty"`
}

// ---- KafkaQuota ----

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=".spec.clusterRef.name"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

type KafkaQuota struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              KafkaQuotaSpec    `json:"spec,omitempty"`
	Status            *KafkaQuotaStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type KafkaQuotaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaQuota `json:"items"`
}

// KafkaQuotaSpec carries CEL transition rules enforcing entity and clusterRef
// immutability at the apiserver (always on, webhooks or not): changing the
// entity orphans the previous entity's quota (a delete + create of a
// different Kafka quota entity), and repointing clusterRef orphans the quota
// on the previous cluster. CEL object equality compares the whole entity
// block field-wise, mirroring the webhook's resolved-entity-key comparison
// (all components are scalars).
// +kubebuilder:validation:XValidation:rule="self.entity == oldSelf.entity",message="spec.entity is immutable (changing it orphans the previous entity's quota)"
// +kubebuilder:validation:XValidation:rule="self.clusterRef.name == oldSelf.clusterRef.name",message="spec.clusterRef.name is immutable (repointing a quota orphans the previous cluster's quota)"
type KafkaQuotaSpec struct {
	ClusterRef     ClusterRef      `json:"clusterRef"`
	Entity         QuotaEntity     `json:"entity"`
	Limits         QuotaLimits     `json:"limits"`
	Reconciliation *Reconciliation `json:"reconciliation,omitempty"`
}

// QuotaEntity identifies a Kafka quota target. It is either an ip dimension
// (ip or ipDefault) OR a user/client-id dimension — never both. A specific name
// XOR its per-type default; at least one component required (spec §39.2).
type QuotaEntity struct {
	// User in KafkaPrincipal form "User:<name>" (the "User:" prefix is stripped
	// for Kafka's quota API). Mutually exclusive with userDefault.
	User string `json:"user,omitempty"`
	// ClientId is a specific client-id. Mutually exclusive with clientIdDefault.
	ClientId string `json:"clientId,omitempty"`
	// UserDefault targets the user-default entity (Kafka null name).
	UserDefault bool `json:"userDefault,omitempty"`
	// ClientIdDefault targets the client-id-default entity (Kafka null name).
	ClientIdDefault bool `json:"clientIdDefault,omitempty"`
	// Ip targets a Kafka ip quota entity by a single IPv4/IPv6 literal. It is a
	// SEPARATE quota dimension: mutually exclusive with ipDefault AND with every
	// user/client-id component (an entity is either ip or user/client-id, never
	// both). Validated as a net.ParseIP literal (no CIDR/hostname). (spec §39.2)
	Ip string `json:"ip,omitempty"`
	// IpDefault targets the ip-default entity (Kafka null name).
	IpDefault bool `json:"ipDefault,omitempty"`
}

// QuotaLimits are the five Kafka quota value keys. At least one required;
// authoritative for the entity (an unset dimension present live is removed).
type QuotaLimits struct {
	// ProducerByteRate caps produce throughput in bytes/second.
	// +kubebuilder:validation:Minimum=0
	ProducerByteRate *float64 `json:"producerByteRate,omitempty"`
	// ConsumerByteRate caps fetch throughput in bytes/second.
	// +kubebuilder:validation:Minimum=0
	ConsumerByteRate *float64 `json:"consumerByteRate,omitempty"`
	// RequestPercentage caps broker request-handler time as a percent (may exceed 100 on multi-core brokers).
	// +kubebuilder:validation:Minimum=0
	RequestPercentage *float64 `json:"requestPercentage,omitempty"`
	// ControllerMutationRate caps the create/delete topic+partition mutation rate (mutations/second).
	// +kubebuilder:validation:Minimum=0
	ControllerMutationRate *float64 `json:"controllerMutationRate,omitempty"`
	// ConnectionCreationRate caps new connections/second. It is valid ONLY on an
	// ip entity (and is the only limit valid there); rejected on user/client-id
	// entities (spec §39.3).
	// +kubebuilder:validation:Minimum=0
	ConnectionCreationRate *float64 `json:"connectionCreationRate,omitempty"`
}

type KafkaQuotaStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	ObservedLimits     *QuotaLimits       `json:"observedLimits,omitempty"`
	Drift              *DriftStatus       `json:"drift,omitempty"`
	LastCheckedTime    *metav1.Time       `json:"lastCheckedTime,omitempty"`
	LastAppliedTime    *metav1.Time       `json:"lastAppliedTime,omitempty"`
}

// ---- KafkaRoleBinding ----

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=".spec.clusterRef.name"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

type KafkaRoleBinding struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              KafkaRoleBindingSpec    `json:"spec,omitempty"`
	Status            *KafkaRoleBindingStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type KafkaRoleBindingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaRoleBinding `json:"items"`
}

// KafkaRoleBindingSpec carries CEL transition rules enforcing the MDS binding
// identity set's immutability at the apiserver (always on, webhooks or not).
// The set mirrors the webhook exactly: clusterRef.name, principal, role, and
// scope.type — changing any of these orphans the previous MDS bindings.
// spec.resources changes ARE allowed (the reconciler converges the set).
// +kubebuilder:validation:XValidation:rule="self.clusterRef.name == oldSelf.clusterRef.name",message="spec.clusterRef.name is immutable (changing it orphans the previous MDS bindings)"
// +kubebuilder:validation:XValidation:rule="self.principal == oldSelf.principal",message="spec.principal is immutable (changing it orphans the previous MDS bindings)"
// +kubebuilder:validation:XValidation:rule="self.role == oldSelf.role",message="spec.role is immutable (changing it orphans the previous MDS bindings)"
// +kubebuilder:validation:XValidation:rule="self.scope.type == oldSelf.scope.type",message="spec.scope.type is immutable (changing it orphans the previous MDS bindings)"
type KafkaRoleBindingSpec struct {
	ClusterRef     ClusterRef       `json:"clusterRef"`
	Principal      string           `json:"principal"`
	Role           string           `json:"role"`
	Scope          RoleBindingScope `json:"scope"`
	Resources      []RoleResource   `json:"resources,omitempty"`
	Reconciliation *Reconciliation  `json:"reconciliation,omitempty"`
	Prune          bool             `json:"prune,omitempty"`
	// DeletionPolicy defaults to Delete (see internal/defaulting): the compiled
	// MDS role bindings are the entire reason this resource exists, so removing
	// them when the resource is deleted is the expected behavior. Set Orphan to
	// leave the live MDS role bindings in place on deletion.
	// +kubebuilder:validation:Enum=Orphan;Delete
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
}

type RoleBindingScope struct {
	// +kubebuilder:validation:Enum=kafka;schema-registry;connect;ksql
	Type string `json:"type"`
}

type RoleResource struct {
	Type string `json:"type"`
	Name string `json:"name"`
	// +kubebuilder:validation:Enum=literal;prefixed
	PatternType string `json:"patternType,omitempty"`
}

// KafkaRoleBindingStatus is the observed state of a KafkaRoleBinding.
// Note: Drift *DriftStatus is intentionally omitted — MDS role bindings are
// managed as an authoritative set, so field-level drift is not separately classified.
type KafkaRoleBindingStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	ObservedResources  []RoleResource     `json:"observedResources,omitempty"`
	LastCheckedTime    *metav1.Time       `json:"lastCheckedTime,omitempty"`
	LastAppliedTime    *metav1.Time       `json:"lastAppliedTime,omitempty"`
}

// ---- KafkaUser ----

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=".spec.clusterRef.name"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Username",type=string,JSONPath=".spec.username"
// +kubebuilder:printcolumn:name="Mechanism",type=string,JSONPath=".spec.mechanism"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"

type KafkaUser struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              KafkaUserSpec    `json:"spec,omitempty"`
	Status            *KafkaUserStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type KafkaUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaUser `json:"items"`
}

// KafkaUserSpec carries CEL transition rules (x-kubernetes-validations) that
// enforce identity immutability AT THE APISERVER, so the default install
// (webhooks disabled) cannot silently orphan broker state by renaming identity
// fields. The rules reference oldSelf, so they run only on UPDATE — create is
// unrestricted. username is immutable ONCE SET: an update from unset/empty to
// set is allowed (defaulting resolves username from metadata.name in-memory,
// so making the default explicit later must not be rejected). Note the CEL
// rule cannot compare against metadata.name from spec scope, so unset→set
// admits any value; the webhook (when enabled) additionally rejects an
// unset→set update whose value differs from metadata.name (resolved-name
// comparison). The !has() disjunct is defensive for a future pointer/optional
// refactor of Username; today the field is a plain string, so the
// .size() == 0 disjunct is what fires for never-set values. CEL messages are
// static (no old/new value interpolation) — the webhook supplies the richer
// old -> new messages.
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.username) || oldSelf.username.size() == 0 || (has(self.username) && self.username == oldSelf.username)",message="spec.username is immutable once set (a rename is a delete + create of a different principal)"
// +kubebuilder:validation:XValidation:rule="self.clusterRef.name == oldSelf.clusterRef.name",message="spec.clusterRef.name is immutable (repointing a user orphans the previous cluster's credential)"
type KafkaUserSpec struct {
	ClusterRef ClusterRef `json:"clusterRef"`
	// Username defaults from metadata.name when empty (defaulting webhook / CLI defaulting package).
	Username string `json:"username,omitempty"`
	// Mechanism defaults to SCRAM-SHA-512 when empty (see internal/defaulting).
	// +kubebuilder:validation:Enum=SCRAM-SHA-256;SCRAM-SHA-512
	Mechanism string `json:"mechanism,omitempty"`
	// Iterations is the SCRAM iteration count. Nil means "use the Kafka broker
	// default" and is NOT drift-compared (an unset value never triggers drift
	// just because the broker reports its own default back).
	// +kubebuilder:validation:Minimum=4096
	// +kubebuilder:validation:Maximum=16384
	Iterations *int32 `json:"iterations,omitempty"`
	// Password is required; validation enforces its presence (kept as a
	// pointer so a missing/malformed block produces a clear shape error
	// rather than a silently-empty struct).
	Password *UserPassword `json:"password,omitempty"`
	// DeletionPolicy defaults to Delete (see internal/defaulting): the
	// credential is this CR's entire reason to exist, so orphaning it on
	// delete is the unusual case, unlike topics/ACLs.
	// +kubebuilder:validation:Enum=Delete;Orphan
	DeletionPolicy string `json:"deletionPolicy,omitempty"`
}

// UserPassword selects exactly one password source. ValueFrom names an
// existing/externally-managed secret value (operator: secretKeyRef; CLI:
// env|file|inline), giving the manifest shape `password: {valueFrom: {...}}}` —
// deliberately the SAME ValueSource shape used everywhere else in the API
// (spec.tls.caCert, scram.password, etc.), for consistency: a referenced
// password always reads as `{valueFrom: {...}}` regardless of which field it's
// under. Generate is operator-only: the operator creates and owns a Secret
// named "<name>-kafka-credentials" holding a generated password, shape
// `password: {generate: {}}`.
type UserPassword struct {
	ValueFrom *ValueSource      `json:"valueFrom,omitempty"`
	Generate  *GeneratePassword `json:"generate,omitempty"`
}

// GeneratePassword is currently empty (future: length/charset knobs).
type GeneratePassword struct{}

// KafkaUserStatus is the observed state of a KafkaUser.
type KafkaUserStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	LastCheckedTime    *metav1.Time       `json:"lastCheckedTime,omitempty"`
	LastAppliedTime    *metav1.Time       `json:"lastAppliedTime,omitempty"`
	// AppliedPasswordRef records the Secret+ResourceVersion whose value was last
	// upserted to Kafka (referenced/valueFrom mode only).
	AppliedPasswordRef *AppliedPasswordRef `json:"appliedPasswordRef,omitempty"`
	// GeneratedSecretName is set in generate mode: the name of the
	// operator-owned Secret holding the generated password.
	GeneratedSecretName string `json:"generatedSecretName,omitempty"`
}

// AppliedPasswordRef identifies the Secret+ResourceVersion whose password
// value was last upserted to Kafka.
type AppliedPasswordRef struct {
	SecretName      string `json:"secretName,omitempty"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
}
