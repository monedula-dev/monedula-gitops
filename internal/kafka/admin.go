package kafka

import "context"

// TopicState is the observed or desired state of a Kafka topic.
type TopicState struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Config            map[string]string
}

// ACLState is the observed or desired state of a single Kafka ACL binding.
type ACLState struct {
	Principal, Host, ResourceType, ResourceName, PatternType, Operation, Permission string
}

// ConfigEntry is a single topic configuration key/value, annotated with whether
// the value is an inherited/broker default (Default=true) rather than a value
// explicitly set on the topic (Default=false). Import uses this to write only
// explicitly-set configs into manifests.
type ConfigEntry struct {
	Name    string
	Value   string
	Default bool // true if the value is an inherited/broker default (not explicitly set on the topic)
}

// TopicSpec is the desired shape of a topic to create.
type TopicSpec struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Config            map[string]string
}

// QuotaEntityComponent mirrors a Kafka quota entity component (Name nil = the
// per-type default).
type QuotaEntityComponent struct {
	Type string  // "user" | "client-id" | "ip"
	Name *string // nil = default
}

// QuotaState is an observed client quota: its entity and value map.
type QuotaState struct {
	Entity []QuotaEntityComponent
	Limits map[string]float64
}

// ScramCredential is the observable identity of a single SCRAM credential:
// exactly the triple Kafka's DescribeUserSCRAMCredentials API exposes. Kafka
// never returns the password (or salted password) over this API, so this
// type has no password field by construction — there is nothing to leak.
// Mirrors internal/user.Credential; kept as a distinct, kafka-layer-owned
// type following the same precedent as QuotaState/QuotaEntityComponent (the
// kafka package defines its own small domain types rather than importing a
// higher-level package), so internal/kafka has no dependency on internal/user.
type ScramCredential struct {
	User       string
	Mechanism  string // canonical enum: "SCRAM-SHA-256" | "SCRAM-SHA-512"
	Iterations int32
}

// ScramUpsert is the desired shape of a SCRAM credential write. Password is
// write-only: it is never echoed back by ListScramCredentials, must never
// appear in an error or log line, and is discarded once the adapter/mock has
// used it to establish the credential.
type ScramUpsert struct {
	User       string
	Mechanism  string // canonical enum: "SCRAM-SHA-256" | "SCRAM-SHA-512"
	Iterations int32  // 0 = use the adapter's default (see franz.defaultScramIterations)
	Password   string
}

// AdminClient is the seam over a Kafka cluster. v0.1 used only the read
// methods; v0.2 adds mutations driven by the apply executor. The mock and the
// real franz-go client (v0.2 Task 4) both implement this interface.
type AdminClient interface {
	GetTopic(ctx context.Context, name string) (*TopicState, error)
	ListTopics(ctx context.Context) ([]TopicState, error)
	ListACLs(ctx context.Context) ([]ACLState, error)
	DescribeTopicConfigs(ctx context.Context, topic string) ([]ConfigEntry, error)

	CreateTopic(ctx context.Context, t TopicSpec) error
	UpdateTopicConfig(ctx context.Context, topic string, set map[string]string) error
	CreatePartitions(ctx context.Context, topic string, count int) error
	DeleteTopic(ctx context.Context, topic string) error
	CreateACLs(ctx context.Context, acls []ACLState) error
	DeleteACLs(ctx context.Context, acls []ACLState) error

	ListQuotas(ctx context.Context) ([]QuotaState, error)
	// SetQuota sets the given limit keys on the entity (merge; does not remove others).
	SetQuota(ctx context.Context, entity []QuotaEntityComponent, limits map[string]float64) error
	// DeleteQuota removes the given limit keys from the entity.
	DeleteQuota(ctx context.Context, entity []QuotaEntityComponent, keys []string) error

	// ListScramCredentials returns the observable SCRAM credential identities
	// for the given usernames (all credentialed users when usernames is
	// empty). Passwords are never returned — Kafka only exposes
	// (user, mechanism, iterations). A requested-but-absent user is simply
	// missing from the result, not an error.
	ListScramCredentials(ctx context.Context, usernames ...string) ([]ScramCredential, error)
	// UpsertScramCredential creates or updates the credential for
	// (u.User, u.Mechanism). u.Password is write-only and MUST NEVER appear
	// in a returned error or be logged.
	UpsertScramCredential(ctx context.Context, u ScramUpsert) error
	// DeleteScramCredential removes only the (username, mechanism) credential;
	// any other mechanism the user has configured is untouched.
	DeleteScramCredential(ctx context.Context, username, mechanism string) error
}
