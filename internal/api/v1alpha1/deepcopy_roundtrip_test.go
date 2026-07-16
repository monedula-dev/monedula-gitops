package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestKafkaClusterV07DeepCopyRoundTrip verifies DeepCopy independence for the
// new v0.7 fields: OAuthConfig, TLS client certs (ValueFrom), and TenancyConfig.
func TestKafkaClusterV07DeepCopyRoundTrip(t *testing.T) {
	rf := 3
	orig := &KafkaCluster{
		Spec: KafkaClusterSpec{
			BootstrapServers: "broker:9092",
			Auth: &AuthConfig{
				Mechanism: "OAUTHBEARER",
				OAuth: &OAuthConfig{
					TokenEndpoint: "https://idp.example.com/token",
					ClientID: ValueFrom{
						ValueFrom: ValueSource{
							SecretKeyRef: &SecretKeyRef{Name: "oauth-secret", Key: "client-id"},
						},
					},
					ClientSecret: ValueFrom{
						ValueFrom: ValueSource{
							SecretKeyRef: &SecretKeyRef{Name: "oauth-secret", Key: "client-secret"},
						},
					},
					Scope: "kafka",
				},
			},
			TLS: &TLSConfig{
				Enabled: true,
				CACert: &ValueFrom{
					ValueFrom: ValueSource{
						SecretKeyRef: &SecretKeyRef{Name: "tls-ca", Key: "ca.crt"},
					},
				},
				ClientCert: &ValueFrom{
					ValueFrom: ValueSource{
						SecretKeyRef: &SecretKeyRef{Name: "tls-client", Key: "tls.crt"},
					},
				},
				ClientKey: &ValueFrom{
					ValueFrom: ValueSource{
						SecretKeyRef: &SecretKeyRef{Name: "tls-client", Key: "tls.key"},
					},
				},
			},
			Tenancy: &TenancyConfig{
				AllowedNamespaces: []string{"team-a", "team-b"},
				TopicPrefixes: []TopicPrefixRule{
					{Namespaces: []string{"team-a"}, Prefixes: []string{"a.", "shared."}},
					{Namespaces: []string{"team-b"}, Prefixes: []string{"b."}},
				},
			},
			Defaults: &ClusterDefaults{
				ReplicationFactor: &rf,
			},
		},
	}

	cp := orig.DeepCopy()

	require.Equal(t, orig, cp, "DeepCopy result must equal original")

	// Mutate copy — original must not be affected.
	cp.Spec.Auth.OAuth.Scope = "mutated"
	cp.Spec.Auth.OAuth.ClientID.ValueFrom.SecretKeyRef.Key = "mutated"
	cp.Spec.TLS.ClientCert.ValueFrom.SecretKeyRef.Key = "mutated"
	cp.Spec.TLS.ClientKey.ValueFrom.SecretKeyRef.Key = "mutated"
	cp.Spec.Tenancy.AllowedNamespaces[0] = "mutated"
	cp.Spec.Tenancy.TopicPrefixes[0].Namespaces[0] = "mutated"
	cp.Spec.Tenancy.TopicPrefixes[0].Prefixes[0] = "mutated"

	require.Equal(t, "kafka", orig.Spec.Auth.OAuth.Scope, "OAuth.Scope must not be corrupted")
	require.Equal(t, "client-id", orig.Spec.Auth.OAuth.ClientID.ValueFrom.SecretKeyRef.Key, "OAuth.ClientID.SecretKeyRef.Key must not be corrupted")
	require.Equal(t, "tls.crt", orig.Spec.TLS.ClientCert.ValueFrom.SecretKeyRef.Key, "TLS.ClientCert.SecretKeyRef.Key must not be corrupted")
	require.Equal(t, "tls.key", orig.Spec.TLS.ClientKey.ValueFrom.SecretKeyRef.Key, "TLS.ClientKey.SecretKeyRef.Key must not be corrupted")
	require.Equal(t, "team-a", orig.Spec.Tenancy.AllowedNamespaces[0], "Tenancy.AllowedNamespaces[0] must not be corrupted")
	require.Equal(t, "team-a", orig.Spec.Tenancy.TopicPrefixes[0].Namespaces[0], "Tenancy.TopicPrefixes[0].Namespaces[0] must not be corrupted")
	require.Equal(t, "a.", orig.Spec.Tenancy.TopicPrefixes[0].Prefixes[0], "Tenancy.TopicPrefixes[0].Prefixes[0] must not be corrupted")
}

// TestKafkaTopicV08HostDeepCopyRoundTrip verifies DeepCopy preserves the new
// optional host field on ProducerAccess and ConsumerAccess (spec §8.4).
func TestKafkaTopicV08HostDeepCopyRoundTrip(t *testing.T) {
	orig := &KafkaTopic{
		Spec: KafkaTopicSpec{
			ClusterRef: ClusterRef{Name: "prod"},
			Partitions: 3,
			Access: TopicAccess{
				Producers: []ProducerAccess{
					{
						Principal:  "User:producer-svc",
						Host:       "10.0.0.1",
						Operations: []string{"Write"},
					},
				},
				Consumers: []ConsumerAccess{
					{
						Principal:       "User:consumer-svc",
						Host:            "10.0.0.2",
						Group:           "consumer-group",
						TopicOperations: []string{"Read", "Describe"},
						GroupOperations: []string{"Read"},
					},
				},
			},
		},
	}

	cp := orig.DeepCopy()

	require.Equal(t, orig, cp, "DeepCopy result must equal original")

	// Mutate copy — original must not be affected (scalar fields, aliasing via slice backing array).
	cp.Spec.Access.Producers[0].Host = "mutated"
	cp.Spec.Access.Consumers[0].Host = "mutated"
	cp.Spec.Access.Producers[0].Operations[0] = "mutated"
	cp.Spec.Access.Consumers[0].TopicOperations[0] = "mutated"

	require.Equal(t, "10.0.0.1", orig.Spec.Access.Producers[0].Host, "ProducerAccess.Host must not be corrupted")
	require.Equal(t, "10.0.0.2", orig.Spec.Access.Consumers[0].Host, "ConsumerAccess.Host must not be corrupted")
	require.Equal(t, "Write", orig.Spec.Access.Producers[0].Operations[0], "ProducerAccess.Operations must not be corrupted")
	require.Equal(t, "Read", orig.Spec.Access.Consumers[0].TopicOperations[0], "ConsumerAccess.TopicOperations must not be corrupted")
}

// TestKafkaTopicV07DeepCopyRoundTrip verifies DeepCopy independence for the
// new v0.7 fields: inline/configMapKeyRef ValueSource extensions and
// valueSubject/keySubject on TopicSchema.
func TestKafkaTopicV07DeepCopyRoundTrip(t *testing.T) {
	orig := &KafkaTopic{
		Spec: KafkaTopicSpec{
			ClusterRef: ClusterRef{Name: "prod"},
			Partitions: 6,
			Schema: &TopicSchema{
				Format:          "AVRO",
				SubjectStrategy: "Custom",
				ValueSubject:    "payments.orders-value",
				KeySubject:      "payments.orders-key",
				ValueSchema: &ValueFrom{
					ValueFrom: ValueSource{
						// Inline is a value type — mutating the copy's string cannot alias
						// the original. Real aliasing protection comes from the pointer/slice
						// fields (SecretKeyRef, ConfigMapKeyRef, slices); use those when
						// adding new fields that need deep-copy bite.
						Inline: `{"type":"record","name":"Order","fields":[]}`,
					},
				},
				KeySchema: &ValueFrom{
					ValueFrom: ValueSource{
						ConfigMapKeyRef: &SecretKeyRef{Name: "schemas-cm", Key: "order-key.avsc"},
					},
				},
			},
		},
	}

	cp := orig.DeepCopy()

	require.Equal(t, orig, cp, "DeepCopy result must equal original")

	// Mutate copy — original must not be affected.
	cp.Spec.Schema.ValueSubject = "mutated"
	cp.Spec.Schema.KeySubject = "mutated"
	cp.Spec.Schema.ValueSchema.ValueFrom.Inline = "mutated"
	cp.Spec.Schema.KeySchema.ValueFrom.ConfigMapKeyRef.Name = "mutated"

	require.Equal(t, "payments.orders-value", orig.Spec.Schema.ValueSubject, "Schema.ValueSubject must not be corrupted")
	require.Equal(t, "payments.orders-key", orig.Spec.Schema.KeySubject, "Schema.KeySubject must not be corrupted")
	// Inline is a value type; the assertion below confirms basic copy but cannot catch
	// a shallow-copy bug. Aliasing protection for this field comes from pointer fields above.
	require.Equal(t, `{"type":"record","name":"Order","fields":[]}`, orig.Spec.Schema.ValueSchema.ValueFrom.Inline, "Schema.ValueSchema.Inline must not be corrupted")
	require.Equal(t, "schemas-cm", orig.Spec.Schema.KeySchema.ValueFrom.ConfigMapKeyRef.Name, "Schema.KeySchema.ConfigMapKeyRef.Name must not be corrupted")
}

// TestKafkaRoleBindingV013DeepCopyRoundTrip verifies DeepCopy independence for
// the new KafkaRoleBinding type (spec §40): Resources slice independence,
// Reconciliation pointer independence, and Status pointer independence.
func TestKafkaRoleBindingV013DeepCopyRoundTrip(t *testing.T) {
	orig := &KafkaRoleBinding{
		Spec: KafkaRoleBindingSpec{
			ClusterRef: ClusterRef{Name: "prod"},
			Principal:  "User:svc-account",
			Role:       "ResourceOwner",
			Scope:      RoleBindingScope{Type: "kafka"},
			Resources: []RoleResource{
				{Type: "Topic", Name: "payments.", PatternType: "prefixed"},
				{Type: "Group", Name: "payments-consumers", PatternType: "literal"},
			},
			Reconciliation: &Reconciliation{Mode: "Enforce"},
			Prune:          true,
			DeletionPolicy: "Orphan",
		},
		Status: &KafkaRoleBindingStatus{
			Phase: PhasePending,
			ObservedResources: []RoleResource{
				{Type: "Topic", Name: "payments.", PatternType: "prefixed"},
			},
		},
	}

	cp := orig.DeepCopy()

	require.Equal(t, orig, cp, "DeepCopy result must equal original")

	// Mutate copy — original must not be affected.
	cp.Spec.Resources[0].Name = "mutated"
	cp.Spec.Resources[0].PatternType = "mutated"
	cp.Spec.Resources[1].Type = "mutated"
	cp.Spec.Reconciliation.Mode = "mutated"
	cp.Status.ObservedResources[0].Name = "mutated"
	cp.Status.Phase = "mutated"

	require.Equal(t, "payments.", orig.Spec.Resources[0].Name, "Spec.Resources[0].Name must not be corrupted")
	require.Equal(t, "prefixed", orig.Spec.Resources[0].PatternType, "Spec.Resources[0].PatternType must not be corrupted")
	require.Equal(t, "Group", orig.Spec.Resources[1].Type, "Spec.Resources[1].Type must not be corrupted")
	require.Equal(t, "Enforce", orig.Spec.Reconciliation.Mode, "Spec.Reconciliation.Mode must not be corrupted")
	require.Equal(t, "payments.", orig.Status.ObservedResources[0].Name, "Status.ObservedResources[0].Name must not be corrupted")
	require.Equal(t, PhasePending, orig.Status.Phase, "Status.Phase must not be corrupted")
}

// TestKafkaUserDeepCopyRoundTrip verifies DeepCopy independence for the new
// KafkaUser type (SCRAM principal management): Password.ValueFrom,
// Password.Generate, Iterations, and Status.AppliedPasswordRef pointer
// independence.
func TestKafkaUserDeepCopyRoundTrip(t *testing.T) {
	iterations := int32(8192)
	orig := &KafkaUser{
		Spec: KafkaUserSpec{
			ClusterRef: ClusterRef{Name: "prod"},
			Username:   "svc-checkout",
			Mechanism:  "SCRAM-SHA-512",
			Iterations: &iterations,
			Password: &UserPassword{
				ValueFrom: &ValueSource{
					SecretKeyRef: &SecretKeyRef{Name: "svc-checkout-credentials", Key: "password"},
				},
				Generate: &GeneratePassword{},
			},
			DeletionPolicy: "Delete",
		},
		Status: &KafkaUserStatus{
			Phase: PhasePending,
			AppliedPasswordRef: &AppliedPasswordRef{
				SecretName:      "svc-checkout-credentials",
				ResourceVersion: "12345",
			},
			GeneratedSecretName: "svc-checkout-kafka-credentials",
		},
	}

	cp := orig.DeepCopy()

	require.Equal(t, orig, cp, "DeepCopy result must equal original")

	// Mutate copy — original must not be affected.
	*cp.Spec.Iterations = 4096
	cp.Spec.Password.ValueFrom.SecretKeyRef.Key = "mutated"
	cp.Spec.Password.ValueFrom.SecretKeyRef.Name = "mutated"
	cp.Status.AppliedPasswordRef.SecretName = "mutated"
	cp.Status.AppliedPasswordRef.ResourceVersion = "mutated"
	cp.Status.GeneratedSecretName = "mutated"

	require.Equal(t, int32(8192), *orig.Spec.Iterations, "Spec.Iterations must not be corrupted")
	require.Equal(t, "password", orig.Spec.Password.ValueFrom.SecretKeyRef.Key, "Spec.Password.ValueFrom.SecretKeyRef.Key must not be corrupted")
	require.Equal(t, "svc-checkout-credentials", orig.Spec.Password.ValueFrom.SecretKeyRef.Name, "Spec.Password.ValueFrom.SecretKeyRef.Name must not be corrupted")
	require.Equal(t, "svc-checkout-credentials", orig.Status.AppliedPasswordRef.SecretName, "Status.AppliedPasswordRef.SecretName must not be corrupted")
	require.Equal(t, "12345", orig.Status.AppliedPasswordRef.ResourceVersion, "Status.AppliedPasswordRef.ResourceVersion must not be corrupted")
	require.Equal(t, "svc-checkout-kafka-credentials", orig.Status.GeneratedSecretName, "Status.GeneratedSecretName must not be corrupted")
}
