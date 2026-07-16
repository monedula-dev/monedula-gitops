package validation

import (
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ---- quota test helpers ----

func fv(v float64) *float64 { return &v }

func mkQuota(name, cluster string) *v1alpha1.KafkaQuota {
	return &v1alpha1.KafkaQuota{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaQuota"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: cluster},
			Entity:     v1alpha1.QuotaEntity{User: "User:svc"},
			Limits:     v1alpha1.QuotaLimits{ProducerByteRate: fv(1024)},
		},
	}
}

func topic(name, cluster, topicName string, partitions int) *v1alpha1.KafkaTopic {
	return &v1alpha1.KafkaTopic{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaTopic"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha1.KafkaTopicSpec{ClusterRef: v1alpha1.ClusterRef{Name: cluster}, TopicName: topicName, Partitions: partitions},
	}
}

func TestValidTopicPasses(t *testing.T) {
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{topic("orders", "prod", "payments.orders", 12)},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.Empty(t, errs)
}

func TestTopicPartitionsMustBePositive(t *testing.T) {
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{topic("orders", "prod", "payments.orders", 0)},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.NotEmpty(t, errs)
}

func TestUnknownClusterRefIsError(t *testing.T) {
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{topic("orders", "missing", "payments.orders", 1)},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.Len(t, errs, 1)
	require.Contains(t, errs[0].Error(), `references cluster "missing"`)
}

func TestTopicIdentityCollisionIsError(t *testing.T) {
	errs := Validate(Input{
		Topics: []*v1alpha1.KafkaTopic{
			topic("a", "prod", "payments.orders", 1),
			topic("b", "prod", "payments.orders", 1),
		},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.NotEmpty(t, errs)
	require.Contains(t, errs[0].Error(), "payments.orders")
}

func TestSchemaRequiresSchemaRegistry(t *testing.T) {
	tp := topic("orders", "prod", "payments.orders", 1)
	tp.Spec.Schema = &v1alpha1.TopicSchema{Format: "AVRO"}
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()}, // no schemaRegistry
	})
	require.NotEmpty(t, errs)
}

func TestUnknownSchemaRegistryAuthTypeIsError(t *testing.T) {
	// internal/cluster only builds basic auth and silently ignores anything
	// else; an unknown type must be caught here instead of connecting
	// unauthenticated.
	cl := srCluster()
	cl.Spec.SchemaRegistry.Auth = &v1alpha1.SchemaRegistryAuth{Type: "bearer"}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.Len(t, errs, 1)
	require.Contains(t, errs[0].Error(), `schemaRegistry.auth.type "bearer"`)
}

func TestSchemaRegistryAuthTypeBasicOrEmptyOK(t *testing.T) {
	cl := srCluster()
	cl.Spec.SchemaRegistry.Auth = &v1alpha1.SchemaRegistryAuth{Type: "basic"}
	require.Empty(t, Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}}))

	cl.Spec.SchemaRegistry.Auth = &v1alpha1.SchemaRegistryAuth{} // type omitted: no auth
	require.Empty(t, Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}}))
}

// cfgCluster is a minimal valid KafkaCluster config object (named "prod").
func cfgCluster() *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: "prod"},
	}
}

func srCluster() *v1alpha1.KafkaCluster {
	cl := cfgCluster()
	cl.Spec.SchemaRegistry = &v1alpha1.SchemaRegistryConf{Endpoint: "http://sr"}
	return cl
}

func schemaTopic(s *v1alpha1.TopicSchema) *v1alpha1.KafkaTopic {
	tp := topic("orders", "prod", "payments.orders", 1)
	tp.Spec.Schema = s
	return tp
}

func valueFrom() *v1alpha1.ValueFrom {
	return &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "schema.avsc"}}
}

func TestValidSchemaPasses(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{
		Format:        "AVRO",
		Compatibility: "BACKWARD",
		ValueSchema:   valueFrom(),
	})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.Empty(t, errs)
}

func TestSchemaBadFormat(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{Format: "YAML", ValueSchema: valueFrom()})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.True(t, errorsContain(errs, `invalid schema format "YAML"`), "got %v", errs)
}

// RecordName/TopicRecordName with valueSchema content are valid (the record
// name is extracted from the body at build time).
func TestSchemaRecordNameWithContentValid(t *testing.T) {
	for _, strategy := range []string{"RecordName", "TopicRecordName"} {
		tp := schemaTopic(&v1alpha1.TopicSchema{Format: "AVRO", SubjectStrategy: strategy, ValueSchema: valueFrom()})
		errs := Validate(Input{
			Topics:   []*v1alpha1.KafkaTopic{tp},
			Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
		})
		require.Empty(t, errs, "%s with valueSchema content must be valid: %v", strategy, errs)
	}
}

// RecordName/TopicRecordName in governance mode (no body) are illegal: there is
// no schema content to extract the record name from.
func TestSchemaRecordNameGovernanceModeErrors(t *testing.T) {
	for _, strategy := range []string{"RecordName", "TopicRecordName"} {
		tp := schemaTopic(&v1alpha1.TopicSchema{Format: "AVRO", SubjectStrategy: strategy, Compatibility: "BACKWARD"})
		errs := Validate(Input{
			Topics:   []*v1alpha1.KafkaTopic{tp},
			Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
		})
		require.True(t, errorsContain(errs, "requires valueSchema"), "%s governance must error: %v", strategy, errs)
	}
}

// Custom strategy requires an explicit valueSubject.
func TestSchemaCustomRequiresValueSubject(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{Format: "AVRO", SubjectStrategy: "Custom", ValueSchema: valueFrom()})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.True(t, errorsContain(errs, "valueSubject"), "got %v", errs)
}

// Custom with valueSubject + content is valid.
func TestSchemaCustomWithValueSubjectValid(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{
		Format: "AVRO", SubjectStrategy: "Custom",
		ValueSchema: valueFrom(), ValueSubject: "my.value.subject",
	})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.Empty(t, errs, "Custom with valueSubject must be valid: %v", errs)
}

// Custom + key content requires keySubject.
func TestSchemaCustomKeyContentRequiresKeySubject(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{
		Format: "AVRO", SubjectStrategy: "Custom",
		ValueSchema: valueFrom(), KeySchema: valueFrom(),
		ValueSubject: "my.value.subject",
	})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.True(t, errorsContain(errs, "keySubject"), "got %v", errs)
}

// Custom strategy with valueSubject == keySubject must be rejected at validate
// time (cheap check: no schema body needed to detect the collision).
func TestSchemaCustomEqualSubjectsIsError(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{
		Format: "AVRO", SubjectStrategy: "Custom",
		ValueSchema: valueFrom(), KeySchema: valueFrom(),
		ValueSubject: "shared.subject", KeySubject: "shared.subject",
	})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.True(t, errorsContain(errs, "must differ"), "Custom same subjects must error: %v", errs)
}

// Custom + governance mode (no bodies, compatibility set, explicit valueSubject)
// is the canonical way to govern an arbitrary subject — must be legal.
func TestSchemaCustomGovernanceModeValid(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{
		Format: "AVRO", SubjectStrategy: "Custom",
		Compatibility: "FULL", ValueSubject: "arbitrary.subject",
	})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.Empty(t, errs, "Custom governance mode must be valid: %v", errs)
}

// valueSubject/keySubject are Custom-only; setting them with another strategy is
// an error (they would be silently ignored otherwise).
func TestSchemaSubjectFieldsRequireCustom(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{
		Format: "AVRO", SubjectStrategy: "TopicName",
		ValueSchema: valueFrom(), ValueSubject: "x",
	})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.True(t, errorsContain(errs, "valueSubject/keySubject"), "got %v", errs)
}

func TestSchemaInvalidSubjectStrategy(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{Format: "AVRO", SubjectStrategy: "Bogus", ValueSchema: valueFrom()})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.True(t, errorsContain(errs, `invalid subjectStrategy "Bogus"`), "got %v", errs)
}

func TestSchemaBadCompatibility(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{Format: "AVRO", Compatibility: "MAYBE", ValueSchema: valueFrom()})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.True(t, errorsContain(errs, `invalid compatibility "MAYBE"`), "got %v", errs)
}

// Governance mode (spec §12.2): spec.schema without valueSchema/keySchema
// manages only the subject compatibility level, so compatibility is required.
func TestSchemaGovernanceWithoutCompatibilityErrors(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{Format: "AVRO"})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.True(t, errorsContain(errs, "governance mode"), "got %v", errs)
	require.True(t, errorsContain(errs, "requires spec.schema.compatibility"), "got %v", errs)
}

func TestSchemaGovernanceWithCompatibilityValid(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{Format: "AVRO", Compatibility: "BACKWARD"})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.Empty(t, errs, "governance mode with compatibility must be valid: %v", errs)
}

// keySchema without valueSchema stays legal (content mode for the key subject).
func TestSchemaKeyOnlyValid(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{
		Format:    "AVRO",
		KeySchema: valueFrom(),
	})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.Empty(t, errs, "keySchema-only (content mode) must be valid: %v", errs)
}

func TestSchemaWithoutSchemaRegistryStillErrors(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{Format: "AVRO", ValueSchema: valueFrom()})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()}, // no schemaRegistry
	})
	require.True(t, errorsContain(errs, "requires KafkaCluster"), "got %v", errs)
}

func TestNoClusterConfigSkipsClusterChecks(t *testing.T) {
	// Clusters nil => syntax/shape only, unknown ref not checked
	errs := Validate(Input{
		Topics: []*v1alpha1.KafkaTopic{topic("orders", "whatever", "payments.orders", 1)},
	})
	require.Empty(t, errs)
}

func TestPolicyRuleValidation(t *testing.T) {
	pol := &v1alpha1.KafkaAccessPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaAccessPolicy"},
		ObjectMeta: metav1.ObjectMeta{Name: "p"},
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Rules: []v1alpha1.ACLRule{
				{Principal: "User:x", Resource: v1alpha1.ACLResource{Type: "bogus", Name: "t"}, Operations: []string{"Nope"}},
			},
		},
	}
	errs := Validate(Input{Policies: []*v1alpha1.KafkaAccessPolicy{pol}, Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()}})
	// invalid resource type AND invalid operation => at least 2 errors
	require.GreaterOrEqual(t, len(errs), 2)
}

func TestEmptyPolicyRulesIsError(t *testing.T) {
	pol := &v1alpha1.KafkaAccessPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaAccessPolicy"},
		ObjectMeta: metav1.ObjectMeta{Name: "p"},
		Spec:       v1alpha1.KafkaAccessPolicySpec{ClusterRef: v1alpha1.ClusterRef{Name: "prod"}},
	}
	errs := Validate(Input{Policies: []*v1alpha1.KafkaAccessPolicy{pol}, Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()}})
	require.NotEmpty(t, errs)
}

func validPolicy() *v1alpha1.KafkaAccessPolicy {
	return &v1alpha1.KafkaAccessPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaAccessPolicy"},
		ObjectMeta: metav1.ObjectMeta{Name: "p"},
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Rules: []v1alpha1.ACLRule{
				{Principal: "User:x", Resource: v1alpha1.ACLResource{Type: "topic", Name: "t"}, Operations: []string{"Read"}},
			},
		},
	}
}

func errorsContain(errs []error, substr string) bool {
	for _, e := range errs {
		if strings.Contains(e.Error(), substr) {
			return true
		}
	}
	return false
}

func TestNegativeValidation(t *testing.T) {
	clusters := map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()}

	tests := []struct {
		name   string
		mutate func() Input
		substr string
	}{
		{
			name: "invalid topic deletionPolicy",
			mutate: func() Input {
				tp := topic("orders", "prod", "payments.orders", 1)
				tp.Spec.DeletionPolicy = "Nope"
				return Input{Topics: []*v1alpha1.KafkaTopic{tp}, Clusters: clusters}
			},
			substr: `invalid deletionPolicy "Nope"`,
		},
		{
			name: "invalid topic reconciliation.mode",
			mutate: func() Input {
				tp := topic("orders", "prod", "payments.orders", 1)
				tp.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: "Bogus"}
				return Input{Topics: []*v1alpha1.KafkaTopic{tp}, Clusters: clusters}
			},
			substr: `invalid reconciliation.mode "Bogus"`,
		},
		{
			name: "invalid policy deletionPolicy",
			mutate: func() Input {
				pol := validPolicy()
				pol.Spec.DeletionPolicy = "Nope"
				return Input{Policies: []*v1alpha1.KafkaAccessPolicy{pol}, Clusters: clusters}
			},
			substr: `invalid deletionPolicy "Nope"`,
		},
		{
			name: "invalid policy reconciliation.mode",
			mutate: func() Input {
				pol := validPolicy()
				pol.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: "Bogus"}
				return Input{Policies: []*v1alpha1.KafkaAccessPolicy{pol}, Clusters: clusters}
			},
			substr: `invalid reconciliation.mode "Bogus"`,
		},
		{
			name: "empty producer principal",
			mutate: func() Input {
				tp := topic("orders", "prod", "payments.orders", 1)
				tp.Spec.Access.Producers = []v1alpha1.ProducerAccess{{Principal: ""}}
				return Input{Topics: []*v1alpha1.KafkaTopic{tp}, Clusters: clusters}
			},
			substr: "producer principal must be non-empty",
		},
		{
			name: "empty consumer group",
			mutate: func() Input {
				tp := topic("orders", "prod", "payments.orders", 1)
				tp.Spec.Access.Consumers = []v1alpha1.ConsumerAccess{{Principal: "User:x", Group: ""}}
				return Input{Topics: []*v1alpha1.KafkaTopic{tp}, Clusters: clusters}
			},
			substr: "consumer group must be non-empty",
		},
		{
			name: "whitespace-only producer host",
			mutate: func() Input {
				tp := topic("orders", "prod", "payments.orders", 1)
				tp.Spec.Access.Producers = []v1alpha1.ProducerAccess{{Principal: "User:svc", Host: "   "}}
				return Input{Topics: []*v1alpha1.KafkaTopic{tp}, Clusters: clusters}
			},
			substr: "producer host must not be blank",
		},
		{
			name: "whitespace-only consumer host",
			mutate: func() Input {
				tp := topic("orders", "prod", "payments.orders", 1)
				tp.Spec.Access.Consumers = []v1alpha1.ConsumerAccess{{Principal: "User:svc", Group: "g", Host: "   "}}
				return Input{Topics: []*v1alpha1.KafkaTopic{tp}, Clusters: clusters}
			},
			substr: "consumer host must not be blank",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			errs := Validate(tc.mutate())
			require.True(t, errorsContain(errs, tc.substr), "expected an error containing %q, got %v", tc.substr, errs)
		})
	}
}

// ---- topic-access operations vocabulary (I1) ----

func TestTopicProducerOperationCaseVariantIsError(t *testing.T) {
	tp := topic("orders", "prod", "payments.orders", 1)
	tp.Spec.Access.Producers = []v1alpha1.ProducerAccess{
		{Principal: "User:svc", Operations: []string{"WRITE"}},
	}
	errs := Validate(Input{Topics: []*v1alpha1.KafkaTopic{tp}, Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()}})
	require.True(t, errorsContain(errs, `"WRITE"`), "error must name the bad op, got %v", errs)
	require.True(t, errorsContain(errs, `"Write"`), "error must name the canonical form, got %v", errs)
}

func TestTopicConsumerOperationsValidated(t *testing.T) {
	tp := topic("orders", "prod", "payments.orders", 1)
	tp.Spec.Access.Consumers = []v1alpha1.ConsumerAccess{
		{Principal: "User:svc", Group: "g", TopicOperations: []string{"read"}, GroupOperations: []string{"Bogus"}},
	}
	errs := Validate(Input{Topics: []*v1alpha1.KafkaTopic{tp}, Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()}})
	require.True(t, errorsContain(errs, `"read"`), "error must name the bad topic op, got %v", errs)
	require.True(t, errorsContain(errs, `"Read"`), "error must name the canonical form, got %v", errs)
	require.True(t, errorsContain(errs, `"Bogus"`), "error must name the bad group op, got %v", errs)
}

func TestTopicAccessHostValidValues(t *testing.T) {
	clusters := map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()}

	// CIDR and wildcard hosts are valid; omitted host (empty string) is also valid.
	tp := topic("orders", "prod", "payments.orders", 1)
	tp.Spec.Access.Producers = []v1alpha1.ProducerAccess{
		{Principal: "User:svc", Host: "10.0.0.0/8"},
	}
	tp.Spec.Access.Consumers = []v1alpha1.ConsumerAccess{
		{Principal: "User:svc", Group: "g", Host: "*"},
	}
	errs := Validate(Input{Topics: []*v1alpha1.KafkaTopic{tp}, Clusters: clusters})
	require.Empty(t, errs)
}

func TestTopicAccessNoHostPasses(t *testing.T) {
	clusters := map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()}

	// Entries with no host field set must continue to validate without error.
	tp := topic("orders", "prod", "payments.orders", 1)
	tp.Spec.Access.Producers = []v1alpha1.ProducerAccess{
		{Principal: "User:svc"},
	}
	tp.Spec.Access.Consumers = []v1alpha1.ConsumerAccess{
		{Principal: "User:svc", Group: "g"},
	}
	errs := Validate(Input{Topics: []*v1alpha1.KafkaTopic{tp}, Clusters: clusters})
	require.Empty(t, errs)
}

func TestTopicAccessCanonicalOperationsPass(t *testing.T) {
	tp := topic("orders", "prod", "payments.orders", 1)
	tp.Spec.Access.Producers = []v1alpha1.ProducerAccess{
		{Principal: "User:svc", Operations: []string{"Write", "Describe", "IdempotentWrite"}},
	}
	tp.Spec.Access.Consumers = []v1alpha1.ConsumerAccess{
		{Principal: "User:svc", Group: "g", TopicOperations: []string{"Read"}, GroupOperations: []string{"Read"}},
	}
	errs := Validate(Input{Topics: []*v1alpha1.KafkaTopic{tp}, Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()}})
	require.Empty(t, errs)
}

func TestPolicyOperationCaseVariantNamesCanonical(t *testing.T) {
	pol := validPolicy()
	pol.Spec.Rules[0].Operations = []string{"WRITE"}
	errs := Validate(Input{Policies: []*v1alpha1.KafkaAccessPolicy{pol}, Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()}})
	require.True(t, errorsContain(errs, `"WRITE"`), "got %v", errs)
	require.True(t, errorsContain(errs, `"Write"`), "error must name the canonical form, got %v", errs)
}

// ---- identity collision without cluster config (I4) ----

func TestTopicIdentityCollisionWithoutClusterConfig(t *testing.T) {
	errs := Validate(Input{
		Topics: []*v1alpha1.KafkaTopic{
			topic("a", "prod", "payments.orders", 1),
			topic("b", "prod", "payments.orders", 1),
		},
		// Clusters nil: the typical PR-lint `validate -f` invocation.
	})
	require.NotEmpty(t, errs, "collision must be detected without cluster config")
	require.True(t, errorsContain(errs, "payments.orders"), "got %v", errs)
}

func TestTopicSameNameDifferentClustersNoCollision(t *testing.T) {
	errs := Validate(Input{
		Topics: []*v1alpha1.KafkaTopic{
			topic("a", "prod", "payments.orders", 1),
			topic("b", "staging", "payments.orders", 1),
		},
	})
	require.Empty(t, errs)
}

// ---- apiVersion + metadata.name (M1, M2) ----

func TestPolicyAPIVersionChecked(t *testing.T) {
	pol := validPolicy()
	pol.APIVersion = "gitops.monedula.dev/v1beta1"
	errs := Validate(Input{Policies: []*v1alpha1.KafkaAccessPolicy{pol}, Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()}})
	require.True(t, errorsContain(errs, "apiVersion must be"), "got %v", errs)
}

func TestClusterAPIVersionChecked(t *testing.T) {
	cl := cfgCluster()
	cl.APIVersion = "gitops.monedula.dev/v1beta1"
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "apiVersion must be"), "got %v", errs)
}

func TestMetadataNameRequired(t *testing.T) {
	tp := topic("", "prod", "payments.orders", 1)
	pol := validPolicy()
	pol.Name = ""
	cl := cfgCluster()
	cl.Name = ""
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Policies: []*v1alpha1.KafkaAccessPolicy{pol},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl},
	})
	require.True(t, errorsContain(errs, "KafkaTopic"), "got %v", errs)
	require.True(t, errorsContain(errs, "KafkaAccessPolicy"), "got %v", errs)
	require.True(t, errorsContain(errs, "KafkaCluster"), "got %v", errs)
	count := 0
	for _, e := range errs {
		if strings.Contains(e.Error(), "metadata.name") {
			count++
		}
	}
	require.GreaterOrEqual(t, count, 3, "want a metadata.name error per kind, got %v", errs)
}

func TestReplicationFactorAndPlacementMutuallyExclusive(t *testing.T) {
	rf := 3
	tp := topic("orders", "prod", "payments.orders", 6)
	tp.Spec.ReplicationFactor = &rf
	tp.Spec.Config = map[string]string{"confluent.placement.constraints": `{"version":2}`}
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.True(t, errorsContain(errs, "mutually exclusive"),
		"expected RF/placement mutual-exclusion error, got %v", errs)
}

func TestPlacementWithoutReplicationFactorIsValid(t *testing.T) {
	tp := topic("orders", "prod", "payments.orders", 6) // no replicationFactor set
	tp.Spec.Config = map[string]string{"confluent.placement.constraints": `{"version":2}`}
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.Empty(t, errs)
}

// ---- drift.ignoreFields syntax (spec §16) ----

func TestDriftIgnoreFieldsValidEntriesPass(t *testing.T) {
	tp := topic("orders", "prod", "payments.orders", 1)
	tp.Spec.Drift = &v1alpha1.DriftConfig{IgnoreFields: []string{
		"partitions", "replicationFactor", "config.retention.ms", "config.confluent.placement.constraints",
	}}
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.Empty(t, errs)
}

// ---- cluster defaults.topicDeletionPolicy (spec §4.7) ----

func TestClusterDefaultTopicDeletionPolicyValidValues(t *testing.T) {
	for _, pol := range []string{"Orphan", "Delete", ""} {
		cl := cfgCluster()
		cl.Spec.Defaults = &v1alpha1.ClusterDefaults{TopicDeletionPolicy: pol}
		errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
		require.Empty(t, errs, "expected no errors for topicDeletionPolicy=%q, got %v", pol, errs)
	}
}

func TestClusterDefaultTopicDeletionPolicyInvalidIsError(t *testing.T) {
	cl := cfgCluster()
	cl.Spec.Defaults = &v1alpha1.ClusterDefaults{TopicDeletionPolicy: "Foo"}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.Len(t, errs, 1)
	require.True(t, errorsContain(errs, `invalid defaults.topicDeletionPolicy "Foo" (must be Orphan or Delete)`),
		"expected error naming bad value, got %v", errs)
	require.True(t, errorsContain(errs, "prod"),
		"expected error naming the cluster, got %v", errs)
}

// ---- OAUTHBEARER auth validation (spec §4.5) ----

func oauthCluster() *v1alpha1.KafkaCluster {
	cl := cfgCluster()
	cl.Spec.Auth = &v1alpha1.AuthConfig{
		Mechanism: "OAUTHBEARER",
		OAuth: &v1alpha1.OAuthConfig{
			TokenEndpoint: "https://idp.example.com/token",
			ClientID:      v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "CLIENT_ID"}},
			ClientSecret:  v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "CLIENT_SECRET"}},
		},
	}
	return cl
}

func TestOAuthBearerHappyPath(t *testing.T) {
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": oauthCluster()}})
	require.Empty(t, errs, "valid OAUTHBEARER config must pass: %v", errs)
}

func TestOAuthBearerWithScopeHappyPath(t *testing.T) {
	cl := oauthCluster()
	cl.Spec.Auth.OAuth.Scope = "kafka"
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.Empty(t, errs, "OAUTHBEARER with scope must pass: %v", errs)
}

func TestOAuthBearerMissingOAuthBlock(t *testing.T) {
	cl := cfgCluster()
	cl.Spec.Auth = &v1alpha1.AuthConfig{Mechanism: "OAUTHBEARER"}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "requires auth.oauth"), "got %v", errs)
}

func TestOAuthBearerMissingTokenEndpoint(t *testing.T) {
	cl := oauthCluster()
	cl.Spec.Auth.OAuth.TokenEndpoint = ""
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "tokenEndpoint"), "got %v", errs)
}

func TestOAuthBearerMissingClientID(t *testing.T) {
	cl := oauthCluster()
	cl.Spec.Auth.OAuth.ClientID = v1alpha1.ValueFrom{}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "clientId"), "got %v", errs)
}

func TestOAuthBearerMissingClientSecret(t *testing.T) {
	cl := oauthCluster()
	cl.Spec.Auth.OAuth.ClientSecret = v1alpha1.ValueFrom{}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "clientSecret"), "got %v", errs)
}

func TestOAuthBearerWithSCRAMBlockIsError(t *testing.T) {
	cl := oauthCluster()
	cl.Spec.Auth.SCRAM = &v1alpha1.SCRAMAuth{
		Username: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "U"}},
		Password: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "P"}},
	}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "auth.scram is not allowed"), "got %v", errs)
}

func TestOAuthBlockWithSCRAMMechanismIsError(t *testing.T) {
	cl := cfgCluster()
	cl.Spec.Auth = &v1alpha1.AuthConfig{
		Mechanism: "SCRAM-SHA-256",
		SCRAM: &v1alpha1.SCRAMAuth{
			Username: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "U"}},
			Password: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "P"}},
		},
		OAuth: &v1alpha1.OAuthConfig{
			TokenEndpoint: "https://idp.example.com/token",
			ClientID:      v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "CLIENT_ID"}},
			ClientSecret:  v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "CLIENT_SECRET"}},
		},
	}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "auth.oauth must not be set"), "got %v", errs)
}

func TestOAuthBlockWithPLAINMechanismIsError(t *testing.T) {
	cl := cfgCluster()
	cl.Spec.Auth = &v1alpha1.AuthConfig{
		Mechanism: "PLAIN",
		OAuth: &v1alpha1.OAuthConfig{
			TokenEndpoint: "https://idp.example.com/token",
			ClientID:      v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "CLIENT_ID"}},
			ClientSecret:  v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "CLIENT_SECRET"}},
		},
	}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "auth.oauth must not be set"), "got %v", errs)
}

func TestMTLSMissingTLSIsError(t *testing.T) {
	// mTLS without tls.enabled + clientCert + clientKey must produce errors.
	cl := cfgCluster()
	cl.Spec.Auth = &v1alpha1.AuthConfig{Mechanism: "mTLS"}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "tls.enabled"), "expected tls.enabled error, got %v", errs)
	require.True(t, errorsContain(errs, "tls.clientCert"), "expected tls.clientCert error, got %v", errs)
	require.True(t, errorsContain(errs, "tls.clientKey"), "expected tls.clientKey error, got %v", errs)
}

// ---- mTLS auth validation (spec §4.5) ----

func mtlsCluster() *v1alpha1.KafkaCluster {
	cl := cfgCluster()
	cl.Spec.TLS = &v1alpha1.TLSConfig{
		Enabled:    true,
		ClientCert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.crt"}},
		ClientKey:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.key"}},
	}
	cl.Spec.Auth = &v1alpha1.AuthConfig{Mechanism: "mTLS"}
	return cl
}

func TestMTLSHappyPath(t *testing.T) {
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": mtlsCluster()}})
	require.Empty(t, errs, "fully-specified mTLS config must pass: %v", errs)
}

func TestMTLSTLSDisabledIsError(t *testing.T) {
	cl := mtlsCluster()
	cl.Spec.TLS.Enabled = false
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "tls.enabled"), "expected tls.enabled error, got %v", errs)
}

func TestMTLSMissingClientCertIsError(t *testing.T) {
	cl := mtlsCluster()
	cl.Spec.TLS.ClientCert = nil
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "tls.clientCert"), "expected tls.clientCert error, got %v", errs)
}

func TestMTLSMissingClientKeyIsError(t *testing.T) {
	cl := mtlsCluster()
	cl.Spec.TLS.ClientKey = nil
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "tls.clientKey"), "expected tls.clientKey error, got %v", errs)
}

func TestMTLSWithSCRAMIsError(t *testing.T) {
	cl := mtlsCluster()
	cl.Spec.Auth.SCRAM = &v1alpha1.SCRAMAuth{
		Username: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "U"}},
		Password: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "P"}},
	}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "auth.scram"), "expected auth.scram error, got %v", errs)
}

func TestMTLSWithOAuthIsError(t *testing.T) {
	cl := mtlsCluster()
	cl.Spec.Auth.OAuth = &v1alpha1.OAuthConfig{
		TokenEndpoint: "https://idp.example.com/token",
		ClientID:      v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "CLIENT_ID"}},
		ClientSecret:  v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "CLIENT_SECRET"}},
	}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "auth.oauth"), "expected auth.oauth error, got %v", errs)
}

func TestTLSCertWithoutKeyIsError(t *testing.T) {
	// Independent rule: clientCert without clientKey (any mechanism) is an error.
	cl := cfgCluster()
	cl.Spec.TLS = &v1alpha1.TLSConfig{
		Enabled:    true,
		ClientCert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.crt"}},
		// ClientKey intentionally omitted
	}
	cl.Spec.Auth = &v1alpha1.AuthConfig{Mechanism: "SCRAM-SHA-256",
		SCRAM: &v1alpha1.SCRAMAuth{
			Username: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "U"}},
			Password: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "P"}},
		},
	}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "tls.clientCert and tls.clientKey must be set together"), "expected cert/key pairing error, got %v", errs)
}

func TestTLSKeyWithoutCertIsError(t *testing.T) {
	// Independent rule: clientKey without clientCert (any mechanism) is an error.
	cl := cfgCluster()
	cl.Spec.TLS = &v1alpha1.TLSConfig{
		Enabled: true,
		// ClientCert intentionally omitted
		ClientKey: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.key"}},
	}
	cl.Spec.Auth = &v1alpha1.AuthConfig{Mechanism: "None"}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "tls.clientCert and tls.clientKey must be set together"), "expected cert/key pairing error, got %v", errs)
}

func TestSCRAMOverMTLSTransportIsLegal(t *testing.T) {
	// SCRAM with both clientCert and clientKey set is legal (SCRAM-over-mTLS transport).
	cl := cfgCluster()
	cl.Spec.TLS = &v1alpha1.TLSConfig{
		Enabled:    true,
		ClientCert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.crt"}},
		ClientKey:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.key"}},
	}
	cl.Spec.Auth = &v1alpha1.AuthConfig{
		Mechanism: "SCRAM-SHA-256",
		SCRAM: &v1alpha1.SCRAMAuth{
			Username: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "U"}},
			Password: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "P"}},
		},
	}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.Empty(t, errs, "SCRAM with client cert/key (mutual TLS transport) must be legal: %v", errs)
}

// TestClientCertsRequireTLSEnabled verifies that setting clientCert/clientKey while
// tls.enabled is false (any mechanism) is a validation error.
func TestClientCertsRequireTLSEnabled(t *testing.T) {
	cl := cfgCluster()
	cl.Spec.TLS = &v1alpha1.TLSConfig{
		Enabled:    false,
		ClientCert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.crt"}},
		ClientKey:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.key"}},
	}
	cl.Spec.Auth = &v1alpha1.AuthConfig{
		Mechanism: "SCRAM-SHA-256",
		SCRAM: &v1alpha1.SCRAMAuth{
			Username: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "U"}},
			Password: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "P"}},
		},
	}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "tls.clientCert/tls.clientKey require tls.enabled: true"),
		"expected cert/enabled error, got %v", errs)
}

// TestClientCertsWithTLSEnabledIsLegal verifies that clientCert/clientKey with tls.enabled=true
// does not produce a cert/enabled error.
func TestClientCertsWithTLSEnabledIsLegal(t *testing.T) {
	cl := cfgCluster()
	cl.Spec.TLS = &v1alpha1.TLSConfig{
		Enabled:    true,
		ClientCert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.crt"}},
		ClientKey:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.key"}},
	}
	cl.Spec.Auth = &v1alpha1.AuthConfig{
		Mechanism: "SCRAM-SHA-256",
		SCRAM: &v1alpha1.SCRAMAuth{
			Username: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "U"}},
			Password: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "P"}},
		},
	}
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.False(t, errorsContain(errs, "tls.clientCert/tls.clientKey require tls.enabled"),
		"should NOT get cert/enabled error when tls.enabled=true, got %v", errs)
}

// ---- one-of ValueSource validation (spec §11) ----

func TestSchemaValueFromInlineValid(t *testing.T) {
	body := `{"type":"record","name":"Order","fields":[]}`
	tp := schemaTopic(&v1alpha1.TopicSchema{
		Format: "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
			Inline: body,
		}},
	})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.Empty(t, errs, "inline schema source must be valid: %v", errs)
}

func TestSchemaValueFromConfigMapKeyRefValid(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{
		Format: "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
			ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: "schemas", Key: "order.avsc"},
		}},
	})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.Empty(t, errs, "configMapKeyRef schema source must be valid: %v", errs)
}

func TestSchemaValueFromTwoSourcesIsError(t *testing.T) {
	body := `{"type":"record","name":"Order","fields":[]}`
	tp := schemaTopic(&v1alpha1.TopicSchema{
		Format: "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
			Inline: body,
			File:   "schema.avsc",
		}},
	})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.True(t, errorsContain(errs, "exactly one source"), "expected one-of error, got %v", errs)
	require.True(t, errorsContain(errs, "spec.schema.valueSchema"), "error must name the field path, got %v", errs)
}

func TestSchemaKeyFromTwoSourcesIsError(t *testing.T) {
	body := `{"type":"record","name":"Order","fields":[]}`
	tp := schemaTopic(&v1alpha1.TopicSchema{
		Format:      "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "value.avsc"}},
		KeySchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
			Inline:       body,
			SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "s", Key: "k"},
		}},
	})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.True(t, errorsContain(errs, "exactly one source"), "expected one-of error for keySchema, got %v", errs)
	require.True(t, errorsContain(errs, "spec.schema.keySchema"), "error must name keySchema, got %v", errs)
}

func TestSchemaValueFromZeroSourcesIsError(t *testing.T) {
	tp := schemaTopic(&v1alpha1.TopicSchema{
		Format:      "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{}, // zero sources
	})
	errs := Validate(Input{
		Topics:   []*v1alpha1.KafkaTopic{tp},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": srCluster()},
	})
	require.True(t, errorsContain(errs, "exactly one source"), "expected zero-source error, got %v", errs)
}

func TestDriftIgnoreFieldsUnknownSyntaxRejected(t *testing.T) {
	for _, bad := range []string{"configs.retention.ms", "Partitions", "config.", "retention.ms", ""} {
		tp := topic("orders", "prod", "payments.orders", 1)
		tp.Spec.Drift = &v1alpha1.DriftConfig{IgnoreFields: []string{bad}}
		errs := Validate(Input{
			Topics:   []*v1alpha1.KafkaTopic{tp},
			Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
		})
		require.Len(t, errs, 1, "entry %q must be rejected", bad)
		require.Contains(t, errs[0].Error(), "drift.ignoreFields")
	}
}

// ---- Tenancy shape-validation tests (spec §20.2) ----

// tenancyCluster returns a KafkaCluster with the given TenancyConfig.
func tenancyCluster(t *v1alpha1.TenancyConfig) *v1alpha1.KafkaCluster {
	cl := cfgCluster()
	cl.Spec.Tenancy = t
	return cl
}

func TestTenancyValidConfigPasses(t *testing.T) {
	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-*", "platform"},
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			{Namespaces: []string{"team-*"}, Prefixes: []string{"payments."}},
		},
	})
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.Empty(t, errs)
}

func TestTenancyEmptyConfigPasses(t *testing.T) {
	// nil Tenancy and empty TenancyConfig are both valid (unrestricted cluster).
	cl := cfgCluster()
	require.Empty(t, Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}}))

	cl.Spec.Tenancy = &v1alpha1.TenancyConfig{}
	require.Empty(t, Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}}))
}

func TestTenancyBadGlobInAllowedNamespacesIsError(t *testing.T) {
	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"[invalid-glob"},
	})
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.NotEmpty(t, errs)
	require.Contains(t, errs[0].Error(), "allowedNamespaces")
	require.Contains(t, errs[0].Error(), "invalid glob pattern")
}

func TestTenancyTopicPrefixRuleEmptyNamespacesIsError(t *testing.T) {
	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			{Namespaces: nil, Prefixes: []string{"payments."}},
		},
	})
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.NotEmpty(t, errs)
	require.Contains(t, errs[0].Error(), "namespaces must be non-empty")
}

func TestTenancyTopicPrefixRuleEmptyPrefixesIsError(t *testing.T) {
	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			{Namespaces: []string{"team-*"}, Prefixes: nil},
		},
	})
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.NotEmpty(t, errs)
	require.Contains(t, errs[0].Error(), "prefixes must be non-empty")
}

func TestTenancyTopicPrefixRuleBadNamespaceGlobIsError(t *testing.T) {
	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			{Namespaces: []string{"[bad-glob"}, Prefixes: []string{"payments."}},
		},
	})
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.NotEmpty(t, errs)
	require.Contains(t, errs[0].Error(), "invalid glob pattern")
}

// ---- ValidateQuotaShape unit tests (spec §39.5) ----

// validQuota returns a KafkaQuota with one entity component and one limit set.
func validQuotaShape(entity v1alpha1.QuotaEntity, limits v1alpha1.QuotaLimits) *v1alpha1.KafkaQuota {
	return &v1alpha1.KafkaQuota{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaQuota"},
		ObjectMeta: metav1.ObjectMeta{Name: "q"},
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Entity:     entity,
			Limits:     limits,
		},
	}
}

func TestValidateQuotaShape_UserOnly(t *testing.T) {
	q := validQuotaShape(
		v1alpha1.QuotaEntity{User: "User:svc"},
		v1alpha1.QuotaLimits{ProducerByteRate: fv(1024)},
	)
	require.Empty(t, ValidateQuotaShape(q))
}

func TestValidateQuotaShape_ClientIdOnly(t *testing.T) {
	q := validQuotaShape(
		v1alpha1.QuotaEntity{ClientId: "my-client"},
		v1alpha1.QuotaLimits{ConsumerByteRate: fv(2048)},
	)
	require.Empty(t, ValidateQuotaShape(q))
}

func TestValidateQuotaShape_UserAndClientId(t *testing.T) {
	q := validQuotaShape(
		v1alpha1.QuotaEntity{User: "User:svc", ClientId: "my-client"},
		v1alpha1.QuotaLimits{RequestPercentage: fv(50)},
	)
	require.Empty(t, ValidateQuotaShape(q))
}

func TestValidateQuotaShape_UserDefault(t *testing.T) {
	q := validQuotaShape(
		v1alpha1.QuotaEntity{UserDefault: true},
		v1alpha1.QuotaLimits{ControllerMutationRate: fv(10)},
	)
	require.Empty(t, ValidateQuotaShape(q))
}

func TestValidateQuotaShape_ClientIdDefault(t *testing.T) {
	q := validQuotaShape(
		v1alpha1.QuotaEntity{ClientIdDefault: true},
		v1alpha1.QuotaLimits{ProducerByteRate: fv(512)},
	)
	require.Empty(t, ValidateQuotaShape(q))
}

func TestValidateQuotaShape_AllFourLimits(t *testing.T) {
	q := validQuotaShape(
		v1alpha1.QuotaEntity{User: "User:svc"},
		v1alpha1.QuotaLimits{
			ProducerByteRate:       fv(1024),
			ConsumerByteRate:       fv(2048),
			RequestPercentage:      fv(75),
			ControllerMutationRate: fv(5),
		},
	)
	require.Empty(t, ValidateQuotaShape(q))
}

func TestValidateQuotaShape_NoEntityComponent(t *testing.T) {
	q := validQuotaShape(
		v1alpha1.QuotaEntity{},
		v1alpha1.QuotaLimits{ProducerByteRate: fv(1024)},
	)
	errs := ValidateQuotaShape(q)
	require.True(t, errorsContain(errs, "at least one"), "got %v", errs)
}

func TestValidateQuotaShape_UserAndUserDefault(t *testing.T) {
	q := validQuotaShape(
		v1alpha1.QuotaEntity{User: "User:svc", UserDefault: true},
		v1alpha1.QuotaLimits{ProducerByteRate: fv(1024)},
	)
	errs := ValidateQuotaShape(q)
	require.True(t, errorsContain(errs, "user"), "got %v", errs)
	require.True(t, errorsContain(errs, "userDefault"), "got %v", errs)
}

func TestValidateQuotaShape_ClientIdAndClientIdDefault(t *testing.T) {
	q := validQuotaShape(
		v1alpha1.QuotaEntity{ClientId: "my-client", ClientIdDefault: true},
		v1alpha1.QuotaLimits{ProducerByteRate: fv(1024)},
	)
	errs := ValidateQuotaShape(q)
	require.True(t, errorsContain(errs, "clientId"), "got %v", errs)
	require.True(t, errorsContain(errs, "clientIdDefault"), "got %v", errs)
}

func TestValidateQuotaShape_UserNotUserPrefix(t *testing.T) {
	q := validQuotaShape(
		v1alpha1.QuotaEntity{User: "svc-checkout"},
		v1alpha1.QuotaLimits{ProducerByteRate: fv(1024)},
	)
	errs := ValidateQuotaShape(q)
	require.True(t, errorsContain(errs, `"User:"`), "got %v", errs)
}

func TestValidateQuotaShape_UserPrefixOnlyEmptyName(t *testing.T) {
	q := validQuotaShape(
		v1alpha1.QuotaEntity{User: "User:"},
		v1alpha1.QuotaLimits{ProducerByteRate: fv(1024)},
	)
	errs := ValidateQuotaShape(q)
	require.True(t, errorsContain(errs, "non-empty name"), "got %v", errs)
}

func TestValidateQuotaShape_ClientIdBlankWhitespace(t *testing.T) {
	q := validQuotaShape(
		v1alpha1.QuotaEntity{ClientId: "   "},
		v1alpha1.QuotaLimits{ProducerByteRate: fv(1024)},
	)
	errs := ValidateQuotaShape(q)
	require.True(t, errorsContain(errs, "clientId"), "got %v", errs)
	require.True(t, errorsContain(errs, "blank"), "got %v", errs)
}

func TestValidateQuotaShape_NoLimits(t *testing.T) {
	q := validQuotaShape(
		v1alpha1.QuotaEntity{User: "User:svc"},
		v1alpha1.QuotaLimits{},
	)
	errs := ValidateQuotaShape(q)
	require.True(t, errorsContain(errs, "at least one"), "got %v", errs)
}

func TestValidateQuotaShape_NegativeLimit(t *testing.T) {
	q := validQuotaShape(
		v1alpha1.QuotaEntity{User: "User:svc"},
		v1alpha1.QuotaLimits{ProducerByteRate: fv(-1)},
	)
	errs := ValidateQuotaShape(q)
	require.True(t, errorsContain(errs, ">= 0"), "got %v", errs)
}

func TestValidateQuotaShape_IP(t *testing.T) {
	// positive cases
	q := validQuotaShape(v1alpha1.QuotaEntity{Ip: "10.0.0.1"}, v1alpha1.QuotaLimits{ConnectionCreationRate: fv(100)})
	require.Empty(t, ValidateQuotaShape(q), "valid IPv4 ip quota")

	q = validQuotaShape(v1alpha1.QuotaEntity{Ip: "2001:db8::1"}, v1alpha1.QuotaLimits{ConnectionCreationRate: fv(100)})
	require.Empty(t, ValidateQuotaShape(q), "valid IPv6 ip quota")

	q = validQuotaShape(v1alpha1.QuotaEntity{IpDefault: true}, v1alpha1.QuotaLimits{ConnectionCreationRate: fv(100)})
	require.Empty(t, ValidateQuotaShape(q), "valid ipDefault quota")

	// negative: ip + user combined (separate quota dimension)
	errs := ValidateQuotaShape(validQuotaShape(
		v1alpha1.QuotaEntity{Ip: "10.0.0.1", User: "User:a"},
		v1alpha1.QuotaLimits{ConnectionCreationRate: fv(100)},
	))
	require.True(t, errorsContain(errs, "separate quota dimension"), "ip + user must error, got %v", errs)

	// negative: ip + ipDefault mutually exclusive
	errs = ValidateQuotaShape(validQuotaShape(
		v1alpha1.QuotaEntity{Ip: "10.0.0.1", IpDefault: true},
		v1alpha1.QuotaLimits{ConnectionCreationRate: fv(100)},
	))
	require.True(t, errorsContain(errs, "ip and spec.entity.ipDefault are mutually exclusive"), "ip + ipDefault must error, got %v", errs)

	// negative: bad IP literals
	for _, bad := range []string{"10.0.0.0/24", "not-an-ip", "host.example.com", "10.0.0.256", "  "} {
		errs = ValidateQuotaShape(validQuotaShape(
			v1alpha1.QuotaEntity{Ip: bad},
			v1alpha1.QuotaLimits{ConnectionCreationRate: fv(100)},
		))
		require.True(t, errorsContain(errs, "must be a valid IPv4 or IPv6"), "invalid ip %q must error, got %v", bad, errs)
	}

	// negative: ip entity with throughput limit (limit partitioning)
	errs = ValidateQuotaShape(validQuotaShape(
		v1alpha1.QuotaEntity{Ip: "10.0.0.1"},
		v1alpha1.QuotaLimits{ProducerByteRate: fv(1)},
	))
	require.True(t, errorsContain(errs, "only connectionCreationRate"), "ip + throughput limit must error, got %v", errs)

	// negative: user entity with connectionCreationRate (limit partitioning)
	errs = ValidateQuotaShape(validQuotaShape(
		v1alpha1.QuotaEntity{User: "User:a"},
		v1alpha1.QuotaLimits{ConnectionCreationRate: fv(100)},
	))
	require.True(t, errorsContain(errs, "connectionCreationRate is valid only on an ip entity"), "user + connectionCreationRate must error, got %v", errs)

	// negative: negative connectionCreationRate
	errs = ValidateQuotaShape(validQuotaShape(
		v1alpha1.QuotaEntity{Ip: "10.0.0.1"},
		v1alpha1.QuotaLimits{ConnectionCreationRate: fv(-1)},
	))
	require.True(t, errorsContain(errs, "connectionCreationRate must be >= 0"), "negative connectionCreationRate must error, got %v", errs)

	// negative: ip entity with no limit
	errs = ValidateQuotaShape(validQuotaShape(
		v1alpha1.QuotaEntity{Ip: "10.0.0.1"},
		v1alpha1.QuotaLimits{},
	))
	require.True(t, errorsContain(errs, "at least one of"), "ip entity with no limit must error, got %v", errs)
}

// ---- ValidateAccessPolicyShape unit tests (spec §20.3) ----

// validAccessPolicyShape returns a minimal valid KafkaAccessPolicy for shape tests.
func validAccessPolicyShape() *v1alpha1.KafkaAccessPolicy {
	return &v1alpha1.KafkaAccessPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaAccessPolicy"},
		ObjectMeta: metav1.ObjectMeta{Name: "p"},
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Rules: []v1alpha1.ACLRule{
				{Principal: "User:svc", Resource: v1alpha1.ACLResource{Type: "topic", Name: "orders"}, Operations: []string{"Read"}},
			},
		},
	}
}

func TestValidateAccessPolicyShape_ValidPasses(t *testing.T) {
	require.Empty(t, ValidateAccessPolicyShape(validAccessPolicyShape()))
}

func TestValidateAccessPolicyShape_BadAPIVersion(t *testing.T) {
	pol := validAccessPolicyShape()
	pol.APIVersion = "gitops.monedula.dev/v1beta1"
	errs := ValidateAccessPolicyShape(pol)
	require.True(t, errorsContain(errs, "apiVersion must be"), "got %v", errs)
}

func TestValidateAccessPolicyShape_EmptyRules(t *testing.T) {
	pol := validAccessPolicyShape()
	pol.Spec.Rules = nil
	errs := ValidateAccessPolicyShape(pol)
	require.True(t, errorsContain(errs, "spec.rules must not be empty"), "got %v", errs)
}

func TestValidateAccessPolicyShape_BadDeletionPolicy(t *testing.T) {
	pol := validAccessPolicyShape()
	pol.Spec.DeletionPolicy = "Nope"
	errs := ValidateAccessPolicyShape(pol)
	require.True(t, errorsContain(errs, `invalid deletionPolicy "Nope"`), "got %v", errs)
}

func TestValidateAccessPolicyShape_BadReconciliationMode(t *testing.T) {
	pol := validAccessPolicyShape()
	pol.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: "Bogus"}
	errs := ValidateAccessPolicyShape(pol)
	require.True(t, errorsContain(errs, `invalid reconciliation.mode "Bogus"`), "got %v", errs)
}

func TestValidateAccessPolicyShape_EmptyPrincipal(t *testing.T) {
	pol := validAccessPolicyShape()
	pol.Spec.Rules[0].Principal = ""
	errs := ValidateAccessPolicyShape(pol)
	require.True(t, errorsContain(errs, "principal required"), "got %v", errs)
}

func TestValidateAccessPolicyShape_BadResourceType(t *testing.T) {
	pol := validAccessPolicyShape()
	pol.Spec.Rules[0].Resource.Type = "bogus"
	errs := ValidateAccessPolicyShape(pol)
	require.True(t, errorsContain(errs, `invalid resource type "bogus"`), "got %v", errs)
}

func TestValidateAccessPolicyShape_EmptyResourceName(t *testing.T) {
	pol := validAccessPolicyShape()
	pol.Spec.Rules[0].Resource.Name = ""
	errs := ValidateAccessPolicyShape(pol)
	require.True(t, errorsContain(errs, "resource name required"), "got %v", errs)
}

func TestValidateAccessPolicyShape_BadPatternType(t *testing.T) {
	pol := validAccessPolicyShape()
	pol.Spec.Rules[0].Resource.PatternType = "glob"
	errs := ValidateAccessPolicyShape(pol)
	require.True(t, errorsContain(errs, `invalid patternType "glob"`), "got %v", errs)
}

func TestValidateAccessPolicyShape_BadPermission(t *testing.T) {
	pol := validAccessPolicyShape()
	pol.Spec.Rules[0].Permission = "Maybe"
	errs := ValidateAccessPolicyShape(pol)
	require.True(t, errorsContain(errs, `invalid permission "Maybe"`), "got %v", errs)
}

func TestValidateAccessPolicyShape_NoOperations(t *testing.T) {
	pol := validAccessPolicyShape()
	pol.Spec.Rules[0].Operations = nil
	errs := ValidateAccessPolicyShape(pol)
	require.True(t, errorsContain(errs, "at least one operation required"), "got %v", errs)
}

func TestValidateAccessPolicyShape_InvalidOperation(t *testing.T) {
	pol := validAccessPolicyShape()
	pol.Spec.Rules[0].Operations = []string{"Nope"}
	errs := ValidateAccessPolicyShape(pol)
	require.True(t, errorsContain(errs, `invalid operation "Nope"`), "got %v", errs)
}

func TestValidateAccessPolicyShape_OperationCaseVariantNamesCanonical(t *testing.T) {
	pol := validAccessPolicyShape()
	pol.Spec.Rules[0].Operations = []string{"WRITE"}
	errs := ValidateAccessPolicyShape(pol)
	require.True(t, errorsContain(errs, `"WRITE"`), "got %v", errs)
	require.True(t, errorsContain(errs, `"Write"`), "error must name the canonical form, got %v", errs)
}

// TestValidate_PolicyShapeErrors_BehaviorPreserved confirms the top-level Validate
// still produces the same shape errors as before the refactor (behavior-preserving).
func TestValidate_PolicyShapeErrors_BehaviorPreserved(t *testing.T) {
	pol := &v1alpha1.KafkaAccessPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaAccessPolicy"},
		ObjectMeta: metav1.ObjectMeta{Name: "p"},
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef:     v1alpha1.ClusterRef{Name: "prod"},
			DeletionPolicy: "Nope",
			Reconciliation: &v1alpha1.Reconciliation{Mode: "Bogus"},
			Rules: []v1alpha1.ACLRule{
				{Principal: "", Resource: v1alpha1.ACLResource{Type: "bogus", Name: ""}, Operations: []string{"WRITE"}},
			},
		},
	}
	errs := Validate(Input{
		Policies: []*v1alpha1.KafkaAccessPolicy{pol},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.True(t, errorsContain(errs, `invalid deletionPolicy "Nope"`), "got %v", errs)
	require.True(t, errorsContain(errs, `invalid reconciliation.mode "Bogus"`), "got %v", errs)
	require.True(t, errorsContain(errs, "principal required"), "got %v", errs)
	require.True(t, errorsContain(errs, `invalid resource type "bogus"`), "got %v", errs)
	require.True(t, errorsContain(errs, "resource name required"), "got %v", errs)
	require.True(t, errorsContain(errs, `"WRITE"`), "got %v", errs)
	require.True(t, errorsContain(errs, `"Write"`), "got %v", errs)
}

// ---- top-level Validate with quotas (spec §39.5) ----

func TestValidate_QuotaUnresolvedClusterRef(t *testing.T) {
	q := mkQuota("q1", "missing")
	errs := Validate(Input{
		Quotas:   []*v1alpha1.KafkaQuota{q},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.True(t, errorsContain(errs, `"missing"`), "expected unresolved cluster error, got %v", errs)
}

func TestValidate_QuotaIdentityCollision(t *testing.T) {
	q1 := mkQuota("q1", "prod")
	q2 := mkQuota("q2", "prod")
	// Both use User:svc on the same cluster → collision.
	errs := Validate(Input{
		Quotas:   []*v1alpha1.KafkaQuota{q1, q2},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.True(t, errorsContain(errs, "q1"), "collision error must name first resource, got %v", errs)
	require.True(t, errorsContain(errs, "q2"), "collision error must name second resource, got %v", errs)
}

func TestValidate_QuotaDifferentEntitiesNoCollision(t *testing.T) {
	q1 := mkQuota("q1", "prod")
	q2 := mkQuota("q2", "prod")
	q2.Spec.Entity = v1alpha1.QuotaEntity{User: "User:other"}
	errs := Validate(Input{
		Quotas:   []*v1alpha1.KafkaQuota{q1, q2},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.Empty(t, errs)
}

func TestValidate_QuotaNilClustersSkipsClusterCheck(t *testing.T) {
	q := mkQuota("q1", "whatever")
	// Clusters nil => shape only, unknown ref not checked.
	errs := Validate(Input{Quotas: []*v1alpha1.KafkaQuota{q}})
	require.Empty(t, errs)
}

// ---- ValidateRoleBindingShape unit tests (spec §40) ----

// validRoleBindingShape returns a minimal valid resource-scoped KafkaRoleBinding.
func validRoleBindingShape() *v1alpha1.KafkaRoleBinding {
	return &v1alpha1.KafkaRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "rb"},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Principal:  "User:svc",
			Role:       "DeveloperRead",
			Scope:      v1alpha1.RoleBindingScope{Type: "kafka"},
			Resources: []v1alpha1.RoleResource{
				{Type: "Topic", Name: "orders"},
			},
		},
	}
}

// validClusterScopedRoleBinding returns a minimal valid cluster-scoped binding.
func validClusterScopedRoleBinding() *v1alpha1.KafkaRoleBinding {
	return &v1alpha1.KafkaRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "rb"},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Principal:  "User:admin",
			Role:       "SystemAdmin",
			Scope:      v1alpha1.RoleBindingScope{Type: "kafka"},
		},
	}
}

// mdsCluster returns a KafkaCluster with a full MDSConfig (all sub-cluster IDs populated).
func mdsCluster() *v1alpha1.KafkaCluster {
	cl := cfgCluster()
	cl.Spec.Authorization = &v1alpha1.AuthorizationConfig{
		MDS: &v1alpha1.MDSConfig{
			Endpoint: "https://mds.example.com",
			Clusters: v1alpha1.MDSClusters{
				KafkaCluster:          "lkc-kafka",
				SchemaRegistryCluster: "lsrc-sr",
				ConnectCluster:        "lcc-connect",
				KsqlCluster:           "lksql-ksql",
			},
		},
	}
	return cl
}

func TestValidateRoleBindingShape_ResourceScopedPasses(t *testing.T) {
	require.Empty(t, ValidateRoleBindingShape(validRoleBindingShape()))
}

func TestValidateRoleBindingShape_ClusterScopedPasses(t *testing.T) {
	require.Empty(t, ValidateRoleBindingShape(validClusterScopedRoleBinding()))
}

func TestValidateRoleBindingShape_GroupPrincipalPasses(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Principal = "Group:my-team"
	require.Empty(t, ValidateRoleBindingShape(rb))
}

func TestValidateRoleBindingShape_BadAPIVersion(t *testing.T) {
	rb := validRoleBindingShape()
	rb.APIVersion = "gitops.monedula.dev/v1beta1"
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, "apiVersion must be"), "got %v", errs)
}

func TestValidateRoleBindingShape_EmptyPrincipal(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Principal = ""
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, "spec.principal must be non-empty"), "got %v", errs)
}

func TestValidateRoleBindingShape_PrincipalNoPrefix(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Principal = "svc-checkout"
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, `"User:<name>" or "Group:<name>"`), "got %v", errs)
}

func TestValidateRoleBindingShape_UserPrefixEmptyName(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Principal = "User:"
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, "non-empty name after it"), "got %v", errs)
}

func TestValidateRoleBindingShape_GroupPrefixEmptyName(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Principal = "Group:"
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, "non-empty name after it"), "got %v", errs)
}

func TestValidateRoleBindingShape_EmptyRole(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Role = ""
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, "spec.role must be non-empty"), "got %v", errs)
}

func TestValidateRoleBindingShape_BadScopeType(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Scope.Type = "bogus"
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, `invalid scope.type "bogus"`), "got %v", errs)
}

func TestValidateRoleBindingShape_BadDeletionPolicy(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.DeletionPolicy = "Nope"
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, `invalid deletionPolicy "Nope"`), "got %v", errs)
}

func TestValidateRoleBindingShape_BadReconciliationMode(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: "Bogus"}
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, `invalid reconciliation.mode "Bogus"`), "got %v", errs)
}

func TestValidateRoleBindingShape_EmptyResourceName(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Resources[0].Name = ""
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, "name must be non-empty"), "got %v", errs)
}

func TestValidateRoleBindingShape_BadPatternType(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Resources[0].PatternType = "glob"
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, `invalid patternType "glob"`), "got %v", errs)
}

func TestValidateRoleBindingShape_LiteralPatternTypePasses(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Resources[0].PatternType = "literal"
	require.Empty(t, ValidateRoleBindingShape(rb))
}

func TestValidateRoleBindingShape_PrefixedPatternTypePasses(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Resources[0].PatternType = "prefixed"
	require.Empty(t, ValidateRoleBindingShape(rb))
}

func TestValidateRoleBindingShape_DefaultPatternTypeAllowed(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Resources[0].PatternType = "" // default (literal)
	require.Empty(t, ValidateRoleBindingShape(rb))
}

func TestValidateRoleBindingShape_InvalidResourceTypeForScope(t *testing.T) {
	rb := validRoleBindingShape()
	// kafka scope does not have "Subject" as a valid resource type
	rb.Spec.Resources[0].Type = "Subject"
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, `invalid resource type "Subject"`), "got %v", errs)
	require.True(t, errorsContain(errs, `scope.type "kafka"`), "got %v", errs)
}

func TestValidateRoleBindingShape_SRScopePasses(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Scope.Type = "schema-registry"
	rb.Spec.Resources[0].Type = "Subject"
	require.Empty(t, ValidateRoleBindingShape(rb))
}

func TestValidateRoleBindingShape_ConnectScopePasses(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Scope.Type = "connect"
	rb.Spec.Resources[0].Type = "Connector"
	require.Empty(t, ValidateRoleBindingShape(rb))
}

func TestValidateRoleBindingShape_KsqlScopePasses(t *testing.T) {
	rb := &v1alpha1.KafkaRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "rb"},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Principal:  "User:svc",
			Role:       "DeveloperRead",
			Scope:      v1alpha1.RoleBindingScope{Type: "ksql"},
			Resources: []v1alpha1.RoleResource{
				{Type: "KsqlCluster", Name: "k1"},
			},
		},
	}
	require.Empty(t, ValidateRoleBindingShape(rb))
}

func TestValidateRoleBindingShape_ClusterScopedWithResourcesIsError(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Role = "SystemAdmin" // cluster-scoped
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, "cluster-scoped"), "got %v", errs)
	require.True(t, errorsContain(errs, "must not have spec.resources"), "got %v", errs)
}

func TestValidateRoleBindingShape_ResourceScopedWithoutResourcesIsError(t *testing.T) {
	rb := validClusterScopedRoleBinding()
	rb.Spec.Role = "DeveloperRead" // resource-scoped
	// no resources set
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, "resource-scoped"), "got %v", errs)
	require.True(t, errorsContain(errs, "requires at least one entry"), "got %v", errs)
}

func TestValidateRoleBindingShape_UnknownRoleNoError(t *testing.T) {
	// Unknown roles must NOT produce an error (decision 18: warning, not error).
	// Resource-presence is not enforced for unknown roles.
	rb := validRoleBindingShape()
	rb.Spec.Role = "FutureRole" // unknown
	rb.Spec.Resources = nil     // no resources — would error for resource-scoped, but unknown → skip
	errs := ValidateRoleBindingShape(rb)
	require.Empty(t, errs, "unknown role must not produce an error, got %v", errs)
}

func TestValidateRoleBindingShape_UnknownRoleWithResourcesNoError(t *testing.T) {
	// Unknown role with resources: shape checks on resources still run, but no
	// resource-presence enforcement error.
	rb := validRoleBindingShape()
	rb.Spec.Role = "FutureRole"
	errs := ValidateRoleBindingShape(rb)
	require.Empty(t, errs, "unknown role with valid resources must not produce an error, got %v", errs)
}

func TestValidateRoleBindingShape_EmptyResourceType(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Resources[0].Type = ""
	errs := ValidateRoleBindingShape(rb)
	require.True(t, errorsContain(errs, "type must be non-empty"), "got %v", errs)
}

// ---- top-level Validate with RoleBindings (spec §40) ----

func TestValidate_RoleBindingUnresolvedClusterRef(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.ClusterRef.Name = "missing"
	errs := Validate(Input{
		RoleBindings: []*v1alpha1.KafkaRoleBinding{rb},
		Clusters:     map[string]*v1alpha1.KafkaCluster{"prod": mdsCluster()},
	})
	require.True(t, errorsContain(errs, `"missing"`), "expected unresolved cluster error, got %v", errs)
}

func TestValidate_RoleBindingClusterWithoutMDSIsError(t *testing.T) {
	rb := validRoleBindingShape()
	errs := Validate(Input{
		RoleBindings: []*v1alpha1.KafkaRoleBinding{rb},
		Clusters:     map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()}, // no MDS
	})
	require.True(t, errorsContain(errs, "authorization.mds"), "expected MDS-required error, got %v", errs)
}

func TestValidate_RoleBindingScopeIdMissingSRIsError(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Scope.Type = "schema-registry"
	rb.Spec.Resources[0].Type = "Subject"
	cl := cfgCluster()
	cl.Spec.Authorization = &v1alpha1.AuthorizationConfig{
		MDS: &v1alpha1.MDSConfig{
			Endpoint: "https://mds.example.com",
			Clusters: v1alpha1.MDSClusters{
				KafkaCluster: "lkc-kafka",
				// SchemaRegistryCluster intentionally omitted
			},
		},
	}
	errs := Validate(Input{
		RoleBindings: []*v1alpha1.KafkaRoleBinding{rb},
		Clusters:     map[string]*v1alpha1.KafkaCluster{"prod": cl},
	})
	require.True(t, errorsContain(errs, "schemaRegistryCluster"), "expected scope-id error, got %v", errs)
}

func TestValidate_RoleBindingScopeIdMissingConnectIsError(t *testing.T) {
	rb := validRoleBindingShape()
	rb.Spec.Scope.Type = "connect"
	rb.Spec.Resources[0].Type = "Connector"
	cl := cfgCluster()
	cl.Spec.Authorization = &v1alpha1.AuthorizationConfig{
		MDS: &v1alpha1.MDSConfig{
			Endpoint: "https://mds.example.com",
			Clusters: v1alpha1.MDSClusters{
				KafkaCluster: "lkc-kafka",
				// ConnectCluster intentionally omitted
			},
		},
	}
	errs := Validate(Input{
		RoleBindings: []*v1alpha1.KafkaRoleBinding{rb},
		Clusters:     map[string]*v1alpha1.KafkaCluster{"prod": cl},
	})
	require.True(t, errorsContain(errs, "connectCluster"), "expected scope-id error, got %v", errs)
}

func TestValidate_RoleBindingScopeIdMissingKsqlIsError(t *testing.T) {
	rb := &v1alpha1.KafkaRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "rb"},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Principal:  "User:svc",
			Role:       "DeveloperRead",
			Scope:      v1alpha1.RoleBindingScope{Type: "ksql"},
			Resources: []v1alpha1.RoleResource{
				{Type: "KsqlCluster", Name: "k1"},
			},
		},
	}
	cl := cfgCluster()
	cl.Spec.Authorization = &v1alpha1.AuthorizationConfig{
		MDS: &v1alpha1.MDSConfig{
			Endpoint: "https://mds.example.com",
			Clusters: v1alpha1.MDSClusters{
				KafkaCluster: "lkc-kafka",
				// KsqlCluster intentionally omitted
			},
		},
	}
	errs := Validate(Input{
		RoleBindings: []*v1alpha1.KafkaRoleBinding{rb},
		Clusters:     map[string]*v1alpha1.KafkaCluster{"prod": cl},
	})
	require.True(t, errorsContain(errs, "ksqlCluster"), "expected scope-id error, got %v", errs)
}

func TestValidate_RoleBindingKafkaClusterMissingIsError(t *testing.T) {
	rb := validRoleBindingShape() // kafka scope, valid resources
	cl := cfgCluster()
	cl.Spec.Authorization = &v1alpha1.AuthorizationConfig{
		MDS: &v1alpha1.MDSConfig{
			Endpoint: "https://mds.example.com",
			Clusters: v1alpha1.MDSClusters{
				// KafkaCluster intentionally omitted
			},
		},
	}
	errs := Validate(Input{
		RoleBindings: []*v1alpha1.KafkaRoleBinding{rb},
		Clusters:     map[string]*v1alpha1.KafkaCluster{"prod": cl},
	})
	require.True(t, errorsContain(errs, "kafkaCluster"), "expected kafkaCluster scope-id error, got %v", errs)
}

func TestValidate_RoleBindingIdentityCollisionIsError(t *testing.T) {
	rb1 := validRoleBindingShape()
	rb1.Name = "rb1"
	rb2 := validRoleBindingShape()
	rb2.Name = "rb2"
	// Both bind the same (principal, role, scope, resource) on the same cluster.
	errs := Validate(Input{
		RoleBindings: []*v1alpha1.KafkaRoleBinding{rb1, rb2},
		Clusters:     map[string]*v1alpha1.KafkaCluster{"prod": mdsCluster()},
	})
	require.True(t, errorsContain(errs, "rb1"), "collision error must name first resource, got %v", errs)
	require.True(t, errorsContain(errs, "rb2"), "collision error must name second resource, got %v", errs)
}

func TestValidate_RoleBindingDifferentPrincipalsNoCollision(t *testing.T) {
	rb1 := validRoleBindingShape()
	rb1.Name = "rb1"
	rb2 := validRoleBindingShape()
	rb2.Name = "rb2"
	rb2.Spec.Principal = "User:other"
	errs := Validate(Input{
		RoleBindings: []*v1alpha1.KafkaRoleBinding{rb1, rb2},
		Clusters:     map[string]*v1alpha1.KafkaCluster{"prod": mdsCluster()},
	})
	require.Empty(t, errs)
}

func TestValidate_RoleBindingValidSetPasses(t *testing.T) {
	rb := validRoleBindingShape()
	errs := Validate(Input{
		RoleBindings: []*v1alpha1.KafkaRoleBinding{rb},
		Clusters:     map[string]*v1alpha1.KafkaCluster{"prod": mdsCluster()},
	})
	require.Empty(t, errs)
}

func TestValidate_RoleBindingNilClustersSkipsClusterCheck(t *testing.T) {
	rb := validRoleBindingShape()
	// Clusters nil => shape only, unknown ref not checked.
	errs := Validate(Input{RoleBindings: []*v1alpha1.KafkaRoleBinding{rb}})
	require.Empty(t, errs)
}

// ---- ValidateClusterAuthorization (spec §40) ----

func TestValidateClusterAuthorizationBackends(t *testing.T) {
	withMDS := func(backends ...string) *v1alpha1.KafkaCluster {
		return &v1alpha1.KafkaCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "prod"},
			Spec: v1alpha1.KafkaClusterSpec{
				Authorization: &v1alpha1.AuthorizationConfig{
					AccessBackends: backends,
					MDS:            &v1alpha1.MDSConfig{Endpoint: "https://mds", Clusters: v1alpha1.MDSClusters{KafkaCluster: "kid"}},
				},
			},
		}
	}
	noMDS := func(backends ...string) *v1alpha1.KafkaCluster {
		return &v1alpha1.KafkaCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "prod"},
			Spec:       v1alpha1.KafkaClusterSpec{Authorization: &v1alpha1.AuthorizationConfig{AccessBackends: backends}},
		}
	}

	require.Empty(t, ValidateClusterAuthorization(nil), "nil cluster → no errors")
	require.Empty(t, ValidateClusterAuthorization(&v1alpha1.KafkaCluster{}), "nil authorization → no errors")
	require.Empty(t, ValidateClusterAuthorization(withMDS("acl", "rbac")), "valid [acl,rbac] with MDS: unexpected errors")
	require.Empty(t, ValidateClusterAuthorization(noMDS()), "default (no backends, no MDS): unexpected errors")
	require.Empty(t, ValidateClusterAuthorization(noMDS("acl")), "[acl] without MDS is valid: unexpected errors")
	require.NotEmpty(t, ValidateClusterAuthorization(noMDS("rbac")), "[rbac] without MDS must error")
	require.NotEmpty(t, ValidateClusterAuthorization(withMDS("acl", "kerberos")), "invalid backend value must error")
}

func TestValidate_RoleBindingMultiResourceCollisionIsError(t *testing.T) {
	// rb1 has two resources; rb2 has one that overlaps with rb1's first resource.
	rb1 := &v1alpha1.KafkaRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "rb1"},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Principal:  "User:svc",
			Role:       "DeveloperRead",
			Scope:      v1alpha1.RoleBindingScope{Type: "kafka"},
			Resources: []v1alpha1.RoleResource{
				{Type: "Topic", Name: "orders"},
				{Type: "Topic", Name: "payments"},
			},
		},
	}
	rb2 := &v1alpha1.KafkaRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "rb2"},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Principal:  "User:svc",
			Role:       "DeveloperRead",
			Scope:      v1alpha1.RoleBindingScope{Type: "kafka"},
			Resources: []v1alpha1.RoleResource{
				{Type: "Topic", Name: "orders"}, // collides with rb1's first resource
			},
		},
	}
	errs := Validate(Input{
		RoleBindings: []*v1alpha1.KafkaRoleBinding{rb1, rb2},
		Clusters:     map[string]*v1alpha1.KafkaCluster{"prod": mdsCluster()},
	})
	require.True(t, errorsContain(errs, "rb1"), "collision error must name rb1, got %v", errs)
	require.True(t, errorsContain(errs, "rb2"), "collision error must name rb2, got %v", errs)
}

// ---- schemaRegistry.tls shape rules (same as spec.tls) ----

// srTLSValidationCluster builds a cluster whose schemaRegistry block carries
// the given TLS config, for the schemaRegistry.tls shape tests.
func srTLSValidationCluster(tls *v1alpha1.TLSConfig) *v1alpha1.KafkaCluster {
	cl := cfgCluster()
	cl.Spec.SchemaRegistry = &v1alpha1.SchemaRegistryConf{
		Endpoint: "https://sr:8081",
		TLS:      tls,
	}
	return cl
}

func TestSchemaRegistryTLSCertWithoutKeyIsError(t *testing.T) {
	cl := srTLSValidationCluster(&v1alpha1.TLSConfig{
		Enabled:    true,
		ClientCert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.crt"}},
		// ClientKey intentionally omitted
	})
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "schemaRegistry.tls.clientCert and schemaRegistry.tls.clientKey must be set together"),
		"expected SR cert/key pairing error, got %v", errs)
}

func TestSchemaRegistryTLSKeyWithoutCertIsError(t *testing.T) {
	cl := srTLSValidationCluster(&v1alpha1.TLSConfig{
		Enabled: true,
		// ClientCert intentionally omitted
		ClientKey: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.key"}},
	})
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "schemaRegistry.tls.clientCert and schemaRegistry.tls.clientKey must be set together"),
		"expected SR cert/key pairing error, got %v", errs)
}

func TestSchemaRegistryTLSCertsRequireEnabled(t *testing.T) {
	cl := srTLSValidationCluster(&v1alpha1.TLSConfig{
		Enabled:    false,
		ClientCert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.crt"}},
		ClientKey:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.key"}},
	})
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "schemaRegistry.tls.clientCert/schemaRegistry.tls.clientKey require schemaRegistry.tls.enabled: true"),
		"expected SR cert/enabled error, got %v", errs)
}

func TestSchemaRegistryTLSCACertOnlyIsLegal(t *testing.T) {
	cl := srTLSValidationCluster(&v1alpha1.TLSConfig{
		Enabled: true,
		CACert:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "ca.crt"}},
	})
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.Empty(t, errs, "SR tls with caCert only must be legal: %v", errs)
}

// ---- authorization.mds.tls shape rules (same as spec.tls / schemaRegistry.tls) ----

// mdsTLSValidationCluster builds a cluster whose authorization.mds block carries
// the given TLS config, for the authorization.mds.tls shape tests.
func mdsTLSValidationCluster(tls *v1alpha1.TLSConfig) *v1alpha1.KafkaCluster {
	cl := mdsCluster()
	cl.Spec.Authorization.MDS.TLS = tls
	return cl
}

func TestMDSTLSCertWithoutKeyIsError(t *testing.T) {
	cl := mdsTLSValidationCluster(&v1alpha1.TLSConfig{
		Enabled:    true,
		ClientCert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.crt"}},
		// ClientKey intentionally omitted
	})
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "authorization.mds.tls.clientCert and authorization.mds.tls.clientKey must be set together"),
		"expected MDS cert/key pairing error, got %v", errs)
}

func TestMDSTLSKeyWithoutCertIsError(t *testing.T) {
	cl := mdsTLSValidationCluster(&v1alpha1.TLSConfig{
		Enabled: true,
		// ClientCert intentionally omitted
		ClientKey: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.key"}},
	})
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "authorization.mds.tls.clientCert and authorization.mds.tls.clientKey must be set together"),
		"expected MDS cert/key pairing error, got %v", errs)
}

func TestMDSTLSCertsRequireEnabled(t *testing.T) {
	cl := mdsTLSValidationCluster(&v1alpha1.TLSConfig{
		Enabled:    false,
		ClientCert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.crt"}},
		ClientKey:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.key"}},
	})
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.True(t, errorsContain(errs, "authorization.mds.tls.clientCert/authorization.mds.tls.clientKey require authorization.mds.tls.enabled: true"),
		"expected MDS cert/enabled error, got %v", errs)
}

func TestMDSTLSCACertOnlyIsLegal(t *testing.T) {
	cl := mdsTLSValidationCluster(&v1alpha1.TLSConfig{
		Enabled: true,
		CACert:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "ca.crt"}},
	})
	errs := Validate(Input{Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cl}})
	require.Empty(t, errs, "MDS tls with caCert only must be legal: %v", errs)
}

// ---- ValidateUserShape unit tests (v0.35 §T2) ----

func userI32(v int32) *int32 { return &v }

// validUserShape returns a minimal valid KafkaUser (env-sourced password).
func validUserShape() *v1alpha1.KafkaUser {
	return &v1alpha1.KafkaUser{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaUser"},
		ObjectMeta: metav1.ObjectMeta{Name: "svc-checkout"},
		Spec: v1alpha1.KafkaUserSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Username:   "svc-checkout",
			Mechanism:  "SCRAM-SHA-512",
			Password: &v1alpha1.UserPassword{
				ValueFrom: &v1alpha1.ValueSource{Env: "SVC_CHECKOUT_PASSWORD"},
			},
		},
	}
}

func mkUser(name, cluster, username string) *v1alpha1.KafkaUser {
	u := validUserShape()
	u.Name = name
	u.Spec.ClusterRef.Name = cluster
	u.Spec.Username = username
	return u
}

func TestValidateUserShape_Valid(t *testing.T) {
	require.Empty(t, ValidateUserShape(validUserShape()))
}

func TestValidateUserShape_PasswordRequired(t *testing.T) {
	u := validUserShape()
	u.Spec.Password = nil
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "spec.password is required"), "got %v", errs)
	require.True(t, errorsContain(errs, "valueFrom"), "message should name valueFrom, got %v", errs)
	require.True(t, errorsContain(errs, "generate"), "message should name generate, got %v", errs)
}

func TestValidateUserShape_PasswordNeitherSourceSet(t *testing.T) {
	u := validUserShape()
	u.Spec.Password = &v1alpha1.UserPassword{}
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "neither set"), "got %v", errs)
}

func TestValidateUserShape_PasswordBothSourcesSet(t *testing.T) {
	u := validUserShape()
	u.Spec.Password = &v1alpha1.UserPassword{
		ValueFrom: &v1alpha1.ValueSource{Env: "PW"},
		Generate:  &v1alpha1.GeneratePassword{},
	}
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "both set"), "got %v", errs)
}

func TestValidateUserShape_PasswordGenerateAloneIsShapeValid(t *testing.T) {
	u := validUserShape()
	u.Spec.Password = &v1alpha1.UserPassword{Generate: &v1alpha1.GeneratePassword{}}
	// generate is operator-only, but that is enforced by the CLI pipeline, not
	// core shape validation (the webhook/reconciler path must accept it).
	require.Empty(t, ValidateUserShape(u))
}

func TestValidateUserShape_PasswordValueFromSecretKeyRefIsShapeValid(t *testing.T) {
	u := validUserShape()
	u.Spec.Password = &v1alpha1.UserPassword{
		ValueFrom: &v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "creds", Key: "password"}},
	}
	require.Empty(t, ValidateUserShape(u))
}

func TestValidateUserShape_PasswordValueFromFileIsValid(t *testing.T) {
	u := validUserShape()
	u.Spec.Password = &v1alpha1.UserPassword{ValueFrom: &v1alpha1.ValueSource{File: "password.txt"}}
	require.Empty(t, ValidateUserShape(u))
}

func TestValidateUserShape_PasswordInlineRejected(t *testing.T) {
	u := validUserShape()
	u.Spec.Password = &v1alpha1.UserPassword{ValueFrom: &v1alpha1.ValueSource{Inline: "hunter2"}}
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "inline"), "got %v", errs)
	require.True(t, errorsContain(errs, "plaintext"), "message should explain the git-plaintext risk, got %v", errs)
}

func TestValidateUserShape_PasswordConfigMapKeyRefRejected(t *testing.T) {
	u := validUserShape()
	u.Spec.Password = &v1alpha1.UserPassword{ValueFrom: &v1alpha1.ValueSource{
		ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: "cm", Key: "password"},
	}}
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "configMapKeyRef"), "got %v", errs)
	require.True(t, errorsContain(errs, "not allowed for passwords"), "got %v", errs)
	require.True(t, errorsContain(errs, "secretKeyRef"), "message should point to the allowed alternative, got %v", errs)
}

func TestValidateUserShape_PasswordValueFromNoSourceSet(t *testing.T) {
	u := validUserShape()
	u.Spec.Password = &v1alpha1.UserPassword{ValueFrom: &v1alpha1.ValueSource{}}
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "none set"), "got %v", errs)
}

func TestValidateUserShape_PasswordValueFromMultipleSourcesSet(t *testing.T) {
	u := validUserShape()
	u.Spec.Password = &v1alpha1.UserPassword{ValueFrom: &v1alpha1.ValueSource{Env: "PW", File: "pw.txt"}}
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "2 set"), "got %v", errs)
}

func TestValidateUserShape_UsernameEmpty(t *testing.T) {
	u := validUserShape()
	u.Spec.Username = ""
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "spec.username must be non-empty"), "got %v", errs)
}

func TestValidateUserShape_UsernameWhitespaceRejected(t *testing.T) {
	u := validUserShape()
	u.Spec.Username = "svc checkout"
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "forbidden character"), "got %v", errs)
}

func TestValidateUserShape_UsernameControlCharRejected(t *testing.T) {
	u := validUserShape()
	u.Spec.Username = "svc\tcheckout"
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "forbidden character"), "got %v", errs)
}

func TestValidateUserShape_UsernameNULRejected(t *testing.T) {
	u := validUserShape()
	u.Spec.Username = "svc\x00checkout"
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "forbidden character"), "got %v", errs)
}

func TestValidateUserShape_UsernameEqualsRejected(t *testing.T) {
	u := validUserShape()
	u.Spec.Username = "svc=checkout"
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "forbidden character"), "got %v", errs)
}

func TestValidateUserShape_UsernameCommaRejected(t *testing.T) {
	u := validUserShape()
	u.Spec.Username = "svc,checkout"
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "forbidden character"), "got %v", errs)
}

func TestValidateUserShape_UsernamePermissiveOtherwise(t *testing.T) {
	u := validUserShape()
	u.Spec.Username = "svc.checkout-team_1:prod"
	require.Empty(t, ValidateUserShape(u), "dots/colons/underscores/hyphens must be permitted")
}

func TestValidateUserShape_MechanismInvalid(t *testing.T) {
	u := validUserShape()
	u.Spec.Mechanism = "PLAIN"
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "invalid mechanism"), "got %v", errs)
}

func TestValidateUserShape_MechanismSCRAM256Valid(t *testing.T) {
	u := validUserShape()
	u.Spec.Mechanism = "SCRAM-SHA-256"
	require.Empty(t, ValidateUserShape(u))
}

func TestValidateUserShape_IterationsTooLow(t *testing.T) {
	u := validUserShape()
	u.Spec.Iterations = userI32(4095)
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "between 4096 and 16384"), "got %v", errs)
}

func TestValidateUserShape_IterationsTooHigh(t *testing.T) {
	u := validUserShape()
	u.Spec.Iterations = userI32(16385)
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "between 4096 and 16384"), "got %v", errs)
}

func TestValidateUserShape_IterationsBoundsValid(t *testing.T) {
	u := validUserShape()
	u.Spec.Iterations = userI32(4096)
	require.Empty(t, ValidateUserShape(u))
	u.Spec.Iterations = userI32(16384)
	require.Empty(t, ValidateUserShape(u))
}

func TestValidateUserShape_IterationsNilValid(t *testing.T) {
	u := validUserShape()
	u.Spec.Iterations = nil
	require.Empty(t, ValidateUserShape(u))
}

func TestValidateUserShape_ClusterRefRequired(t *testing.T) {
	u := validUserShape()
	u.Spec.ClusterRef.Name = ""
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "spec.clusterRef.name is required"), "got %v", errs)
}

func TestValidateUserShape_DeletionPolicyInvalid(t *testing.T) {
	u := validUserShape()
	u.Spec.DeletionPolicy = "Bogus"
	errs := ValidateUserShape(u)
	require.True(t, errorsContain(errs, "invalid deletionPolicy"), "got %v", errs)
}

func TestValidateUserShape_DeletionPolicyValidValues(t *testing.T) {
	u := validUserShape()
	u.Spec.DeletionPolicy = "Orphan"
	require.Empty(t, ValidateUserShape(u))
	u.Spec.DeletionPolicy = "Delete"
	require.Empty(t, ValidateUserShape(u))
}

// ---- Validate() KafkaUser cross-resource tests ----

func TestValidate_UserUnresolvedClusterRef(t *testing.T) {
	u := mkUser("u1", "missing", "svc")
	errs := Validate(Input{
		Users:    []*v1alpha1.KafkaUser{u},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.True(t, errorsContain(errs, `"missing"`), "expected unresolved cluster error, got %v", errs)
}

func TestValidate_UserIdentityCollisionSameUsername(t *testing.T) {
	u1 := mkUser("u1", "prod", "svc")
	u2 := mkUser("u2", "prod", "svc")
	errs := Validate(Input{
		Users:    []*v1alpha1.KafkaUser{u1, u2},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.True(t, errorsContain(errs, "u1"), "collision error must name first resource, got %v", errs)
	require.True(t, errorsContain(errs, "u2"), "collision error must name second resource, got %v", errs)
}

func TestValidate_UserIdentityCollisionDifferentMechanismStillCollides(t *testing.T) {
	// Identity is (cluster, username) only: a second CR for the same username
	// with a different mechanism still collides — it would fight over the
	// same principal's credential set.
	u1 := mkUser("u1", "prod", "svc")
	u1.Spec.Mechanism = "SCRAM-SHA-256"
	u2 := mkUser("u2", "prod", "svc")
	u2.Spec.Mechanism = "SCRAM-SHA-512"
	errs := Validate(Input{
		Users:    []*v1alpha1.KafkaUser{u1, u2},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.True(t, errorsContain(errs, "collides with"), "got %v", errs)
}

func TestValidate_UserDifferentUsernamesNoCollision(t *testing.T) {
	u1 := mkUser("u1", "prod", "svc-a")
	u2 := mkUser("u2", "prod", "svc-b")
	errs := Validate(Input{
		Users:    []*v1alpha1.KafkaUser{u1, u2},
		Clusters: map[string]*v1alpha1.KafkaCluster{"prod": cfgCluster()},
	})
	require.Empty(t, errs)
}

func TestValidate_UserDifferentClustersNoCollision(t *testing.T) {
	u1 := mkUser("u1", "prod", "svc")
	u2 := mkUser("u2", "staging", "svc")
	errs := Validate(Input{
		Users: []*v1alpha1.KafkaUser{u1, u2},
		Clusters: map[string]*v1alpha1.KafkaCluster{
			"prod":    cfgCluster(),
			"staging": {TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaCluster"}, ObjectMeta: metav1.ObjectMeta{Name: "staging"}},
		},
	})
	require.Empty(t, errs)
}

func TestValidate_UserNilClustersSkipsClusterCheck(t *testing.T) {
	u := mkUser("u1", "whatever", "svc")
	errs := Validate(Input{Users: []*v1alpha1.KafkaUser{u}})
	require.Empty(t, errs)
}
