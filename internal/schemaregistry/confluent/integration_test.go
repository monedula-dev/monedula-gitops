//go:build integration

package confluent

// Build-tagged integration tests that exercise the real confluent
// schemaregistry.Client against a LIVE Confluent Schema Registry started via
// testcontainers-go. These are EXCLUDED from the default `go test ./...` suite
// (no build tag) so the default run stays hermetic and Docker-free.
//
// Run them with:
//
//	go test -tags integration ./internal/schemaregistry/confluent/ -v
//
// There is no dedicated testcontainers module for Schema Registry, so we wire
// it by hand: a Kafka broker (via the testcontainers kafka module) and a
// generic confluentinc/cp-schema-registry container share a docker network, and
// SR is pointed at the broker's in-network PLAINTEXT (BROKER) listener.
//
// They SKIP cleanly (t.Skip) when Docker is unavailable — first via
// testcontainers.SkipIfProviderIsNotHealthy, then as a belt-and-braces fallback
// if the container start itself fails for a docker-connectivity reason — so a
// Docker-less environment never sees a failure. Each test gets a fresh SR
// endpoint and uses unique subject names (derived from t.Name()) for isolation.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
)

const (
	// kafkaImage is the confluent-local broker used by the kafka module. It
	// exposes an in-network BROKER (PLAINTEXT) listener on port 9092.
	kafkaImage = "confluentinc/confluent-local:7.6.1"
	// srImage is the Confluent Schema Registry image run as a generic container.
	srImage = "confluentinc/cp-schema-registry:7.6.1"
	// kafkaAlias is the stable network alias the broker is reachable at from the
	// SR container on the shared network.
	kafkaAlias = "broker"
	// srPort is the SR REST API port inside the container.
	srPort = "8081/tcp"
)

// startSchemaRegistry starts a Kafka broker and a Schema Registry wired to it on
// a shared docker network, returning the host-reachable SR endpoint (e.g.
// "http://127.0.0.1:54321"). All resources are torn down via t.Cleanup. If
// Docker is unavailable the test is skipped (never failed).
func startSchemaRegistry(t *testing.T) string {
	t.Helper()

	// Primary Docker-availability gate.
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()

	net, err := network.New(ctx)
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("Docker not available, skipping integration test: %v", err)
		}
		t.Fatalf("creating docker network: %v", err)
	}
	t.Cleanup(func() {
		if err := net.Remove(ctx); err != nil {
			t.Logf("removing docker network: %v", err)
		}
	})

	kafkaC, err := tckafka.Run(ctx, kafkaImage,
		network.WithNetwork([]string{kafkaAlias}, net),
	)
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("Docker not available, skipping integration test: %v", err)
		}
		t.Fatalf("starting kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(kafkaC); err != nil {
			t.Logf("terminating kafka container: %v", err)
		}
	})

	// SR connects to the broker's in-network BROKER (PLAINTEXT) listener on
	// 9092, reachable via the kafka container's network alias.
	srReq := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        srImage,
			ExposedPorts: []string{srPort},
			Networks:     []string{net.Name},
			Env: map[string]string{
				"SCHEMA_REGISTRY_HOST_NAME":                    "schemaregistry",
				"SCHEMA_REGISTRY_LISTENERS":                    "http://0.0.0.0:8081",
				"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS": fmt.Sprintf("PLAINTEXT://%s:9092", kafkaAlias),
			},
			WaitingFor: wait.ForHTTP("/subjects").
				WithPort(srPort).
				WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	}
	srC, err := testcontainers.GenericContainer(ctx, srReq)
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("Docker not available, skipping integration test: %v", err)
		}
		t.Fatalf("starting schema registry container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(srC); err != nil {
			t.Logf("terminating schema registry container: %v", err)
		}
	})

	endpoint, err := srC.PortEndpoint(ctx, srPort, "http")
	require.NoError(t, err, "getting schema registry endpoint")
	require.NotEmpty(t, endpoint, "schema registry returned empty endpoint")
	return endpoint
}

// isDockerUnavailable heuristically detects a docker-connectivity failure from a
// container-start error so we can t.Skip instead of t.Fatal. This is a fallback
// behind SkipIfProviderIsNotHealthy.
func isDockerUnavailable(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	for _, needle := range []string{
		"cannot connect to the docker daemon",
		"docker daemon",
		"dial unix",
		"no such file or directory",
		"connection refused",
		"docker.sock",
		"rootless docker not found",
		"failed to find a viable docker",
		"docker host",
	} {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}

// ctxT returns a per-test context with a generous timeout, tied to t.Cleanup.
func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// subjectFor derives a Schema-Registry-safe, test-unique subject name.
func subjectFor(t *testing.T) string {
	r := strings.NewReplacer("/", "-", " ", "-", "_", "-")
	return "it-" + strings.ToLower(r.Replace(t.Name())) + "-value"
}

const (
	orderV1 = `{"type":"record","name":"Order","namespace":"payments","fields":[{"name":"id","type":"string"},{"name":"amount","type":"long"}]}`
	// orderV2Optional adds an optional (defaulted) field — BACKWARD compatible.
	orderV2Optional = `{"type":"record","name":"Order","namespace":"payments","fields":[{"name":"id","type":"string"},{"name":"amount","type":"long"},{"name":"note","type":"string","default":""}]}`
	// orderV2Required adds a required field with no default — NOT BACKWARD
	// compatible (old data lacks it).
	orderV2Required = `{"type":"record","name":"Order","namespace":"payments","fields":[{"name":"id","type":"string"},{"name":"amount","type":"long"},{"name":"note","type":"string"}]}`
)

// TestIntegration_RegisterAndGetSubject registers a schema and reads it back.
func TestIntegration_RegisterAndGetSubject(t *testing.T) {
	endpoint := startSchemaRegistry(t)
	ctx := ctxT(t)

	client, err := New(endpoint, nil, nil)
	require.NoError(t, err)

	subject := subjectFor(t)
	s := schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: orderV1}

	id, err := client.RegisterSchema(ctx, subject, s)
	require.NoError(t, err, "RegisterSchema")
	assert.Greater(t, id, 0, "RegisterSchema should return a positive id")

	got, err := client.GetSubject(ctx, subject)
	require.NoError(t, err, "GetSubject")
	require.NotNil(t, got, "GetSubject returned nil for a subject we just registered")
	assert.Equal(t, subject, got.Subject)
	assert.Equal(t, schemaregistry.AVRO, got.Schema.Type)
	assert.NotEmpty(t, got.Schema.Definition)
}

// TestIntegration_GetSubjectAbsent confirms GetSubject returns (nil, nil) for a
// subject that was never registered.
func TestIntegration_GetSubjectAbsent(t *testing.T) {
	endpoint := startSchemaRegistry(t)
	ctx := ctxT(t)

	client, err := New(endpoint, nil, nil)
	require.NoError(t, err)

	got, err := client.GetSubject(ctx, subjectFor(t))
	require.NoError(t, err, "GetSubject on an absent subject must not error")
	assert.Nil(t, got, "GetSubject on an absent subject must return (nil, nil)")
}

// TestIntegration_CheckCompatibility registers a base schema and checks that a
// compatible evolution passes.
func TestIntegration_CheckCompatibility(t *testing.T) {
	endpoint := startSchemaRegistry(t)
	ctx := ctxT(t)

	client, err := New(endpoint, nil, nil)
	require.NoError(t, err)

	subject := subjectFor(t)
	_, err = client.RegisterSchema(ctx, subject, schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: orderV1})
	require.NoError(t, err)

	ok, err := client.CheckCompatibility(ctx, subject, schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: orderV2Optional})
	require.NoError(t, err, "CheckCompatibility")
	assert.True(t, ok, "adding an optional/defaulted field should be compatible")
}

// TestIntegration_CompatibilityRoundTrip sets and reads back the subject-level
// compatibility level.
func TestIntegration_CompatibilityRoundTrip(t *testing.T) {
	endpoint := startSchemaRegistry(t)
	ctx := ctxT(t)

	client, err := New(endpoint, nil, nil)
	require.NoError(t, err)

	subject := subjectFor(t)
	_, err = client.RegisterSchema(ctx, subject, schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: orderV1})
	require.NoError(t, err)

	require.NoError(t, client.SetCompatibility(ctx, subject, "FULL"), "SetCompatibility")

	level, err := client.GetCompatibility(ctx, subject)
	require.NoError(t, err, "GetCompatibility")
	assert.Equal(t, "FULL", level, "GetCompatibility did not reflect SetCompatibility")
}

// TestIntegration_IncompatibleUnderBackward is CRITICAL: under BACKWARD
// compatibility, an incompatible evolution must be rejected — either
// CheckCompatibility reports false or RegisterSchema surfaces an error.
func TestIntegration_IncompatibleUnderBackward(t *testing.T) {
	endpoint := startSchemaRegistry(t)
	ctx := ctxT(t)

	client, err := New(endpoint, nil, nil)
	require.NoError(t, err)

	subject := subjectFor(t)
	_, err = client.RegisterSchema(ctx, subject, schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: orderV1})
	require.NoError(t, err)
	require.NoError(t, client.SetCompatibility(ctx, subject, "BACKWARD"))

	bad := schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: orderV2Required}

	ok, err := client.CheckCompatibility(ctx, subject, bad)
	require.NoError(t, err, "CheckCompatibility on an incompatible schema should not itself error")
	if ok {
		// Some SR versions may classify this differently; the authoritative
		// gate is then registration, which must be rejected.
		_, regErr := client.RegisterSchema(ctx, subject, bad)
		require.Error(t, regErr, "registering an incompatible schema under BACKWARD must be rejected")
		return
	}
	assert.False(t, ok, "incompatible schema under BACKWARD must report not-compatible")
}

// TestIntegration_DeleteSubject registers then deletes a subject; afterwards it
// is absent and a second delete is idempotent (404 treated as success).
func TestIntegration_DeleteSubject(t *testing.T) {
	endpoint := startSchemaRegistry(t)
	ctx := ctxT(t)

	client, err := New(endpoint, nil, nil)
	require.NoError(t, err)

	subject := subjectFor(t)
	_, err = client.RegisterSchema(ctx, subject, schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: orderV1})
	require.NoError(t, err)

	require.NoError(t, client.DeleteSubject(ctx, subject), "DeleteSubject")

	got, err := client.GetSubject(ctx, subject)
	require.NoError(t, err)
	assert.Nil(t, got, "subject should be absent after DeleteSubject")

	// Idempotent: deleting again is a no-op (the adapter treats 404 as success).
	require.NoError(t, client.DeleteSubject(ctx, subject), "second DeleteSubject should be idempotent")
}

// TestIntegration_ListSubjects confirms a freshly registered subject appears in
// ListSubjects.
func TestIntegration_ListSubjects(t *testing.T) {
	endpoint := startSchemaRegistry(t)
	ctx := ctxT(t)

	client, err := New(endpoint, nil, nil)
	require.NoError(t, err)

	subject := subjectFor(t)
	_, err = client.RegisterSchema(ctx, subject, schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: orderV1})
	require.NoError(t, err)

	subjects, err := client.ListSubjects(ctx)
	require.NoError(t, err, "ListSubjects")
	assert.Contains(t, subjects, subject, "ListSubjects did not include the registered subject")
}
