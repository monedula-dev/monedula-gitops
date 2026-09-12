//go:build integration || e2e

package matrix

// Container start paths for the matrix cells. Behind a build tag so the default
// `go build ./...` never pulls testcontainers into the production dependency
// graph.
//
// Two families, two paths:
//
//   - confluent: the testcontainers kafka module handles it.
//   - apache: the module CANNOT. Its starter script hard-codes the Confluent
//     image layout (/etc/confluent/docker/{bash-config,configure,launch}). The
//     apache/kafka image mirrors that layout under /etc/kafka/docker/ instead,
//     so this file replicates the module's technique with the apache paths.
//     Unlike the Confluent path, no explicit `kafka-storage format` is needed:
//     the apache image's own `launch` formats storage and `configureDefaults`
//     supplies a fallback CLUSTER_ID.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// apacheStarterScript is the apache/kafka image's own /etc/kafka/docker/run with
// one line inserted: the advertised listeners, which are only knowable after the
// container starts and Docker assigns the host port.
//
// First %s is the host-reachable PLAINTEXT endpoint, second is the container
// hostname used for the in-network BROKER listener.
const apacheStarterScript = `#!/usr/bin/env bash
. /etc/kafka/docker/bash-config
export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%s,BROKER://%s:9092
. /etc/kafka/docker/configureDefaults
. /etc/kafka/docker/configure
. /etc/kafka/docker/launch
`

const (
	apacheStarterPath = "/usr/sbin/monedula_start.sh"
	publicPort        = "9093/tcp"
)

// authorizerEnv is the ACL configuration every matrix broker needs, whatever
// its family. Without an authorizer a broker answers CreateAcls/DescribeAcls
// with SECURITY_DISABLED — documented broker behaviour, not a client fault —
// so the ACL tests cannot run at all. Neither the testcontainers kafka module
// nor startApache set it, so both start paths merge this map in.
//
// StandardAuthorizer is the KRaft-native authorizer (AclAuthorizer is the
// ZooKeeper one and does not exist in Kafka 4.x). It ships in the broker's
// `metadata` module on both apache/kafka (3.9+) and confluentinc/cp-kafka
// (7.6+), so one class name covers the whole matrix.
//
// Super users: `User:ANONYMOUS`, and nothing else.
//
//   - Why ANONYMOUS: these containers expose a plaintext, unauthenticated
//     listener, so every admin connection authenticates as the ANONYMOUS
//     principal. StandardAuthorizer denies by default, so without this the
//     broker would refuse the ANONYMOUS client's cluster operations and all
//     twelve integration tests would fail, not just the ACL ones. The KRaft
//     controller is reached over a plaintext CONTROLLER listener too, so the
//     broker's own intra-cluster principal is ANONYMOUS as well and is covered
//     by the same entry.
//
//   - Why NOT allow.everyone.if.no.acl.found=true, which the shared-sasl
//     quickstart compose sets: that flips the authorizer to allow-by-default,
//     which would make "this ACL grants access" unfalsifiable and drain the
//     meaning out of the very assertions these tests exist to make. Super-user
//     status is a per-request bypass for one principal; it changes nothing
//     about which ACLs exist or what DescribeAcls reports, so the ACL
//     round-trip assertions stay honest.
var authorizerEnv = map[string]string{
	"KAFKA_AUTHORIZER_CLASS_NAME": "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
	"KAFKA_SUPER_USERS":           "User:ANONYMOUS",
}

// Broker is a started broker container plus the addresses to reach it.
type Broker struct {
	testcontainers.Container
	// Bootstrap is the host-reachable bootstrap server list.
	Bootstrap []string
	// Internal is the in-network "alias:9092" address, empty unless the caller
	// passed WithNetwork.
	Internal string
}

type startConfig struct {
	networkName string
	alias       string
}

// Option customises StartBroker.
type Option func(*startConfig)

// WithNetwork attaches the broker to an existing docker network under the given
// alias, and populates Broker.Internal so sibling containers can reach it.
func WithNetwork(networkName, alias string) Option {
	return func(sc *startConfig) {
		sc.networkName = networkName
		sc.alias = alias
	}
}

// StartBroker starts the broker for a cell. The caller terminates it via
// testcontainers.TerminateContainer.
func StartBroker(ctx context.Context, c Cell, opts ...Option) (*Broker, error) {
	var sc startConfig
	for _, o := range opts {
		o(&sc)
	}

	switch c.Family {
	case FamilyConfluent:
		return startConfluent(ctx, c, sc)
	case FamilyApache:
		return startApache(ctx, c, sc)
	default:
		return nil, fmt.Errorf("matrix: cell %q has unknown family %q", c.ID, c.Family)
	}
}

func startConfluent(ctx context.Context, c Cell, sc startConfig) (*Broker, error) {
	// WithEnv merges into the module's own env map, and module options are
	// applied before ours, so this adds the authorizer without disturbing the
	// module's listener setup.
	//
	// WithClusterID is required here: the module sets no default CLUSTER_ID
	// (it only sets one when the caller passes this customizer), and
	// cp-kafka's own configure script calls `dub ensure CLUSTER_ID`, which
	// fails the container at startup without it. Use the same value as
	// startApache so both paths stay consistent.
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithEnv(authorizerEnv),
		tckafka.WithClusterID("4L6g3nShT-eMCtK--X86sw"),
	}
	if sc.networkName != "" {
		opts = append(opts, network.WithNetworkName([]string{sc.alias}, sc.networkName))
	}

	ctr, err := tckafka.Run(ctx, c.Image, opts...)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("matrix: starting confluent broker %s: %w", c.Image, err),
			testcontainers.TerminateContainer(ctr),
		)
	}
	brokers, err := ctr.Brokers(ctx)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("matrix: reading brokers from %s: %w", c.Image, err),
			testcontainers.TerminateContainer(ctr),
		)
	}
	return &Broker{
		Container: ctr,
		Bootstrap: brokers,
		Internal:  internalAddr(sc.alias),
	}, nil
}

func startApache(ctx context.Context, c Cell, sc startConfig) (*Broker, error) {
	env := map[string]string{
		"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094",
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
		"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
		"KAFKA_NODE_ID":                                  "1",
		"KAFKA_PROCESS_ROLES":                            "broker,controller",
		"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
		"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9094",
		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
		"KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS":             "1",
		"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
		"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
		"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
		"CLUSTER_ID":                                     "4L6g3nShT-eMCtK--X86sw",
	}
	for k, v := range authorizerEnv {
		env[k] = v
	}

	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithExposedPorts(publicPort),
		testcontainers.WithEnv(env),
		// Hold the container at a shell until the starter script lands, exactly
		// as the testcontainers kafka module does: the advertised listener host
		// port is unknown until Docker has assigned it.
		testcontainers.WithEntrypoint("sh"),
		testcontainers.WithCmd("-c", "while [ ! -f "+apacheStarterPath+" ]; do sleep 0.1; done; bash "+apacheStarterPath),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{
				func(ctx context.Context, ctr testcontainers.Container) error {
					if err := copyApacheStarter(ctx, ctr, sc.alias); err != nil {
						return fmt.Errorf("copy starter script: %w", err)
					}
					return wait.ForLog("Kafka Server started").
						WaitUntilReady(ctx, ctr)
				},
			},
		}),
	}
	if sc.networkName != "" {
		opts = append(opts, network.WithNetworkName([]string{sc.alias}, sc.networkName))
	}

	ctr, err := testcontainers.Run(ctx, c.Image, opts...)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("matrix: starting apache broker %s: %w", c.Image, err),
			testcontainers.TerminateContainer(ctr),
		)
	}

	endpoint, err := ctr.PortEndpoint(ctx, publicPort, "")
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("matrix: reading endpoint from %s: %w", c.Image, err),
			testcontainers.TerminateContainer(ctr),
		)
	}
	return &Broker{
		Container: ctr,
		Bootstrap: []string{endpoint},
		Internal:  internalAddr(sc.alias),
	}, nil
}

func copyApacheStarter(ctx context.Context, ctr testcontainers.Container, alias string) error {
	if err := wait.ForMappedPort(publicPort).WaitUntilReady(ctx, ctr); err != nil {
		return fmt.Errorf("wait for mapped port: %w", err)
	}
	endpoint, err := ctr.PortEndpoint(ctx, publicPort, "")
	if err != nil {
		return fmt.Errorf("port endpoint: %w", err)
	}
	inspect, err := ctr.Inspect(ctx)
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}
	hostname := inspect.Config.Hostname
	if alias != "" {
		hostname = alias
	}
	script := fmt.Sprintf(apacheStarterScript, endpoint, hostname)
	return ctr.CopyToContainer(ctx, []byte(script), apacheStarterPath, 0o755)
}

func internalAddr(alias string) string {
	if alias == "" {
		return ""
	}
	return alias + ":9092"
}

// SkipUnlessDocker skips the test when Docker is unavailable, matching the
// long-standing behaviour of this repository's integration tests: a Docker-less
// environment never sees a failure.
func SkipUnlessDocker(t *testing.T) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
}

// SkipWithoutCapability skips the test with a stated reason when the cell does
// not offer the capability. Never skip silently: a reader must be able to tell
// a pass from a no-op.
func SkipWithoutCapability(t *testing.T, c Cell, capability string) {
	t.Helper()
	if !c.Supports(capability) {
		t.Skipf("cell %s (%s) has no %s capability", c.ID, c.Image, capability)
	}
}

// IsDockerUnavailable reports whether an error from a container start is a
// Docker-daemon-connectivity problem rather than a real failure. Every call
// site gates on the provider's health immediately before starting a container
// — via SkipUnlessDocker, or via testcontainers.SkipIfProviderIsNotHealthy,
// which SkipUnlessDocker simply wraps — so by the time this runs, Docker has
// already been proven healthy. This is
// only a fallback for the (rare) case where the daemon dropped away between
// that check and the start call. Because of that ordering, the needle set
// deliberately excludes generic strings such as "connection refused" and "no
// such file or directory": those also match a testcontainers port-wait
// timeout on a broker that failed to come up (e.g. "dial tcp
// 127.0.0.1:32768: connect: connection refused"), which would otherwise
// report a genuine broker failure as a Docker-unavailable skip instead of a
// test failure. Only strings unambiguous to daemon connectivity are matched.
func IsDockerUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{
		"cannot connect to the docker daemon",
		"docker daemon",
		"dial unix",
		"docker.sock",
		"rootless docker not found",
		"failed to find a viable docker",
		"docker host",
		"error during connect",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
