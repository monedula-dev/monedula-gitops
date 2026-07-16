package e2e

import (
	"context"
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
)

func TestCheckLiveStateUsers(t *testing.T) {
	creds := []kafka.ScramCredential{
		{User: "svc-orders-app", Mechanism: "SCRAM-SHA-512", Iterations: 8192},
	}
	admin := kafkamock.NewWithScramCredentials(nil, nil, creds)
	p := NewKafkaProber(admin)
	ctx := context.Background()

	// Present credential, no iterations asserted -> PASS.
	ok := LiveState{Users: []UserExpect{{Username: "svc-orders-app", Mechanism: "SCRAM-SHA-512"}}}
	if rep := CheckLiveState(ctx, p, ok); rep.Failed() {
		t.Errorf("expected pass:\n%s", rep.String())
	}

	// Present credential, matching iterations -> PASS.
	okIter := LiveState{Users: []UserExpect{{Username: "svc-orders-app", Mechanism: "SCRAM-SHA-512", Iterations: 8192}}}
	if rep := CheckLiveState(ctx, p, okIter); rep.Failed() {
		t.Errorf("expected pass with matching iterations:\n%s", rep.String())
	}

	// Wrong iterations -> FAIL, naming the credential.
	badIter := LiveState{Users: []UserExpect{{Username: "svc-orders-app", Mechanism: "SCRAM-SHA-512", Iterations: 4096}}}
	rep := CheckLiveState(ctx, p, badIter)
	if !rep.Failed() || !strings.Contains(rep.String(), "svc-orders-app") {
		t.Errorf("wrong iterations should fail and name the user:\n%s", rep.String())
	}

	// Wrong mechanism -> FAIL.
	badMech := LiveState{Users: []UserExpect{{Username: "svc-orders-app", Mechanism: "SCRAM-SHA-256"}}}
	rep2 := CheckLiveState(ctx, p, badMech)
	if !rep2.Failed() || !strings.Contains(rep2.String(), "svc-orders-app") {
		t.Errorf("wrong mechanism should fail and name the user:\n%s", rep2.String())
	}

	// Missing username -> FAIL.
	missing := LiveState{Users: []UserExpect{{Username: "does-not-exist", Mechanism: "SCRAM-SHA-512"}}}
	rep3 := CheckLiveState(ctx, p, missing)
	if !rep3.Failed() || !strings.Contains(rep3.String(), "does-not-exist") {
		t.Errorf("missing user should fail and name it:\n%s", rep3.String())
	}
}

func TestCheckLiveStateUsersProbeError(t *testing.T) {
	rep := CheckLiveState(context.Background(),
		errProber{err: errReason("connection refused")},
		LiveState{Users: []UserExpect{{Username: "svc-orders-app", Mechanism: "SCRAM-SHA-512"}}})
	if !rep.Failed() {
		t.Fatalf("probe error should be a failed check, got:\n%s", rep.String())
	}
	if !strings.Contains(rep.String(), "connection refused") {
		t.Errorf("probe-error report should name the error:\n%s", rep.String())
	}
}

func TestCheckLiveStateAbsentUsers(t *testing.T) {
	creds := []kafka.ScramCredential{
		{User: "present-user", Mechanism: "SCRAM-SHA-512", Iterations: 8192},
	}
	admin := kafkamock.NewWithScramCredentials(nil, nil, creds)
	p := NewKafkaProber(admin)
	ctx := context.Background()

	// A present credential asserted absent -> FAIL, naming it.
	rep := CheckLiveState(ctx, p, LiveState{Absent: &AbsentState{
		Users: []UserAbsentEntry{{Username: "present-user", Mechanism: "SCRAM-SHA-512"}},
	}})
	if !rep.Failed() || !strings.Contains(rep.String(), "present-user") {
		t.Errorf("present credential asserted absent should fail and name it:\n%s", rep.String())
	}

	// A non-existent credential asserted absent -> PASS.
	rep = CheckLiveState(ctx, p, LiveState{Absent: &AbsentState{
		Users: []UserAbsentEntry{{Username: "ghost-user", Mechanism: "SCRAM-SHA-512"}},
	}})
	if rep.Failed() {
		t.Errorf("absent user should pass:\n%s", rep.String())
	}

	// Different mechanism for the same username still present -> absence of
	// the OTHER mechanism should still pass (matched by the pair, not just username).
	rep = CheckLiveState(ctx, p, LiveState{Absent: &AbsentState{
		Users: []UserAbsentEntry{{Username: "present-user", Mechanism: "SCRAM-SHA-256"}},
	}})
	if rep.Failed() {
		t.Errorf("absent (username, other-mechanism) pair should pass:\n%s", rep.String())
	}
}

func TestCheckLiveStateAbsentUsersProbeError(t *testing.T) {
	rep := CheckLiveState(context.Background(),
		errProber{err: errReason("connection refused")},
		LiveState{Absent: &AbsentState{Users: []UserAbsentEntry{{Username: "svc-orders-app", Mechanism: "SCRAM-SHA-512"}}}})
	if !rep.Failed() {
		t.Fatalf("absent probe error should be a failed check, got:\n%s", rep.String())
	}
	if !strings.Contains(rep.String(), "svc-orders-app") || !strings.Contains(rep.String(), "connection refused") {
		t.Errorf("absent probe-error report should name the user and the error:\n%s", rep.String())
	}
}

func TestCheckLiveStateTopics(t *testing.T) {
	admin := kafkamock.New(
		[]kafka.TopicState{{
			Name:       "payments.orders",
			Partitions: 3,
			Config:     map[string]string{"cleanup.policy": "compact"},
		}},
		nil,
	)
	prober := NewKafkaProber(admin)
	ctx := context.Background()

	// Present topic with matching config passes.
	ls := LiveState{Topics: []TopicExpect{
		{Name: "payments.orders", Config: map[string]string{"cleanup.policy": "compact"}},
	}}
	if rep := CheckLiveState(ctx, prober, ls); rep.Failed() {
		t.Errorf("expected pass, got:\n%s", rep.String())
	}

	// Missing topic fails and names it.
	ls2 := LiveState{Topics: []TopicExpect{{Name: "does.not.exist"}}}
	rep2 := CheckLiveState(ctx, prober, ls2)
	if !rep2.Failed() || !strings.Contains(rep2.String(), "does.not.exist") {
		t.Errorf("missing topic should fail and name it:\n%s", rep2.String())
	}

	// Wrong config value fails and the detail names the key.
	ls3 := LiveState{Topics: []TopicExpect{
		{Name: "payments.orders", Config: map[string]string{"cleanup.policy": "delete"}},
	}}
	rep3 := CheckLiveState(ctx, prober, ls3)
	if !rep3.Failed() || !strings.Contains(rep3.String(), "cleanup.policy") {
		t.Errorf("wrong config should fail and name the key:\n%s", rep3.String())
	}
}

// errProber is a LiveProber stub whose TopicConfig always errors, used to prove
// CheckLiveState records a probe error as a failed check (never panics). The
// kafka mock cannot inject read-method errors, so a stub is the cleanest way to
// exercise this path (the one Task 5's real AdminClient hits on network errors).
type errProber struct{ err error }

func (e errProber) TopicConfig(context.Context, string) (bool, map[string]string, error) {
	return false, nil, e.err
}

func (e errProber) ACLs(context.Context) ([]kafka.ACLState, error)     { return nil, e.err }
func (e errProber) Quotas(context.Context) ([]kafka.QuotaState, error) { return nil, e.err }
func (e errProber) Users(context.Context, ...string) ([]kafka.ScramCredential, error) {
	return nil, e.err
}

func TestCheckLiveStateProbeError(t *testing.T) {
	rep := CheckLiveState(context.Background(),
		errProber{err: errReason("connection refused")},
		LiveState{Topics: []TopicExpect{{Name: "payments.orders"}}})
	if !rep.Failed() {
		t.Fatalf("probe error should be a failed check, got:\n%s", rep.String())
	}
	if !strings.Contains(rep.String(), "payments.orders") || !strings.Contains(rep.String(), "connection refused") {
		t.Errorf("probe-error report should name the topic and the error:\n%s", rep.String())
	}
}

func TestCheckLiveStateAbsentProbeError(t *testing.T) {
	rep := CheckLiveState(context.Background(),
		errProber{err: errReason("connection refused")},
		LiveState{Absent: &AbsentState{Topics: []string{"delete.demo"}}})
	if !rep.Failed() {
		t.Fatalf("absent probe error should be a failed check, got:\n%s", rep.String())
	}
	if !strings.Contains(rep.String(), "delete.demo") || !strings.Contains(rep.String(), "connection refused") {
		t.Errorf("absent probe-error report should name the topic and the error:\n%s", rep.String())
	}
}

// errReason is a tiny error type for table-free error injection in tests.
type errReason string

func (e errReason) Error() string { return string(e) }

func TestCheckLiveStateACLs(t *testing.T) {
	acls := []kafka.ACLState{{
		Principal: "User:svc", Host: "*", ResourceType: "topic",
		ResourceName: "orders", PatternType: "literal",
		Operation: "Write", Permission: "Allow",
	}}
	admin := kafkamock.New(nil, acls)
	p := NewKafkaProber(admin)
	ctx := context.Background()

	ok := LiveState{ACLs: []ACLExpect{{Principal: "User:svc", Operation: "Write", ResourceType: "topic", ResourceName: "orders"}}}
	if rep := CheckLiveState(ctx, p, ok); rep.Failed() {
		t.Errorf("expected pass:\n%s", rep.String())
	}
	bad := LiveState{ACLs: []ACLExpect{{Principal: "User:svc", Operation: "Write", ResourceType: "topic", ResourceName: "orders", Permission: "Deny"}}}
	if rep := CheckLiveState(ctx, p, bad); !rep.Failed() || !strings.Contains(rep.String(), "User:svc") {
		t.Errorf("wrong permission should fail and name it:\n%s", rep.String())
	}
}

func TestCheckLiveStateAbsent(t *testing.T) {
	admin := kafkamock.New([]kafka.TopicState{{Name: "present.topic"}}, nil)
	p := NewKafkaProber(admin)
	ctx := context.Background()

	// A present topic asserted absent -> FAIL, naming it.
	rep := CheckLiveState(ctx, p, LiveState{Absent: &AbsentState{Topics: []string{"present.topic"}}})
	if !rep.Failed() || !strings.Contains(rep.String(), "present.topic") {
		t.Errorf("present topic asserted absent should fail and name it:\n%s", rep.String())
	}
	// A non-existent topic asserted absent -> PASS.
	rep = CheckLiveState(ctx, p, LiveState{Absent: &AbsentState{Topics: []string{"ghost.topic"}}})
	if rep.Failed() {
		t.Errorf("absent topic should pass:\n%s", rep.String())
	}
	// Mixed presence + absence both satisfied -> PASS.
	rep = CheckLiveState(ctx, p, LiveState{
		Topics: []TopicExpect{{Name: "present.topic"}},
		Absent: &AbsentState{Topics: []string{"ghost.topic"}},
	})
	if rep.Failed() {
		t.Errorf("present+absent both satisfied should pass:\n%s", rep.String())
	}
}

func TestCheckLiveStateQuotas(t *testing.T) {
	u := "svc"
	ipName := "10.0.0.1"
	cid := "svc-app"
	quotas := []kafka.QuotaState{
		{Entity: []kafka.QuotaEntityComponent{{Type: "user", Name: &u}}, Limits: map[string]float64{"producer_byte_rate": 1048576}},
		{Entity: []kafka.QuotaEntityComponent{{Type: "ip", Name: &ipName}}, Limits: map[string]float64{"connection_creation_rate": 100}},
		{Entity: []kafka.QuotaEntityComponent{{Type: "client-id", Name: &cid}}, Limits: map[string]float64{"consumer_byte_rate": 2048}},
	}
	admin := kafkamock.NewWithQuotas(nil, nil, quotas)
	p := NewKafkaProber(admin)
	ctx := context.Background()

	ok := LiveState{Quotas: []QuotaExpect{
		{Entity: QuotaEntityExpect{User: "svc"}, Limits: map[string]float64{"producer_byte_rate": 1048576}},
		{Entity: QuotaEntityExpect{IP: "10.0.0.1"}, Limits: map[string]float64{"connection_creation_rate": 100}},
		// clientId expect must map to the live "client-id" component type.
		{Entity: QuotaEntityExpect{ClientID: "svc-app"}, Limits: map[string]float64{"consumer_byte_rate": 2048}},
	}}
	if rep := CheckLiveState(ctx, p, ok); rep.Failed() {
		t.Errorf("expected pass:\n%s", rep.String())
	}
	bad := LiveState{Quotas: []QuotaExpect{{Entity: QuotaEntityExpect{User: "svc"}, Limits: map[string]float64{"producer_byte_rate": 999}}}}
	if rep := CheckLiveState(ctx, p, bad); !rep.Failed() {
		t.Errorf("wrong quota limit should fail:\n%s", rep.String())
	}
	miss := LiveState{Quotas: []QuotaExpect{{Entity: QuotaEntityExpect{User: "absent"}, Limits: map[string]float64{"producer_byte_rate": 1}}}}
	if rep := CheckLiveState(ctx, p, miss); !rep.Failed() {
		t.Errorf("missing quota entity should fail:\n%s", rep.String())
	}
}
