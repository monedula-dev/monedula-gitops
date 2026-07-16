package mock_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
)

func TestCreateTopic(t *testing.T) {
	c := mock.New(nil, nil)
	ctx := context.Background()

	spec := kafka.TopicSpec{
		Name:              "payments.orders",
		Partitions:        6,
		ReplicationFactor: 3,
		Config:            map[string]string{"retention.ms": "604800000"},
	}
	if err := c.CreateTopic(ctx, spec); err != nil {
		t.Fatalf("CreateTopic: unexpected error: %v", err)
	}

	got, err := c.GetTopic(ctx, "payments.orders")
	if err != nil {
		t.Fatalf("GetTopic: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatalf("GetTopic: expected topic, got nil")
	}
	if got.Partitions != 6 || got.ReplicationFactor != 3 {
		t.Errorf("got partitions=%d rf=%d, want 6/3", got.Partitions, got.ReplicationFactor)
	}
	if got.Config["retention.ms"] != "604800000" {
		t.Errorf("got config %v, want retention.ms=604800000", got.Config)
	}
}

func TestCreateTopicAlreadyExists(t *testing.T) {
	c := mock.New([]kafka.TopicState{{Name: "t", Partitions: 1, ReplicationFactor: 1}}, nil)
	ctx := context.Background()

	err := c.CreateTopic(ctx, kafka.TopicSpec{Name: "t", Partitions: 1, ReplicationFactor: 1})
	if err == nil {
		t.Fatalf("CreateTopic on existing topic: expected error, got nil")
	}
}

func TestUpdateTopicConfig(t *testing.T) {
	c := mock.New([]kafka.TopicState{{
		Name:              "t",
		Partitions:        1,
		ReplicationFactor: 1,
		Config:            map[string]string{"a": "1", "b": "2"},
	}}, nil)
	ctx := context.Background()

	if err := c.UpdateTopicConfig(ctx, "t", map[string]string{"b": "20", "c": "3"}); err != nil {
		t.Fatalf("UpdateTopicConfig: unexpected error: %v", err)
	}

	got, _ := c.GetTopic(ctx, "t")
	want := map[string]string{"a": "1", "b": "20", "c": "3"}
	if !reflect.DeepEqual(got.Config, want) {
		t.Errorf("got config %v, want %v", got.Config, want)
	}
}

func TestUpdateTopicConfigNilMap(t *testing.T) {
	c := mock.New([]kafka.TopicState{{Name: "t", Partitions: 1, ReplicationFactor: 1}}, nil)
	ctx := context.Background()

	if err := c.UpdateTopicConfig(ctx, "t", map[string]string{"a": "1"}); err != nil {
		t.Fatalf("UpdateTopicConfig: unexpected error: %v", err)
	}
	got, _ := c.GetTopic(ctx, "t")
	if got.Config["a"] != "1" {
		t.Errorf("got config %v, want a=1", got.Config)
	}
}

func TestUpdateTopicConfigAbsent(t *testing.T) {
	c := mock.New(nil, nil)
	if err := c.UpdateTopicConfig(context.Background(), "nope", map[string]string{"a": "1"}); err == nil {
		t.Fatalf("UpdateTopicConfig on absent topic: expected error, got nil")
	}
}

func TestCreatePartitions(t *testing.T) {
	c := mock.New([]kafka.TopicState{{Name: "t", Partitions: 3, ReplicationFactor: 1}}, nil)
	ctx := context.Background()

	if err := c.CreatePartitions(ctx, "t", 6); err != nil {
		t.Fatalf("CreatePartitions: unexpected error: %v", err)
	}
	got, _ := c.GetTopic(ctx, "t")
	if got.Partitions != 6 {
		t.Errorf("got partitions=%d, want 6", got.Partitions)
	}
}

func TestCreatePartitionsAbsent(t *testing.T) {
	c := mock.New(nil, nil)
	if err := c.CreatePartitions(context.Background(), "nope", 3); err == nil {
		t.Fatalf("CreatePartitions on absent topic: expected error, got nil")
	}
}

func TestCreatePartitionsDecrease(t *testing.T) {
	c := mock.New([]kafka.TopicState{{Name: "t", Partitions: 6, ReplicationFactor: 1}}, nil)
	if err := c.CreatePartitions(context.Background(), "t", 3); err == nil {
		t.Fatalf("CreatePartitions decreasing count: expected error, got nil")
	}
	got, _ := c.GetTopic(context.Background(), "t")
	if got.Partitions != 6 {
		t.Errorf("partitions mutated on error: got %d, want 6", got.Partitions)
	}
}

func TestDeleteTopic(t *testing.T) {
	c := mock.New([]kafka.TopicState{{Name: "t", Partitions: 1, ReplicationFactor: 1}}, nil)
	ctx := context.Background()

	if err := c.DeleteTopic(ctx, "t"); err != nil {
		t.Fatalf("DeleteTopic: unexpected error: %v", err)
	}
	got, err := c.GetTopic(ctx, "t")
	if err != nil {
		t.Fatalf("GetTopic: unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("GetTopic after delete: got %v, want nil", got)
	}
}

func aclT(principal, op string) kafka.ACLState {
	return kafka.ACLState{
		Principal:    principal,
		Host:         "*",
		ResourceType: "TOPIC",
		ResourceName: "payments.orders",
		PatternType:  "LITERAL",
		Operation:    op,
		Permission:   "ALLOW",
	}
}

func TestCreateACLs(t *testing.T) {
	c := mock.New(nil, nil)
	ctx := context.Background()

	// Add out of sorted order to verify ListACLs still sorts deterministically.
	in := []kafka.ACLState{aclT("User:b", "WRITE"), aclT("User:a", "READ")}
	if err := c.CreateACLs(ctx, in); err != nil {
		t.Fatalf("CreateACLs: unexpected error: %v", err)
	}

	got, _ := c.ListACLs(ctx)
	if len(got) != 2 {
		t.Fatalf("got %d acls, want 2", len(got))
	}
	if got[0].Principal != "User:a" || got[1].Principal != "User:b" {
		t.Errorf("ListACLs not deterministically sorted: %+v", got)
	}
}

func TestDeleteACLs(t *testing.T) {
	a := aclT("User:a", "READ")
	b := aclT("User:b", "WRITE")
	d := aclT("User:d", "DELETE")
	c := mock.New(nil, []kafka.ACLState{a, b, d})
	ctx := context.Background()

	if err := c.DeleteACLs(ctx, []kafka.ACLState{b}); err != nil {
		t.Fatalf("DeleteACLs: unexpected error: %v", err)
	}

	got, _ := c.ListACLs(ctx)
	if len(got) != 2 {
		t.Fatalf("got %d acls, want 2", len(got))
	}
	for _, g := range got {
		if g == b {
			t.Errorf("DeleteACLs did not remove %+v", b)
		}
	}
	if got[0] != a || got[1] != d {
		t.Errorf("DeleteACLs removed wrong tuples: %+v", got)
	}
}

func TestCalls(t *testing.T) {
	c := mock.New([]kafka.TopicState{{Name: "t", Partitions: 1, ReplicationFactor: 1}}, nil)
	ctx := context.Background()

	_ = c.CreateTopic(ctx, kafka.TopicSpec{Name: "payments.orders", Partitions: 1, ReplicationFactor: 1})
	_ = c.UpdateTopicConfig(ctx, "t", map[string]string{"a": "1"})
	_ = c.CreatePartitions(ctx, "t", 2)
	_ = c.DeleteTopic(ctx, "t")
	_ = c.CreateACLs(ctx, []kafka.ACLState{aclT("User:a", "READ")})
	_ = c.DeleteACLs(ctx, []kafka.ACLState{aclT("User:a", "READ")})

	got := c.Calls()
	want := []string{
		"CreateTopic payments.orders",
		"UpdateTopicConfig t",
		"CreatePartitions t 2",
		"DeleteTopic t",
		"CreateACLs 1",
		"DeleteACLs 1",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Calls() =\n  %v\nwant\n  %v", got, want)
	}
}

func TestCallsNotRecordedOnReads(t *testing.T) {
	c := mock.New([]kafka.TopicState{{Name: "t", Partitions: 1, ReplicationFactor: 1}}, nil)
	ctx := context.Background()
	_, _ = c.GetTopic(ctx, "t")
	_, _ = c.ListTopics(ctx)
	_, _ = c.ListACLs(ctx)
	if len(c.Calls()) != 0 {
		t.Errorf("reads recorded calls: %v", c.Calls())
	}
}

func TestFailOn(t *testing.T) {
	boom := errors.New("boom")
	c := mock.New(nil, nil)
	c.FailOn("CreateTopic", "payments.orders", boom)
	ctx := context.Background()

	err := c.CreateTopic(ctx, kafka.TopicSpec{Name: "payments.orders", Partitions: 1, ReplicationFactor: 1})
	if !errors.Is(err, boom) {
		t.Fatalf("CreateTopic: got err %v, want %v", err, boom)
	}

	// Must NOT mutate state.
	got, _ := c.GetTopic(ctx, "payments.orders")
	if got != nil {
		t.Errorf("FailOn'd CreateTopic mutated state: %+v", got)
	}
	// The failed call is still recorded so the executor can assert it was attempted.
	if calls := c.Calls(); len(calls) != 1 || calls[0] != "CreateTopic payments.orders" {
		t.Errorf("expected failed call to be recorded, got %v", calls)
	}
}

func TestFailOnOnlyMatchesTarget(t *testing.T) {
	boom := errors.New("boom")
	c := mock.New(nil, nil)
	c.FailOn("CreateTopic", "payments.orders", boom)
	ctx := context.Background()

	// A different target must succeed.
	if err := c.CreateTopic(ctx, kafka.TopicSpec{Name: "other", Partitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateTopic other: unexpected error: %v", err)
	}
	got, _ := c.GetTopic(ctx, "other")
	if got == nil {
		t.Errorf("non-targeted CreateTopic did not mutate state")
	}
}

func TestInterfaceSatisfied(t *testing.T) {
	var _ kafka.AdminClient = (*mock.Client)(nil)
}
