package mirror

import (
	"context"
	"errors"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kmock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
	rmock "github.com/monedula-dev/monedula-gitops/internal/kafkarest/mock"
)

// countingAdmin records the read calls VerifyTopic makes and can fail GetTopic.
type countingAdmin struct {
	kafka.AdminClient
	getTopic, listTopics int
	getErr               error
}

func (c *countingAdmin) GetTopic(ctx context.Context, name string) (*kafka.TopicState, error) {
	c.getTopic++
	if c.getErr != nil {
		return nil, c.getErr
	}
	return c.AdminClient.GetTopic(ctx, name)
}

func (c *countingAdmin) ListTopics(ctx context.Context) ([]kafka.TopicState, error) {
	c.listTopics++
	return c.AdminClient.ListTopics(ctx)
}

func TestVerifyTopicTable(t *testing.T) {
	type tc struct {
		name    string
		topics  []kafka.TopicState
		adminID string
		restID  string
		mirrors []kafkarest.Mirror
		fail    string // kafkarest mock method to fail
		getErr  error
		want    Classification
		wantErr error // nil: no error; errAny: any error
	}
	errAny := errors.New("any")
	orders := []kafka.TopicState{{Name: "orders"}}
	cases := []tc{
		{name: "absent", adminID: "A", restID: "A", want: Absent},
		{name: "normal", topics: orders, adminID: "A", restID: "A",
			mirrors: []kafkarest.Mirror{{LinkName: "l", MirrorTopicName: "other", Status: "ACTIVE"}}, want: Normal},
		{name: "mirror on other link", topics: orders, adminID: "A", restID: "A",
			mirrors: []kafkarest.Mirror{
				{LinkName: "a", MirrorTopicName: "x", Status: "ACTIVE"},
				{LinkName: "b", MirrorTopicName: "orders", Status: "ACTIVE"},
			}, want: Mirror},
		{name: "pending stopped", topics: orders, adminID: "A", restID: "A",
			mirrors: []kafkarest.Mirror{{LinkName: "l", MirrorTopicName: "orders", Status: "PENDING_STOPPED"}}, want: StoppingMirror},
		{name: "stopped with topic", topics: orders, adminID: "A", restID: "A",
			mirrors: []kafkarest.Mirror{{LinkName: "l", MirrorTopicName: "orders", Status: "STOPPED"}}, want: StoppedFormerMirror},
		{name: "stopped without topic", adminID: "A", restID: "A",
			mirrors: []kafkarest.Mirror{{LinkName: "l", MirrorTopicName: "orders", Status: "STOPPED"}}, want: Unknown},
		{name: "id mismatch", topics: orders, adminID: "A", restID: "B", want: Unknown, wantErr: ErrClusterIdentityMismatch},
		{name: "admin id empty", topics: orders, adminID: "", restID: "A", want: Unknown, wantErr: ErrClusterIdentityMismatch},
		{name: "rest id error", topics: orders, adminID: "A", restID: "A", fail: "ClusterID", want: Unknown, wantErr: ErrClusterIdentityMismatch},
		{name: "list mirrors error", topics: orders, adminID: "A", restID: "A", fail: "ListMirrors", want: Unknown, wantErr: errAny},
		{name: "get topic error", adminID: "A", restID: "A", getErr: errors.New("boom"), want: Unknown, wantErr: errAny},
		{name: "duplicate mirror name", topics: orders, adminID: "A", restID: "A",
			mirrors: []kafkarest.Mirror{
				{LinkName: "a", MirrorTopicName: "orders", Status: "ACTIVE"},
				{LinkName: "b", MirrorTopicName: "orders", Status: "STOPPED"},
			}, want: Unknown, wantErr: errAny},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			km := kmock.New(c.topics, nil)
			km.SetClusterID(c.adminID)
			k := &countingAdmin{AdminClient: km, getErr: c.getErr}
			r := rmock.New(c.restID)
			for _, m := range c.mirrors {
				r.AddMirror(m)
			}
			if c.fail != "" {
				r.Fail(c.fail, errors.New("rest "+c.fail+" failed"))
			}
			got, err := VerifyTopic(context.Background(), k, r, "orders")
			if got != c.want {
				t.Errorf("class = %s, want %s (err %v)", got, c.want, err)
			}
			switch {
			case c.wantErr == nil && err != nil:
				t.Errorf("unexpected error: %v", err)
			case c.wantErr == errAny && err == nil:
				t.Error("want an error, got nil")
			case c.wantErr != nil && c.wantErr != errAny && !errors.Is(err, c.wantErr):
				t.Errorf("err = %v, want %v", err, c.wantErr)
			}
		})
	}
}

func TestVerifyTopicUsesGetTopicNotListTopics(t *testing.T) {
	km := kmock.New([]kafka.TopicState{{Name: "orders"}, {Name: "other"}}, nil)
	km.SetClusterID("A")
	km.FailOn("ListTopics", "", errors.New("ListTopics must not be called"))
	k := &countingAdmin{AdminClient: km}
	r := rmock.New("A")
	r.Fail("ListLinks", errors.New("ListLinks must not be called"))
	r.Fail("LinkConfigs", errors.New("LinkConfigs must not be called"))

	got, err := VerifyTopic(context.Background(), k, r, "orders")
	if err != nil || got != Normal {
		t.Fatalf("got %s, %v; want Normal, nil", got, err)
	}
	if k.listTopics != 0 {
		t.Fatalf("ListTopics called %d times, want 0", k.listTopics)
	}
	if k.getTopic != 1 {
		t.Fatalf("GetTopic called %d times, want 1", k.getTopic)
	}
}
