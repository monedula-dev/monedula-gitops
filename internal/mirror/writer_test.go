package mirror

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kmock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
	rmock "github.com/monedula-dev/monedula-gitops/internal/kafkarest/mock"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
)

const syncInclude = "topic.config.sync.include"

// writerFixture is a destination cluster "A" with an ACTIVE link "l" (no
// prefix, syncs cleanup.policy) and no topics.
func writerFixture(t *testing.T) (*kmock.Client, *rmock.Client, Writer) {
	t.Helper()
	k := kmock.New(nil, nil)
	k.SetClusterID("A")
	r := rmock.New("A")
	r.AddLink(kafkarest.Link{Name: "l", State: "ACTIVE"}, map[string]string{syncInclude: "cleanup.policy"})
	return k, r, Writer{Kafka: k, REST: r}
}

func createOp() operations.Operation {
	op := operations.New(operations.CreateMirrorTopic)
	op.Target = "orders"
	op.MirrorLink = "l"
	op.MirrorSource = "orders"
	op.ReplicationFactor = 3
	op.Config = map[string]string{"retention.ms": "1000"}
	return op
}

func alterOp() operations.Operation {
	op := operations.New(operations.UpdateMirrorTopicConfig)
	op.Target = "orders"
	op.MirrorLink = "l"
	op.MirrorSource = "orders"
	op.Field, op.To = "retention.ms", "2000"
	return op
}

func requirePreWrite(t *testing.T, err error, r *rmock.Client) {
	t.Helper()
	if err == nil || !strings.HasPrefix(err.Error(), "pre-write check: ") {
		t.Fatalf("want pre-write check error, got %v", err)
	}
	if len(r.Created()) != 0 || len(r.Altered()) != 0 {
		t.Fatalf("no write expected: created %v altered %v", r.Created(), r.Altered())
	}
}

func TestWriterCreateSucceeds(t *testing.T) {
	_, r, w := writerFixture(t)
	if err := w.CreateMirror(context.Background(), createOp()); err != nil {
		t.Fatal(err)
	}
	got := r.Created()
	if len(got) != 1 {
		t.Fatalf("created %v", got)
	}
	c := got[0]
	if c.LinkName != "l" || c.SourceTopicName != "orders" || c.MirrorTopicName != "orders" ||
		c.ReplicationFactor != 3 || c.Configs["retention.ms"] != "1000" {
		t.Fatalf("request %+v", c)
	}
}

func TestWriterCreateRechecksTopicAppeared(t *testing.T) {
	k, r, w := writerFixture(t)
	// A normal topic appears between planning and write.
	if err := k.CreateTopic(context.Background(), kafka.TopicSpec{Name: "orders", Partitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	requirePreWrite(t, w.CreateMirror(context.Background(), createOp()), r)
}

func TestWriterCreateRechecksMirrorAppeared(t *testing.T) {
	_, r, w := writerFixture(t)
	// Only the REST mirror entry appears (no Kafka topic): the fresh snapshot
	// classifies the target Unknown, which must block the write on its own,
	// independently of the topic-appeared path.
	r.AddMirror(kafkarest.Mirror{LinkName: "l", MirrorTopicName: "orders", SourceTopicName: "orders", Status: "ACTIVE"})
	err := w.CreateMirror(context.Background(), createOp())
	requirePreWrite(t, err, r)
	if !strings.Contains(err.Error(), "topic orders is now Unknown, not Absent") {
		t.Fatalf("want the Unknown (mirror entry without topic) branch, got %v", err)
	}
}

func TestWriterCreateClusterIDMismatch(t *testing.T) {
	k, r, w := writerFixture(t)
	k.SetClusterID("B")
	err := w.CreateMirror(context.Background(), createOp())
	requirePreWrite(t, err, r)
	if !errors.Is(err, ErrClusterIdentityMismatch) {
		t.Fatalf("want ClusterIdentityMismatch, got %v", err)
	}
}

func TestWriterCreateRechecksLinkAndConfigs(t *testing.T) {
	cases := []struct {
		name  string
		setup func(r *rmock.Client)
		op    func() operations.Operation
	}{
		{"link missing", func(*rmock.Client) {}, func() operations.Operation {
			op := createOp()
			op.MirrorLink = "other"
			return op
		}},
		{"link paused", func(r *rmock.Client) {
			r.AddLink(kafkarest.Link{Name: "p", State: "PAUSED"}, map[string]string{syncInclude: "x"})
		}, func() operations.Operation {
			op := createOp()
			op.MirrorLink = "p"
			return op
		}},
		{"prefix appeared", func(r *rmock.Client) {
			r.AddLink(kafkarest.Link{Name: "pf", State: "ACTIVE"}, map[string]string{syncInclude: "x", "cluster.link.prefix": "src."})
		}, func() operations.Operation {
			op := createOp()
			op.MirrorLink = "pf"
			return op
		}},
		{"key link-managed", func(*rmock.Client) {}, func() operations.Operation {
			op := createOp()
			op.Config = map[string]string{"cleanup.policy": "compact"}
			return op
		}},
		{"sync include unknown", func(r *rmock.Client) {
			r.AddLink(kafkarest.Link{Name: "u", State: "ACTIVE"}, map[string]string{})
		}, func() operations.Operation {
			op := createOp()
			op.MirrorLink = "u"
			return op
		}},
		{"link configs unreadable", func(r *rmock.Client) {
			r.Fail("LinkConfigs", errors.New("boom"))
		}, createOp},
		{"mirrors unreadable", func(r *rmock.Client) {
			r.Fail("ListMirrors", errors.New("boom"))
		}, createOp},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, r, w := writerFixture(t)
			c.setup(r)
			requirePreWrite(t, w.CreateMirror(context.Background(), c.op()), r)
		})
	}
}

func TestWriterCreatePostFailureSaysOutcomeUnknown(t *testing.T) {
	_, r, w := writerFixture(t)
	boom := errors.New("timeout")
	r.Fail("CreateMirror", boom)
	err := w.CreateMirror(context.Background(), createOp())
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("want wrapped POST error, got %v", err)
	}
	want := "create mirror orders: outcome unknown, re-run diff before retrying: timeout"
	if err.Error() != want {
		t.Fatalf("got %q want %q", err.Error(), want)
	}
}

func liveMirror(t *testing.T, k *kmock.Client, r *rmock.Client, link, source string) {
	t.Helper()
	if err := k.CreateTopic(context.Background(), kafka.TopicSpec{Name: "orders", Partitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	r.AddMirror(kafkarest.Mirror{LinkName: link, MirrorTopicName: "orders", SourceTopicName: source, Status: "ACTIVE"})
}

func TestWriterAlterSucceeds(t *testing.T) {
	k, r, w := writerFixture(t)
	liveMirror(t, k, r, "l", "orders")
	if err := w.AlterMirrorConfig(context.Background(), alterOp()); err != nil {
		t.Fatal(err)
	}
	if got := r.Altered()["orders"]; len(got) != 1 || got["retention.ms"] != "2000" {
		t.Fatalf("altered %v", r.Altered())
	}
}

func TestWriterAlterRequiresSameLink(t *testing.T) {
	k, r, w := writerFixture(t)
	r.AddLink(kafkarest.Link{Name: "other", State: "ACTIVE"}, map[string]string{syncInclude: "x"})
	liveMirror(t, k, r, "other", "orders")
	requirePreWrite(t, w.AlterMirrorConfig(context.Background(), alterOp()), r)
}

func TestWriterAlterRechecks(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, k *kmock.Client, r *rmock.Client)
		op    func() operations.Operation
	}{
		{"different source", func(t *testing.T, k *kmock.Client, r *rmock.Client) {
			liveMirror(t, k, r, "l", "elsewhere")
		}, alterOp},
		{"normal topic", func(t *testing.T, k *kmock.Client, _ *rmock.Client) {
			if err := k.CreateTopic(context.Background(), kafka.TopicSpec{Name: "orders", Partitions: 1, ReplicationFactor: 1}); err != nil {
				t.Fatal(err)
			}
		}, alterOp},
		{"absent", func(*testing.T, *kmock.Client, *rmock.Client) {}, alterOp},
		{"stopped", func(t *testing.T, k *kmock.Client, r *rmock.Client) {
			if err := k.CreateTopic(context.Background(), kafka.TopicSpec{Name: "orders", Partitions: 1, ReplicationFactor: 1}); err != nil {
				t.Fatal(err)
			}
			r.AddMirror(kafkarest.Mirror{LinkName: "l", MirrorTopicName: "orders", SourceTopicName: "orders", Status: "STOPPED"})
		}, alterOp},
		{"key link-managed", func(t *testing.T, k *kmock.Client, r *rmock.Client) {
			liveMirror(t, k, r, "l", "orders")
		}, func() operations.Operation {
			op := alterOp()
			op.Field = "cleanup.policy"
			return op
		}},
		{"sync include unknown", func(t *testing.T, k *kmock.Client, r *rmock.Client) {
			r.AddLink(kafkarest.Link{Name: "u", State: "ACTIVE"}, map[string]string{syncInclude: ""})
			liveMirror(t, k, r, "u", "orders")
		}, func() operations.Operation {
			op := alterOp()
			op.MirrorLink = "u"
			return op
		}},
		{"link configs unreadable", func(t *testing.T, k *kmock.Client, r *rmock.Client) {
			liveMirror(t, k, r, "l", "orders")
			r.Fail("LinkConfigs", errors.New("boom"))
		}, alterOp},
		{"cluster mismatch", func(t *testing.T, k *kmock.Client, r *rmock.Client) {
			liveMirror(t, k, r, "l", "orders")
			k.SetClusterID("B")
		}, alterOp},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			k, r, w := writerFixture(t)
			c.setup(t, k, r)
			requirePreWrite(t, w.AlterMirrorConfig(context.Background(), c.op()), r)
		})
	}
}

func TestWriterAlterFailureWrapped(t *testing.T) {
	k, r, w := writerFixture(t)
	liveMirror(t, k, r, "l", "orders")
	boom := errors.New("timeout")
	r.Fail("AlterTopicConfigs", boom)
	err := w.AlterMirrorConfig(context.Background(), alterOp())
	if !errors.Is(err, boom) || strings.HasPrefix(err.Error(), "pre-write check") {
		t.Fatalf("got %v", err)
	}
}

func TestWriterCreateDefiniteRejection(t *testing.T) {
	_, r, w := writerFixture(t)
	r.Fail("CreateMirror", &kafkarest.StatusError{Code: 404, ErrorCode: 40403, Message: "no such topic"})
	err := w.CreateMirror(context.Background(), createOp())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "rejected by Kafka REST (404)") || strings.Contains(err.Error(), "outcome unknown") {
		t.Fatalf("got %q", err.Error())
	}
	var se *kafkarest.StatusError
	if !errors.As(err, &se) {
		t.Fatalf("StatusError not wrapped: %v", err)
	}
}

func TestWriterCreateTransportErrorIsUnknown(t *testing.T) {
	_, r, w := writerFixture(t)
	r.Fail("CreateMirror", errors.New("connection reset"))
	err := w.CreateMirror(context.Background(), createOp())
	if err == nil || !strings.Contains(err.Error(), "outcome unknown") {
		t.Fatalf("got %v", err)
	}
}

func TestWriterCreateServerErrorIsUnknown(t *testing.T) {
	for _, code := range []int{503, 408} {
		_, r, w := writerFixture(t)
		r.Fail("CreateMirror", &kafkarest.StatusError{Code: code, Message: "uncertain"})
		err := w.CreateMirror(context.Background(), createOp())
		if err == nil || !strings.Contains(err.Error(), "outcome unknown") || strings.Contains(err.Error(), "rejected by Kafka REST") {
			t.Fatalf("code %d: got %v", code, err)
		}
	}
}
