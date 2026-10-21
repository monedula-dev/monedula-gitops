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
)

func TestClassify(t *testing.T) {
	topic := map[string]kafka.TopicState{"x": {Name: "x"}}
	mir := func(st string) map[string]kafkarest.Mirror {
		return map[string]kafkarest.Mirror{"x": {MirrorTopicName: "x", Status: st}}
	}
	cases := []struct {
		name string
		s    Snapshot
		want Classification
	}{
		{"absent", Snapshot{}, Absent},
		{"normal", Snapshot{Topics: topic}, Normal},
		{"active", Snapshot{Topics: topic, Mirrors: mir("ACTIVE")}, Mirror},
		{"failed", Snapshot{Topics: topic, Mirrors: mir("FAILED")}, Mirror},
		{"pending mirror", Snapshot{Topics: topic, Mirrors: mir("PENDING_MIRROR")}, Mirror},
		{"stopping", Snapshot{Topics: topic, Mirrors: mir("PENDING_STOPPED")}, StoppingMirror},
		{"stopped", Snapshot{Topics: topic, Mirrors: mir("STOPPED")}, StoppedFormerMirror},
		{"stopped no topic", Snapshot{Mirrors: mir("STOPPED")}, Unknown},
		{"active no topic", Snapshot{Mirrors: mir("ACTIVE")}, Unknown},
		{"weird", Snapshot{Topics: topic, Mirrors: mir("WEIRD")}, Unknown},
		{"err", Snapshot{Err: errors.New("boom")}, Unknown},
		{"err with data", Snapshot{Err: errors.New("boom"), Topics: topic}, Unknown},
	}
	for _, c := range cases {
		if got := c.s.Classify("x"); got != c.want {
			t.Errorf("%s: got %s want %s", c.name, got, c.want)
		}
	}
	if (Snapshot{Err: errors.New("e")}).Complete() || !(Snapshot{}).Complete() {
		t.Error("Complete wrong")
	}
}

func fixtures(admin, rest string) (*kmock.Client, *rmock.Client) {
	k := kmock.New([]kafka.TopicState{{Name: "x"}}, nil)
	k.SetClusterID(admin)
	return k, rmock.New(rest)
}

func TestReadSnapshotOK(t *testing.T) {
	k, r := fixtures("A", "A")
	r.AddLink(kafkarest.Link{Name: "l", State: "ACTIVE"}, map[string]string{"a": "b"})
	r.AddMirror(kafkarest.Mirror{LinkName: "l", MirrorTopicName: "x", Status: "ACTIVE"})
	s := ReadSnapshot(context.Background(), k, r, []string{"l", "missing"})
	if s.Err != nil || s.Classify("x") != Mirror {
		t.Fatalf("%v %s", s.Err, s.Classify("x"))
	}
	if s.Links["l"].Configs["a"] != "b" {
		t.Error("configs missing")
	}
	if _, ok := s.Links["missing"]; ok {
		t.Error("absent link must have no entry")
	}
}

func TestReadSnapshotClusterIDMismatch(t *testing.T) {
	k, r := fixtures("A", "B")
	s := ReadSnapshot(context.Background(), k, r, nil)
	if !errors.Is(s.Err, ErrClusterIdentityMismatch) {
		t.Fatal(s.Err)
	}
	if !strings.Contains(s.Err.Error(), "A") || !strings.Contains(s.Err.Error(), "B") {
		t.Error(s.Err)
	}
	if s.Classify("x") != Unknown {
		t.Error("must be Unknown")
	}
}

func TestReadSnapshotAdminIDError(t *testing.T) {
	k, r := fixtures("A", "A")
	k.FailOn("ClusterID", "", errors.New("down"))
	s := ReadSnapshot(context.Background(), k, r, nil)
	if !errors.Is(s.Err, ErrClusterIdentityMismatch) {
		t.Fatal(s.Err)
	}
}

func TestReadSnapshotRESTIDError(t *testing.T) {
	k, r := fixtures("A", "A")
	r.Fail("ClusterID", errors.New("down"))
	s := ReadSnapshot(context.Background(), k, r, nil)
	if !errors.Is(s.Err, ErrClusterIdentityMismatch) {
		t.Fatal(s.Err)
	}
}

func TestReadSnapshotListMirrorsFailure(t *testing.T) {
	k, r := fixtures("A", "A")
	r.Fail("ListMirrors", errors.New("boom"))
	s := ReadSnapshot(context.Background(), k, r, nil)
	if s.Err == nil || s.Classify("x") != Unknown {
		t.Fatal("want fail closed")
	}
}

func TestReadSnapshotListTopicsAndLinksFailure(t *testing.T) {
	k, r := fixtures("A", "A")
	k.FailOn("ListTopics", "", errors.New("boom"))
	if s := ReadSnapshot(context.Background(), k, r, nil); s.Err == nil {
		t.Error("ListTopics failure must set Err")
	}
	k, r = fixtures("A", "A")
	r.Fail("ListLinks", errors.New("boom"))
	if s := ReadSnapshot(context.Background(), k, r, nil); s.Err == nil {
		t.Error("ListLinks failure must set Err")
	}
}

func TestReadSnapshotLinkConfigFailureIsPerLink(t *testing.T) {
	k, r := fixtures("A", "A")
	r.AddLink(kafkarest.Link{Name: "l", State: "ACTIVE"}, nil)
	r.Fail("LinkConfigs", errors.New("boom"))
	s := ReadSnapshot(context.Background(), k, r, []string{"l"})
	if s.Err != nil || s.Links["l"].ConfigErr == nil {
		t.Fatalf("err=%v links=%v", s.Err, s.Links)
	}
}
