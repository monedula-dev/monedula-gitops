package e2e

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest/mock"
)

func ordersMock() *mock.Client {
	m := mock.New("c")
	m.AddMirror(kafkarest.Mirror{LinkName: "src-to-dest", MirrorTopicName: "orders", SourceTopicName: "orders-src", Status: "ACTIVE"})
	return m
}

func TestCheckMirrorsMatch(t *testing.T) {
	rep := CheckMirrors(context.Background(), ordersMock(),
		[]MirrorExpect{{Name: "orders", LinkName: "src-to-dest", SourceTopic: "orders-src", Status: "ACTIVE"}}, nil)
	if rep.Failed() || len(rep.Results) == 0 {
		t.Fatalf("expected pass:\n%s", rep.String())
	}
}

func TestCheckMirrorsStatusMismatch(t *testing.T) {
	rep := CheckMirrors(context.Background(), ordersMock(),
		[]MirrorExpect{{Name: "orders", Status: "PAUSED"}}, nil)
	if !rep.Failed() {
		t.Fatal("expected failure")
	}
	s := rep.String()
	if !strings.Contains(s, "PAUSED") || !strings.Contains(s, "ACTIVE") {
		t.Fatalf("detail should name both statuses:\n%s", s)
	}
}

func TestCheckMirrorsMissing(t *testing.T) {
	rep := CheckMirrors(context.Background(), ordersMock(), []MirrorExpect{{Name: "ledger"}}, nil)
	if !rep.Failed() {
		t.Fatal("expected failure")
	}
}

func TestCheckMirrorsAbsent(t *testing.T) {
	rep := CheckMirrors(context.Background(), ordersMock(), nil, []string{"ledger"})
	if rep.Failed() || len(rep.Results) != 1 {
		t.Fatalf("expected pass:\n%s", rep.String())
	}
	rep = CheckMirrors(context.Background(), ordersMock(), nil, []string{"orders"})
	if !rep.Failed() {
		t.Fatal("expected failure when mirror present")
	}
}

func TestCheckMirrorsReadErrorFails(t *testing.T) {
	m := ordersMock()
	m.Fail("ListMirrors", errors.New("boom"))
	rep := CheckMirrors(context.Background(), m, []MirrorExpect{{Name: "orders"}, {Name: "x"}}, []string{"y"})
	if len(rep.Results) != 3 {
		t.Fatalf("want 3 results, got %d", len(rep.Results))
	}
	for _, r := range rep.Results {
		if r.Pass || !strings.Contains(r.Detail, "boom") {
			t.Fatalf("result should fail with error: %+v", r)
		}
	}
}
