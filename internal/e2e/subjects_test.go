package e2e

import (
	"context"
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	srmock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

func TestCheckSubjects(t *testing.T) {
	sr := srmock.New()
	ctx := context.Background()
	if _, err := sr.RegisterSchema(ctx, "orders-value", schemaregistry.Schema{Type: "AVRO", Definition: `{"type":"record","name":"V","fields":[]}`}); err != nil {
		t.Fatal(err)
	}
	if err := sr.SetCompatibility(ctx, "orders-value", "BACKWARD"); err != nil {
		t.Fatal(err)
	}
	sp := NewSchemaSubjectProber(sr)

	ok := LiveState{Subjects: []SubjectExpect{{Name: "orders-value", Compatibility: "BACKWARD"}}}
	if rep := CheckSubjects(ctx, sp, ok); rep.Failed() {
		t.Errorf("expected pass:\n%s", rep.String())
	}
	exist := LiveState{Subjects: []SubjectExpect{{Name: "orders-value"}}}
	if rep := CheckSubjects(ctx, sp, exist); rep.Failed() {
		t.Errorf("existence-only should pass:\n%s", rep.String())
	}
	miss := LiveState{Subjects: []SubjectExpect{{Name: "ghost-value"}}}
	if rep := CheckSubjects(ctx, sp, miss); !rep.Failed() || !strings.Contains(rep.String(), "ghost-value") {
		t.Errorf("missing subject should fail and name it:\n%s", rep.String())
	}
	wrong := LiveState{Subjects: []SubjectExpect{{Name: "orders-value", Compatibility: "FULL"}}}
	if rep := CheckSubjects(ctx, sp, wrong); !rep.Failed() {
		t.Errorf("wrong compatibility should fail:\n%s", rep.String())
	}
}

func TestCheckSubjectsNilProber(t *testing.T) {
	rep := CheckSubjects(context.Background(), nil, LiveState{Subjects: []SubjectExpect{{Name: "x"}}})
	if !rep.Failed() {
		t.Errorf("nil prober with asserted subjects should fail:\n%s", rep.String())
	}
}

// errSubjectProber always errors, proving CheckSubjects records a probe error as
// a failed check (not a panic). The SR mock's read methods can't inject errors,
// so a stub is the cleanest way to exercise this branch. errReason is defined in
// live_test.go (same package).
type errSubjectProber struct{}

func (errSubjectProber) Subject(context.Context, string) (bool, string, error) {
	return false, "", errReason("registry unreachable")
}

func TestCheckSubjectsProbeError(t *testing.T) {
	rep := CheckSubjects(context.Background(), errSubjectProber{},
		LiveState{Subjects: []SubjectExpect{{Name: "orders-value"}}})
	if !rep.Failed() || !strings.Contains(rep.String(), "orders-value") ||
		!strings.Contains(rep.String(), "registry unreachable") {
		t.Errorf("probe error should fail and name the subject + error:\n%s", rep.String())
	}
}
