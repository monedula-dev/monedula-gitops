package e2e

import (
	"context"
	"fmt"

	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
)

// SubjectProber reads Schema Registry subject state for the liveState subjects
// surface. Backed by schemaregistry.Client in production and the SR mock in tests.
type SubjectProber interface {
	// Subject reports whether the subject exists and its subject-level
	// compatibility ("" when unset/inherited). exists=false is not an error.
	Subject(ctx context.Context, name string) (exists bool, compatibility string, err error)
}

type schemaSubjectProber struct{ sr schemaregistry.Client }

// NewSchemaSubjectProber wraps a schemaregistry.Client as a SubjectProber.
func NewSchemaSubjectProber(sr schemaregistry.Client) SubjectProber {
	return &schemaSubjectProber{sr: sr}
}

func (p *schemaSubjectProber) Subject(ctx context.Context, name string) (bool, string, error) {
	st, err := p.sr.GetSubject(ctx, name)
	if err != nil {
		return false, "", err
	}
	if st == nil {
		return false, "", nil
	}
	compat, err := p.sr.GetCompatibility(ctx, name)
	if err != nil {
		return true, "", err
	}
	return true, compat, nil
}

// CheckSubjects asserts each ls.Subjects entry. A nil prober (no Schema Registry
// configured on the cluster) with asserted subjects is a failure, not a panic.
func CheckSubjects(ctx context.Context, sp SubjectProber, ls LiveState) Report {
	var rep Report
	if len(ls.Subjects) == 0 {
		return rep
	}
	if sp == nil {
		for _, se := range ls.Subjects {
			rep.Add(CheckResult{Name: fmt.Sprintf("subject %s", se.Name), Pass: false,
				Detail: "no Schema Registry configured on the cluster (cannot assert subjects)"})
		}
		return rep
	}
	for _, se := range ls.Subjects {
		exists, compat, err := sp.Subject(ctx, se.Name)
		if err != nil {
			rep.Add(CheckResult{Name: fmt.Sprintf("subject %s", se.Name), Pass: false,
				Detail: fmt.Sprintf("probe error: %v", err)})
			continue
		}
		if !exists {
			rep.Add(CheckResult{Name: fmt.Sprintf("subject %s exists", se.Name), Pass: false,
				Detail: fmt.Sprintf("subject %q not found in registry", se.Name)})
			continue
		}
		rep.Add(CheckResult{Name: fmt.Sprintf("subject %s exists", se.Name), Pass: true})
		if se.Compatibility != "" {
			pass := compat == se.Compatibility
			detail := ""
			if !pass {
				detail = fmt.Sprintf("expected compatibility %q, got %q", se.Compatibility, compat)
			}
			rep.Add(CheckResult{Name: fmt.Sprintf("subject %s compatibility %s", se.Name, se.Compatibility),
				Pass: pass, Detail: detail})
		}
	}
	return rep
}
