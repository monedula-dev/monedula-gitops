// Package user compiles KafkaUser resources into the observable credential
// identity Kafka actually exposes, and computes that identity's uniqueness
// key for cross-CR collision detection.
package user

import "github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"

// Credential is the OBSERVABLE identity of a SCRAM user: the triple Kafka's
// DescribeUserSCRAMs actually returns (username, mechanism, iterations).
// Kafka never exposes the password itself over that API — only which
// mechanism(s) a principal has configured and their iteration count — so
// this triple, and only this triple, IS the drift surface for a KafkaUser.
// Password changes are event-driven (rotation via Secret update / generate),
// never drift-detected: there is nothing to compare against.
//
// Iterations 0 means "unset" (the CR did not request a specific iteration
// count) and must be treated as "not compared" by drift logic, not as a
// literal iteration count of zero.
type Credential struct {
	Username   string
	Mechanism  string
	Iterations int32
}

// Compile builds the observable Credential from a KafkaUser. It assumes the
// CR has already been through defaulting.User (Username resolved from
// metadata.name, Mechanism defaulted to SCRAM-SHA-512): Compile performs no
// defaulting of its own and reads the spec fields verbatim. A nil
// Iterations pointer compiles to 0 (unset).
func Compile(u *v1alpha1.KafkaUser) Credential {
	c := Credential{
		Username:  u.Spec.Username,
		Mechanism: u.Spec.Mechanism,
	}
	if u.Spec.Iterations != nil {
		c.Iterations = *u.Spec.Iterations
	}
	return c
}

// Desired is a compiled KafkaUser for the diff/apply path (the user analogue
// of quota.Desired): the observable credential identity plus the password
// *reference*. There is no Mode field because KafkaUser has no
// spec.reconciliation — its ops always execute as Enforce.
type Desired struct {
	Credential Credential
	// PasswordRef references the password source (CLI: env|file; operator:
	// secretKeyRef). It is a reference by construction: the value is resolved
	// by the executor at execute time, immediately before the SCRAM upsert,
	// and never flows through the pipeline, the diff, or rendered output.
	// Nil only when the spec had no valueFrom source (generate mode, which
	// the CLI pipeline rejects upstream); the executor fails such an op
	// cleanly rather than upserting a blank password.
	PasswordRef *v1alpha1.ValueFrom
}

// CompileDesired builds the full desired state for a KafkaUser: the observable
// Credential (see Compile) plus the password reference. Like Compile it
// assumes defaulting.User has run and performs no defaulting of its own.
func CompileDesired(u *v1alpha1.KafkaUser) Desired {
	d := Desired{Credential: Compile(u)}
	if u.Spec.Password != nil && u.Spec.Password.ValueFrom != nil {
		d.PasswordRef = &v1alpha1.ValueFrom{ValueFrom: *u.Spec.Password.ValueFrom}
	}
	return d
}

// Key returns the cross-CR identity key for a Credential: (clusterRef,
// username) alone, joined with a NUL separator (the aclKey/quota-identity
// convention used elsewhere in this codebase). Mechanism and iterations are
// deliberately NOT part of the key: a Kafka principal owns exactly one
// credential set, so two KafkaUser CRs declaring the same username on the
// same cluster collide regardless of mechanism — a second CR would fight the
// first over the same principal's SCRAM credentials.
func Key(clusterRef, credentialUsername string) string {
	return clusterRef + "\x00" + credentialUsername
}
