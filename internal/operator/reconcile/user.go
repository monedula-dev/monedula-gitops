package reconcile

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/defaulting"
	"github.com/monedula-dev/monedula-gitops/internal/diff"
	"github.com/monedula-dev/monedula-gitops/internal/executor"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/tenancy"
	"github.com/monedula-dev/monedula-gitops/internal/user"
	"github.com/monedula-dev/monedula-gitops/internal/validation"
)

// Condition Reason strings specific to the KafkaUser reconcile.
const (
	// reasonPasswordUnresolved is set on Ready=False when the password source
	// could not be read this pass (referenced Secret missing / key absent /
	// read error, or a generated-Secret read/write failure). TRANSIENT: the
	// controller requeues with backoff, so a Secret created after the CR is
	// picked up even when it lacks the credential-source watch label.
	reasonPasswordUnresolved = "PasswordUnresolved"
	// reasonForeignSecret is set on ValidationFailed=True when generate mode
	// finds a Secret with the generated name that is NOT owned by this
	// KafkaUser. TERMINAL: the operator never adopts or overwrites a foreign
	// Secret — a human must delete/rename it (or rename the CR).
	reasonForeignSecret = "ForeignSecret"
)

// generatedSecretSuffix names the operator-owned Secret in generate mode:
// "<cr-name>-kafka-credentials" (see v1alpha1.UserPassword).
const generatedSecretSuffix = "-kafka-credentials"

// Data keys of the generated credentials Secret. username/mechanism are
// convenience metadata for the consuming workload's client config; password is
// the credential itself.
const (
	secretKeyUsername  = "username"
	secretKeyPassword  = "password"
	secretKeyMechanism = "mechanism"
)

// PasswordSecret is a PasswordSecretStore read result: the Secret's data, its
// resourceVersion (the referenced-mode rotation trigger), and whether it is
// controller-owned by the reconciling KafkaUser (the generate-mode adoption
// guard). It deliberately carries no client-go types so this package stays
// controller-runtime-free.
type PasswordSecret struct {
	Data map[string][]byte
	// ResourceVersion is the Secret's metadata.resourceVersion at read time.
	// The controller-side store reads Secrets UNCACHED (the manager client
	// disables Secret caching, §11.4 — the same read path operator.K8sResolver
	// uses), so this is the live apiserver value, and the data + version come
	// from the SAME read (no value/version skew).
	ResourceVersion string
	// Owned is true when the Secret has a controller owner reference to the
	// reconciling KafkaUser.
	Owned bool
}

// PasswordSecretStore is ReconcileUser's seam to Kubernetes Secrets in the
// CR's namespace. It exists (instead of passing a client.Client) so the
// reconcile engine stays controller-runtime-free, mirroring the
// secrets.Resolver seam; the controller implements it with the manager client
// + scheme (owner references), and unit tests with an in-memory fake.
type PasswordSecretStore interface {
	// GetSecret returns the named Secret, or (nil, nil) when it does not exist.
	GetSecret(ctx context.Context, name string) (*PasswordSecret, error)
	// CreateOwnedSecret creates the named Secret with data, controller-owned by
	// the reconciling KafkaUser (so Kubernetes garbage-collects it with the CR).
	CreateOwnedSecret(ctx context.Context, name string, data map[string][]byte) error
	// UpdateOwnedSecret replaces the named Secret's data. It MUST refuse to
	// touch a Secret not controller-owned by the reconciling KafkaUser.
	UpdateOwnedSecret(ctx context.Context, name string, data map[string][]byte) error
}

// staticPassword is a secrets.Resolver that yields one pre-resolved password
// regardless of the reference it is asked for. ReconcileUser determines the
// password BEFORE diff/apply (from the referenced Secret or the generated
// one), so the executor's resolve-at-execute-time hook simply returns that
// value — in generate mode there is no CR-side reference to resolve at all.
// The plaintext lives only in this value and the local variables of a single
// ReconcileUser call; it is never placed in ops, status, or any struct that
// outlives the reconcile.
type staticPassword string

// Resolve returns the pre-resolved password; the reference is ignored.
func (p staticPassword) Resolve(v1alpha1.ValueFrom) (string, error) { return string(p), nil }

// passwordSource is the outcome of sourcing a KafkaUser's password. Exactly
// one of the three shapes applies: terminal (terminalReason set), transient
// (transientErr set), or success (password populated).
type passwordSource struct {
	password string
	// rotate requests a RotateScramCredential for an in-sync credential:
	// referenced mode when the source Secret's resourceVersion moved (or was
	// never recorded), generate mode when a fresh password was just minted.
	// Out-of-sync credentials need no rotate op — their Create/Update already
	// writes the new password (see diff.computeUserOps).
	rotate bool
	// appliedRef is the appliedPasswordRef to commit to status after a
	// successful apply. Referenced (secretKeyRef) mode only; nil otherwise.
	appliedRef *v1alpha1.AppliedPasswordRef
	// generatedSecretName is the operator-owned Secret's name (generate mode).
	generatedSecretName string
	// terminalReason/terminalMsg report a terminal failure (nil reconcile error).
	terminalReason, terminalMsg string
	// transientErr reports a retryable failure (requeue with backoff).
	transientErr error
}

// ReconcileUser is the SCRAM-credential analogue of ReconcileQuota for a
// KafkaUser (v0.35). It validates the (defaulted) shape, sources the password
// (referenced Secret or operator-generated Secret), observes the live SCRAM
// credential identity, reuses diff.Compute + executor.Apply, and returns the
// status the controller should write plus a retryable-error signal.
//
// The returned status is ALWAYS populated. The returned error is non-nil ONLY
// for TRANSIENT failures the controller should requeue-with-backoff on: a
// live-state read failure (ListScramCredentials), an unreadable/missing
// password source, a generated-Secret write failure, or an apply with Failed
// ops. Terminal outcomes (ValidationFailed, TenancyDenied, ForeignSecret)
// set the Error phase + conditions but return a nil error. See the package doc.
//
// KafkaUser has no spec.reconciliation (see user.Desired): its ops always
// execute as Enforce — there is no DetectOnly/ObserveOnly arm here, unlike
// quota. The only gated op reachable from this path would be a standalone
// DeleteScramCredential, which the diff never emits (the finalizer path calls
// it directly); approvals are still passed from the CR's risk-gate annotations
// for uniformity with the other reconcile cores.
//
// Password semantics (the KafkaUser-specific part):
//
//   - Drift surface is ONLY the observable identity (username, mechanism,
//     iterations) — Kafka never exposes passwords (see user.Credential).
//     Password changes are therefore EVENT-driven:
//   - referenced (valueFrom.secretKeyRef): the source Secret's
//     resourceVersion is compared against status.appliedPasswordRef; a
//     mismatch (or an empty ref) requests a rotation of the in-sync
//     credential. After a successful apply the ref is updated, so steady
//     state performs no upserts.
//   - generate: the operator provisions Secret "<cr-name>-kafka-credentials"
//     (controller-owned). Deleting that Secret is the user's explicit
//     rotation request: a fresh password is generated and the Secret is
//     recreated. The Secret is ALWAYS persisted BEFORE the broker upsert:
//     if the upsert ran first and the process crashed before the write, the
//     new password would exist only on the broker — an unusable credential
//     nobody can read back. Secret-first means the worst crash outcome is a
//     stored-but-not-yet-applied password, which the next reconcile applies.
//
// Tenancy enforcement (spec §20.2): the cluster's namespace ALLOW-LIST applies
// to users like every other data-plane kind — otherwise any namespace could
// take over another team's principal. Usernames cannot be topic-prefix-scoped,
// so prefix-restricted namespaces get no additional check (the same documented
// limitation as quota entities).
func ReconcileUser(ctx context.Context, u *v1alpha1.KafkaUser, cluster *v1alpha1.KafkaCluster,
	k kafka.AdminClient, store PasswordSecretStore) (v1alpha1.KafkaUserStatus, error) {

	now := metav1.Now()
	st := v1alpha1.KafkaUserStatus{ObservedGeneration: u.Generation, LastCheckedTime: &now}
	// Seed conditions from the existing status so meta.SetStatusCondition can
	// preserve LastTransitionTime when a condition's Type+Status are unchanged.
	// The password-tracking fields are carried forward too, so terminal early
	// returns (validation/tenancy/foreign-Secret) never wipe rotation state.
	if u.Status != nil {
		st.Conditions = append([]metav1.Condition(nil), u.Status.Conditions...)
		st.AppliedPasswordRef = u.Status.AppliedPasswordRef.DeepCopy()
		st.GeneratedSecretName = u.Status.GeneratedSecretName
	}

	// Default BEFORE validation (username from metadata.name, mechanism
	// SCRAM-SHA-512, deletionPolicy Delete), mirroring ReconcileTopic. Objects
	// fetched through the typed client have an empty TypeMeta, so fill the
	// known apiVersion first.
	defaulting.User(u)
	if u.APIVersion == "" {
		u.APIVersion = v1alpha1.APIVersion
	}

	// Validate the (defaulted) spec BEFORE touching any live state. A failure
	// is terminal: Phase Error, ValidationFailed=True, no mutation, nil error.
	// ValidateUserShape is the single-resource entry; clusterRef resolution +
	// (clusterRef, username) uniqueness are cross-resource concerns not checked
	// here (mirrors ReconcileQuota).
	if verrs := validation.ValidateUserShape(u); len(verrs) > 0 {
		msg := joinErrMsgs(verrs)
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaUser, &st.Conditions, reasonValidationFailed, msg, u.Generation)
		return st, nil // terminal: needs a spec change
	}

	// Tenancy enforcement (spec §20.2): namespace allow-list only (see the
	// function doc for why usernames are not prefix-scoped). Runs AFTER shape
	// validation but BEFORE any live-state read, Secret write, or mutation,
	// mirroring ReconcileQuota. A denial is terminal.
	var clusterTenancy *v1alpha1.TenancyConfig
	if cluster != nil {
		clusterTenancy = cluster.Spec.Tenancy
	}
	if err := tenancy.CheckNamespace(clusterTenancy, u.Namespace); err != nil {
		msg := err.Error()
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaUser, &st.Conditions, reasonTenancyDenied, msg, u.Generation)
		return st, nil // terminal: needs a tenancy config or namespace change
	}

	// Validation passed: clear a stale ValidationFailed left by a prior pass
	// (review I11); see ReconcileTopic.
	setCond(&st.Conditions, v1alpha1.CondValidationFailed, metav1.ConditionFalse, reasonValid, "spec validated", u.Generation)

	desired := user.CompileDesired(u)

	// Live state, bounded to this CR's username (the diff scope is
	// declared-mechanism-only; other principals are never in play).
	liveCreds, err := k.ListScramCredentials(ctx, desired.Credential.Username)
	if err != nil {
		st.Phase = v1alpha1.PhaseError
		setCond(&st.Conditions, v1alpha1.CondClusterReachable, metav1.ConditionFalse, reasonLiveStateError, err.Error(), u.Generation)
		setCond(&st.Conditions, v1alpha1.CondReady, metav1.ConditionFalse, reasonLiveStateError, err.Error(), u.Generation)
		return st, err // transient: requeue with backoff
	}
	setCond(&st.Conditions, v1alpha1.CondClusterReachable, metav1.ConditionTrue, reasonObserved, "listed live SCRAM credentials", u.Generation)

	// Password sourcing (referenced Secret or generated Secret). Runs AFTER the
	// live read so generate mode never mints + persists a password it cannot
	// apply because the cluster is unreachable.
	ps := sourceUserPassword(ctx, u, desired.Credential, store)
	switch {
	case ps.terminalReason != "":
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaUser, &st.Conditions, ps.terminalReason, ps.terminalMsg, u.Generation)
		return st, nil // terminal: needs a human (spec change or Secret cleanup)
	case ps.transientErr != nil:
		st.Phase = v1alpha1.PhaseError
		setCond(&st.Conditions, v1alpha1.CondReady, metav1.ConditionFalse, reasonPasswordUnresolved, ps.transientErr.Error(), u.Generation)
		return st, ps.transientErr // transient: requeue with backoff
	}

	// Each mode owns its status tracking field; the other is cleared so a spec
	// flip between valueFrom and generate never reports stale state. (A Secret
	// generated by a previous generate-mode spec stays owner-ref'd and is
	// garbage-collected with the CR.)
	if u.Spec.Password.Generate != nil {
		st.GeneratedSecretName = ps.generatedSecretName
		st.AppliedPasswordRef = nil
	} else {
		st.GeneratedSecretName = ""
	}

	// The executor requires a password reference on upsert ops. CompileDesired
	// only sets one for valueFrom specs, so generate mode stamps a synthetic
	// ref naming the generated Secret — descriptive in op listings, and never
	// actually dereferenced (staticPassword ignores it).
	if desired.PasswordRef == nil {
		desired.PasswordRef = &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
			SecretKeyRef: &v1alpha1.SecretKeyRef{Name: ps.generatedSecretName, Key: secretKeyPassword},
		}}
	}

	// Diff THIS user only: computeUserOps scopes to the desired set, so a
	// single-element Desired.Users yields ops for exactly this credential
	// (undeclared live users/mechanisms are never touched). RotatePasswords is
	// all-or-nothing in the CLI; operator-side each reconcile diffs ONE CR, so
	// setting it here rotates exactly this credential.
	ops := diff.Compute(
		diff.Desired{Users: []user.Desired{desired}, RotatePasswords: ps.rotate},
		diff.Live{ScramCredentials: liveCreds},
	)

	res := executor.Apply(ctx, executor.Clients{Kafka: k, Passwords: staticPassword(ps.password)},
		ops, approvalsFromAnnotations(u.Annotations))
	st.LastAppliedTime = &now
	applyUserEnforceResult(&st, res, u.Generation)

	// Commit the rotation watermark only after a fully-successful apply: a
	// failed/blocked op keeps the previous ref so the next reconcile retries
	// the rotation instead of silently recording the new version as applied.
	if res.OK() && ps.appliedRef != nil {
		st.AppliedPasswordRef = ps.appliedRef
	}

	return st, applyRetryError(res)
}

// sourceUserPassword dispatches to the referenced- or generated-password
// sourcing path. The spec shape (password set, exactly one of
// valueFrom/generate) is already guaranteed by ValidateUserShape.
func sourceUserPassword(ctx context.Context, u *v1alpha1.KafkaUser, cred user.Credential,
	store PasswordSecretStore) passwordSource {

	if u.Spec.Password.Generate != nil {
		return sourceGeneratedPassword(ctx, u, cred, store)
	}
	return sourceReferencedPassword(ctx, u, store)
}

// sourceReferencedPassword reads the password from the valueFrom source.
// Operator-side only secretKeyRef is supported: env/file are CLI-only (the
// operator never reads its host environment or filesystem — mirrors
// operator.K8sResolver). inline and configMapKeyRef are rejected earlier by
// ValidateUserShape (ReconcileUser's validation pass) for the same
// plaintext/non-secret-storage reason, so in practice only env/file reach the
// terminal error below; it is kept as defense in depth. Rotation detection is
// resourceVersion-based and therefore exists for secretKeyRef sources ONLY.
func sourceReferencedPassword(ctx context.Context, u *v1alpha1.KafkaUser, store PasswordSecretStore) passwordSource {
	ref := u.Spec.Password.ValueFrom.SecretKeyRef
	if ref == nil {
		return passwordSource{
			terminalReason: reasonValidationFailed,
			terminalMsg: "spec.password.valueFrom must use secretKeyRef in operator mode " +
				"(env/file are CLI-only, and a ConfigMap is not a secret store)",
		}
	}

	sec, err := store.GetSecret(ctx, ref.Name)
	if err != nil {
		return passwordSource{transientErr: fmt.Errorf("reading password secret %q: %w", ref.Name, err)}
	}
	if sec == nil {
		return passwordSource{transientErr: fmt.Errorf("password secret %q not found", ref.Name)}
	}
	v, ok := sec.Data[ref.Key]
	if !ok {
		return passwordSource{transientErr: fmt.Errorf("password secret %q has no key %q", ref.Name, ref.Key)}
	}

	// Event-driven rotation: rotate when the source Secret's resourceVersion
	// differs from the last APPLIED one (or none was ever recorded — covers
	// both the first apply over a pre-existing in-sync credential and a ref
	// retargeted to a different Secret). Steady state (unchanged Secret,
	// credential in sync) emits no ops.
	rotate := true
	if applied := userAppliedRef(u); applied != nil &&
		applied.SecretName == ref.Name && applied.ResourceVersion == sec.ResourceVersion {
		rotate = false
	}

	return passwordSource{
		password:   string(v),
		rotate:     rotate,
		appliedRef: &v1alpha1.AppliedPasswordRef{SecretName: ref.Name, ResourceVersion: sec.ResourceVersion},
	}
}

// sourceGeneratedPassword provisions or reuses the operator-owned credentials
// Secret "<cr-name>-kafka-credentials".
//
//   - Secret exists + owned: the password is already provisioned — reuse it.
//     No rotation is requested: if the broker credential was deleted
//     out-of-band, the diff's Create re-upserts this same password. Stale
//     metadata keys (a mechanism change) are refreshed in place.
//   - Secret exists + NOT owned: terminal. Never adopt or overwrite a foreign
//     Secret — it may belong to another workload.
//   - Secret absent: first provisioning, or the user DELETED the Secret (the
//     explicit rotation request). Mint a fresh password and persist the Secret
//     FIRST — see the ReconcileUser doc for the crash-ordering rationale —
//     then request a rotation so an in-sync live credential is re-upserted
//     with the new password.
func sourceGeneratedPassword(ctx context.Context, u *v1alpha1.KafkaUser, cred user.Credential,
	store PasswordSecretStore) passwordSource {

	name := u.Name + generatedSecretSuffix

	sec, err := store.GetSecret(ctx, name)
	if err != nil {
		return passwordSource{transientErr: fmt.Errorf("reading generated secret %q: %w", name, err)}
	}

	if sec != nil && !sec.Owned {
		return passwordSource{
			terminalReason: reasonForeignSecret,
			terminalMsg: fmt.Sprintf("secret %q already exists and is not owned by this KafkaUser; "+
				"refusing to adopt or overwrite it — delete or rename that Secret (or this KafkaUser) to proceed", name),
		}
	}

	if sec != nil {
		if pw, ok := sec.Data[secretKeyPassword]; ok && len(pw) > 0 {
			// Keep the convenience metadata truthful: a mechanism change (or a
			// late-made-explicit username) re-stamps the keys, preserving the
			// password. Username itself is CEL-immutable once set.
			if string(sec.Data[secretKeyUsername]) != cred.Username ||
				string(sec.Data[secretKeyMechanism]) != cred.Mechanism {
				if uerr := store.UpdateOwnedSecret(ctx, name, generatedSecretData(cred, pw)); uerr != nil {
					return passwordSource{transientErr: fmt.Errorf("updating generated secret %q: %w", name, uerr)}
				}
			}
			return passwordSource{password: string(pw), generatedSecretName: name}
		}
		// Owned but missing/empty password key (hand-edited): self-heal by
		// minting a fresh password INTO the existing Secret, then rotating.
		// Secret-first ordering holds here too (Update before upsert).
		pw, gerr := generatePassword()
		if gerr != nil {
			return passwordSource{transientErr: gerr}
		}
		if uerr := store.UpdateOwnedSecret(ctx, name, generatedSecretData(cred, []byte(pw))); uerr != nil {
			return passwordSource{transientErr: fmt.Errorf("updating generated secret %q: %w", name, uerr)}
		}
		return passwordSource{password: pw, rotate: true, generatedSecretName: name}
	}

	pw, gerr := generatePassword()
	if gerr != nil {
		return passwordSource{transientErr: gerr}
	}
	// CREATE BEFORE UPSERT — never reorder (see ReconcileUser doc). A create
	// race (AlreadyExists) is transient: the next pass reads whatever won.
	if cerr := store.CreateOwnedSecret(ctx, name, generatedSecretData(cred, []byte(pw))); cerr != nil {
		return passwordSource{transientErr: fmt.Errorf("creating generated secret %q: %w", name, cerr)}
	}
	return passwordSource{password: pw, rotate: true, generatedSecretName: name}
}

// generatedSecretData builds the credentials Secret's data map.
func generatedSecretData(cred user.Credential, password []byte) map[string][]byte {
	return map[string][]byte{
		secretKeyUsername:  []byte(cred.Username),
		secretKeyPassword:  password,
		secretKeyMechanism: []byte(cred.Mechanism),
	}
}

// generatePassword returns a fresh password: 32 bytes from crypto/rand,
// encoded with the padding-free URL-safe base64 alphabet (43 chars, 256 bits
// of entropy, no shell/URL/YAML-hostile characters).
func generatePassword() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generating password: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// userAppliedRef returns the PRIOR status' appliedPasswordRef, nil-safe.
func userAppliedRef(u *v1alpha1.KafkaUser) *v1alpha1.AppliedPasswordRef {
	if u.Status == nil {
		return nil
	}
	return u.Status.AppliedPasswordRef
}

// ---- user mode handlers ----

// userStatusTarget adapts a KafkaUserStatus to the shared drift/Ready skeleton.
func userStatusTarget(st *v1alpha1.KafkaUserStatus) driftTarget {
	return driftTarget{
		conds: &st.Conditions,
		// drift is an intentional discard sink: KafkaUserStatus omits the Drift
		// field by design — the drift surface is the credential identity triple
		// and any divergence is either converged in the same (always-Enforce)
		// pass or reported through the DriftDetected condition. The driftTarget
		// contract requires a non-nil pointer, so a throwaway value is
		// allocated (mirrors roleBindingTarget).
		drift:    new(*v1alpha1.DriftStatus),
		setPhase: func(p string) { st.Phase = p },
	}
}

// applyUserEnforceResult sets the per-area UserSynced condition from an
// executor.Result, then delegates the drift/Ready/phase decision to the shared
// finishEnforce skeleton.
func applyUserEnforceResult(st *v1alpha1.KafkaUserStatus, res executor.Result, gen int64) {
	if res.OK() {
		setCond(&st.Conditions, v1alpha1.CondUserSynced, metav1.ConditionTrue, reasonReconciled, "all SCRAM credential operations succeeded", gen)
	} else {
		setCond(&st.Conditions, v1alpha1.CondUserSynced, metav1.ConditionFalse, reasonApplyIncomplete, applyFailureMsg(res), gen)
	}
	finishEnforce(userStatusTarget(st), res, gen)
}
