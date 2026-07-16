// Package secrets resolves secret references to plaintext. Resolved values MUST
// never be logged or included in error messages.
package secrets

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// Resolver resolves secret references to their plaintext (or PEM) values. The
// CLI uses FileEnvResolver (env/file); the operator supplies a
// Kubernetes-Secret backed implementation. TLS material (CA cert, client
// cert/key) resolves through the same Resolve path as credentials.
type Resolver interface {
	// Resolve returns the plaintext for a value reference.
	Resolve(vf v1alpha1.ValueFrom) (string, error)
}

// FileEnvResolver resolves from environment variables and files (CLI mode).
// secretKeyRef and configMapKeyRef are unsupported (Kubernetes-only).
type FileEnvResolver struct {
	BaseDir string
}

// compile-time assertion that FileEnvResolver implements Resolver.
var _ Resolver = FileEnvResolver{}

// Resolve resolves a ValueFrom reference to its plaintext value from an
// environment variable, a file, or an inline literal.
//
// File references are resolved relative to BaseDir unless they are absolute.
// Relative paths are confined under BaseDir: a reference that escapes the base
// directory (via "..") is rejected. Resolved values are never logged or embedded
// in returned errors.
func (r FileEnvResolver) Resolve(vf v1alpha1.ValueFrom) (string, error) {
	src := vf.ValueFrom

	switch {
	case src.Inline != "":
		return src.Inline, nil

	case src.Env != "":
		v, ok := os.LookupEnv(src.Env)
		if !ok {
			return "", fmt.Errorf("env var %q not set", src.Env)
		}
		return v, nil

	case src.File != "":
		path, err := safeJoin(r.BaseDir, src.File)
		if err != nil {
			return "", err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("reading secret file %q: %w", path, err)
		}
		return strings.TrimRight(string(b), "\r\n"), nil

	case src.SecretKeyRef != nil:
		return "", errors.New("secretKeyRef is not supported in CLI mode (use env or file)")

	case src.ConfigMapKeyRef != nil:
		return "", errors.New("configMapKeyRef is not supported in CLI mode")

	default:
		return "", errors.New("no secret source specified (set env or file)")
	}
}

// Resolve resolves a ValueFrom reference relative to baseDir using a
// FileEnvResolver. It is a thin wrapper preserved for existing callers.
//
// File references are resolved relative to baseDir unless they are absolute.
// Resolved values are never logged or embedded in returned errors.
func Resolve(vf v1alpha1.ValueFrom, baseDir string) (string, error) {
	return FileEnvResolver{BaseDir: baseDir}.Resolve(vf)
}

// SafeJoin resolves file relative to baseDir, confining relative paths under
// baseDir. Absolute paths are returned unchanged (v0.2/v0.4 behavior: absolute
// file refs are read as-is). A relative path that escapes baseDir via ".."
// returns an error. It is exported for callers (e.g. pipeline) that read files
// using the same confinement rules.
func SafeJoin(baseDir, file string) (string, error) {
	return safeJoin(baseDir, file)
}

// SafeJoinUnder resolves file relative to joinDir but confines the result under
// confineDir (which may be an ancestor of joinDir). This supports layouts where
// a relative ref legitimately hops to a sibling directory (e.g. "../schemas/x"
// from a "<ns>/topics" manifest dir, confined under "<ns>"), while still
// rejecting deeper "../.." traversal that escapes confineDir. Absolute paths are
// returned unchanged.
func SafeJoinUnder(joinDir, confineDir, file string) (string, error) {
	if filepath.IsAbs(file) {
		return file, nil
	}
	if joinDir == "" {
		joinDir = "."
	}
	if confineDir == "" {
		confineDir = "."
	}
	joined := filepath.Join(joinDir, file)
	if escapes(confineDir, joined) {
		return "", fmt.Errorf("file reference %q escapes the base directory", file)
	}
	return joined, nil
}

// safeJoin resolves file relative to baseDir with path-traversal confinement.
//
// Absolute paths are allowed and returned unchanged (preserving existing
// CLI behavior of reading absolute file refs). A relative path is cleaned and
// joined under baseDir; if the cleaned result escapes baseDir (via ".."), it is
// rejected.
func safeJoin(baseDir, file string) (string, error) {
	if filepath.IsAbs(file) {
		return file, nil
	}
	if baseDir == "" {
		baseDir = "."
	}
	joined := filepath.Join(baseDir, file)
	if escapes(baseDir, joined) {
		return "", fmt.Errorf("file reference %q escapes the base directory", file)
	}
	return joined, nil
}

// escapes reports whether the cleaned path leaves dir (via ".." traversal).
func escapes(dir, path string) bool {
	rel, err := filepath.Rel(filepath.Clean(dir), path)
	if err != nil {
		return true
	}
	return rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
