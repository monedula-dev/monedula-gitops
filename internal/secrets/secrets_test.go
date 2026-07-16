package secrets

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/stretchr/testify/require"
)

func TestResolveEnv(t *testing.T) {
	t.Setenv("MG_TEST_SECRET", "hunter2")
	v, err := Resolve(v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "MG_TEST_SECRET"}}, "")
	require.NoError(t, err)
	require.Equal(t, "hunter2", v)
}

func TestResolveEnvMissing(t *testing.T) {
	_, err := Resolve(v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "MG_NOPE_XYZ"}}, "")
	require.Error(t, err)
}

func TestResolveFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "pw")
	require.NoError(t, os.WriteFile(p, []byte("filepass\n"), 0o600))
	v, err := Resolve(v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "pw"}}, dir)
	require.NoError(t, err)
	require.Equal(t, "filepass", v) // trailing newline trimmed
}

func TestResolveFileAbsolute(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "pw")
	require.NoError(t, os.WriteFile(p, []byte("abs"), 0o600))
	v, err := Resolve(v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: p}}, "/some/other/base")
	require.NoError(t, err)
	require.Equal(t, "abs", v) // absolute path ignores baseDir
}

func TestResolveSecretKeyRefUnsupported(t *testing.T) {
	_, err := Resolve(v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "s", Key: "k"}}}, "")
	require.ErrorContains(t, err, "secretKeyRef")
}

func TestResolveEmpty(t *testing.T) {
	_, err := Resolve(v1alpha1.ValueFrom{}, "")
	require.Error(t, err) // no source set
}

func TestFileEnvResolver_FileConfined(t *testing.T) {
	dir := t.TempDir()
	// A relative ref that escapes baseDir must be rejected.
	_, err := FileEnvResolver{BaseDir: dir}.Resolve(
		v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "../escape"}})
	require.ErrorContains(t, err, "escapes the base directory")
}

func TestFileEnvResolver_FileConfinedNested(t *testing.T) {
	dir := t.TempDir()
	// Deeper traversal that still escapes is rejected.
	_, err := FileEnvResolver{BaseDir: dir}.Resolve(
		v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "sub/../../escape"}})
	require.ErrorContains(t, err, "escapes the base directory")
}

func TestFileEnvResolver_RelativeStaysInside(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o755))
	p := filepath.Join(dir, "sub", "pw")
	require.NoError(t, os.WriteFile(p, []byte("inside"), 0o600))
	v, err := FileEnvResolver{BaseDir: dir}.Resolve(
		v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "sub/pw"}})
	require.NoError(t, err)
	require.Equal(t, "inside", v)
}

func TestFileEnvResolver_SecretKeyRefUnsupported(t *testing.T) {
	_, err := FileEnvResolver{}.Resolve(
		v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "s", Key: "k"}}})
	require.ErrorContains(t, err, "secretKeyRef")
}

func TestFileEnvResolver_InlineReturnsVerbatim(t *testing.T) {
	body := `{"type":"record","name":"Order","fields":[]}`
	v, err := FileEnvResolver{}.Resolve(
		v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Inline: body}})
	require.NoError(t, err)
	require.Equal(t, body, v)
}

func TestFileEnvResolver_InlinePreservesWhitespace(t *testing.T) {
	body := "  leading and trailing  \n"
	v, err := FileEnvResolver{}.Resolve(
		v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Inline: body}})
	require.NoError(t, err)
	require.Equal(t, body, v, "inline value must be returned verbatim, no trimming")
}

func TestFileEnvResolver_ConfigMapKeyRefUnsupported(t *testing.T) {
	_, err := FileEnvResolver{}.Resolve(
		v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: "cm", Key: "k"}}})
	require.ErrorContains(t, err, "configMapKeyRef is not supported in CLI mode")
}
