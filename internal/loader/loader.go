package loader

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	yamlv3 "gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

type Options struct {
	Filenames []string
	Recursive bool
	Stdin     io.Reader // used when a filename is "-"
}

// Object is a parsed manifest tagged with its source for diagnostics.
type Object struct {
	Kind        string
	Source      string // file path or "stdin"
	Topic       *v1alpha1.KafkaTopic
	Policy      *v1alpha1.KafkaAccessPolicy
	Cluster     *v1alpha1.KafkaCluster
	Quota       *v1alpha1.KafkaQuota
	RoleBinding *v1alpha1.KafkaRoleBinding
	User        *v1alpha1.KafkaUser
}

var supportedExt = map[string]bool{".yaml": true, ".yml": true, ".json": true}

func Load(opts Options) ([]Object, error) {
	var out []Object
	// Dedupe repeated identical inputs by absolute path ("-" included: stdin is
	// a one-shot stream). Loading the same file twice would duplicate resources
	// and trigger spurious identity-collision validation errors downstream.
	// This single "seen" set also covers files discovered by walking a
	// directory argument, so e.g. `-f dir -f dir/a.yaml` loads a.yaml once
	// regardless of argument order.
	seen := map[string]bool{}
	for _, fn := range opts.Filenames {
		if fn == "-" {
			if seen["-"] {
				continue
			}
			seen["-"] = true
			if opts.Stdin == nil {
				// I15: callers that deliberately pass Stdin: nil (the pipeline's
				// cluster-config load, import) must get an error, not a panic from
				// a nil reader fed to the YAML decoder.
				return nil, fmt.Errorf("stdin requested (-) but no stdin available")
			}
			docs, err := readDocs(opts.Stdin, "stdin")
			if err != nil {
				return nil, err
			}
			out = append(out, docs...)
			continue
		}
		abs, err := filepath.Abs(fn)
		if err != nil {
			return nil, fmt.Errorf("cannot resolve %q: %w", fn, err)
		}
		info, err := os.Stat(fn)
		if err != nil {
			return nil, fmt.Errorf("cannot read %q: %w", fn, err)
		}
		if info.IsDir() {
			// The directory argument itself is not a loadable unit — only the
			// files within it are — so it is not recorded in seen. Repeating
			// the same directory arg would just re-walk and re-skip already
			// seen files below.
			files, err := walkDir(fn, opts.Recursive)
			if err != nil {
				return nil, err
			}
			for _, f := range files {
				fAbs, err := filepath.Abs(f)
				if err != nil {
					return nil, fmt.Errorf("cannot resolve %q: %w", f, err)
				}
				if seen[fAbs] {
					continue
				}
				seen[fAbs] = true
				docs, err := readFile(f)
				if err != nil {
					return nil, err
				}
				out = append(out, docs...)
			}
			continue
		}
		if seen[abs] {
			continue
		}
		seen[abs] = true
		docs, err := readFile(fn)
		if err != nil {
			return nil, err
		}
		out = append(out, docs...)
	}
	return out, nil
}

func walkDir(dir string, recursive bool) ([]string, error) {
	var files []string
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		p := filepath.Join(dir, e.Name())
		if e.IsDir() {
			if recursive {
				sub, err := walkDir(p, true)
				if err != nil {
					return nil, err
				}
				files = append(files, sub...)
			}
			continue
		}
		if supportedExt[strings.ToLower(filepath.Ext(e.Name()))] {
			files = append(files, p)
		}
	}
	return files, nil
}

func readFile(path string) ([]Object, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return readDocs(f, path)
}

// readDocs splits a stream into YAML documents and decodes each into a typed Object.
func readDocs(r io.Reader, source string) ([]Object, error) {
	dec := yamlv3.NewDecoder(r)
	var out []Object
	for {
		var raw yamlv3.Node
		err := dec.Decode(&raw)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", source, err)
		}
		if raw.Kind == 0 {
			continue // empty document
		}
		yml, err := yamlv3.Marshal(&raw)
		if err != nil {
			return nil, err
		}
		obj, err := decodeTyped(yml, source)
		if err != nil {
			return nil, err
		}
		out = append(out, obj)
	}
	return out, nil
}

func decodeTyped(yml []byte, source string) (Object, error) {
	var tm metav1.TypeMeta
	// Lenient by design: this sniff only ever reads apiVersion/kind out of a
	// full document, so unrelated spec fields must not trip it up.
	if err := yaml.Unmarshal(yml, &tm); err != nil {
		return Object{}, fmt.Errorf("%s: %w", source, err)
	}
	o := Object{Kind: tm.Kind, Source: source}
	// Strict decode below: an unknown/typo'd field (e.g. "configs:" instead of
	// "config:") must fail loudly rather than being silently dropped, since a
	// dropped field means validate/apply run against a different desired
	// state than the user wrote.
	var err error
	switch tm.Kind {
	case "KafkaTopic":
		o.Topic = &v1alpha1.KafkaTopic{}
		err = yaml.UnmarshalStrict(yml, o.Topic)
	case "KafkaAccessPolicy":
		o.Policy = &v1alpha1.KafkaAccessPolicy{}
		err = yaml.UnmarshalStrict(yml, o.Policy)
	case "KafkaCluster":
		o.Cluster = &v1alpha1.KafkaCluster{}
		err = yaml.UnmarshalStrict(yml, o.Cluster)
	case "KafkaQuota":
		o.Quota = &v1alpha1.KafkaQuota{}
		err = yaml.UnmarshalStrict(yml, o.Quota)
	case "KafkaRoleBinding":
		o.RoleBinding = &v1alpha1.KafkaRoleBinding{}
		err = yaml.UnmarshalStrict(yml, o.RoleBinding)
	case "KafkaUser":
		o.User = &v1alpha1.KafkaUser{}
		err = yaml.UnmarshalStrict(yml, o.User)
	default:
		return Object{}, fmt.Errorf("%s: unknown kind %q", source, tm.Kind)
	}
	if err != nil {
		return Object{}, fmt.Errorf("%s: %w", source, err)
	}
	return o, nil
}
