package cli

import (
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/monedula-dev/monedula-gitops/internal/pipeline"
)

// sharedFlags holds the values for the flags every command exposes. A single
// instance is bound to a command's flag set via register and read back via
// options after parsing.
type sharedFlags struct {
	filenames          []string
	recursive          bool
	clusterConfigFiles []string
	cluster            string
	output             string
}

// register binds the shared flags onto cmd.
func (f *sharedFlags) register(cmd *cobra.Command) {
	fl := cmd.Flags()
	// StringArray (not StringSlice) deliberately: repeatable flags must NOT
	// comma-split, or paths containing commas break.
	fl.StringArrayVarP(&f.filenames, "filename", "f", nil, "manifest file or directory (repeatable; use - for stdin)")
	fl.BoolVarP(&f.recursive, "recursive", "R", false, "recurse into directories given to -f")
	fl.StringArrayVarP(&f.clusterConfigFiles, "cluster-config-file", "c", nil, "KafkaCluster config file or directory (repeatable)")
	fl.StringVar(&f.cluster, "cluster", "", "select/filter a single cluster by name")
	fl.StringVarP(&f.output, "output", "o", "human", "output format: human, yaml, or json")
}

// options validates the shared flags and converts them into pipeline.Options.
// requireClusters mirrors the pipeline option of the same name. Stdin is wired
// to os.Stdin only when a filename is "-".
func (f *sharedFlags) options(requireClusters bool) (pipeline.Options, error) {
	if len(f.filenames) == 0 {
		return pipeline.Options{}, &ExitError{Code: 2, Msg: "no manifests specified (use -f)"}
	}

	// Stdin stays nil unless a filename is "-", so the loader's Stdin == nil
	// check (which gates whether stdin is read at all) behaves correctly.
	var stdin io.Reader
	for _, name := range f.filenames {
		if name == "-" {
			stdin = os.Stdin
			break
		}
	}

	return pipeline.Options{
		Filenames:          f.filenames,
		Recursive:          f.recursive,
		Stdin:              stdin,
		ClusterConfigFiles: f.clusterConfigFiles,
		Cluster:            f.cluster,
		RequireClusters:    requireClusters,
	}, nil
}

// validateOutputFormat ensures the requested format is one the renderer accepts.
func validateOutputFormat(format string) error {
	switch format {
	case "human", "yaml", "json":
		return nil
	default:
		return &ExitError{Code: 2, Msg: "unsupported output format " + format + " (want human, yaml, or json)"}
	}
}
