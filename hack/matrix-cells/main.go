// Command matrix-cells renders internal/matrix/versions.yaml for consumers that
// cannot parse it themselves: the GitHub Actions job matrix (as a JSON array of
// cell ids) and the README support table (as a markdown block between markers).
//
// Using this tool rather than yq in the workflow means CI and the docs share the
// loader the unit tests cover, so they cannot disagree with each other.
//
// Usage:
//
//	go run ./hack/matrix-cells -tier=integration   # ["ak-3.9","ak-4.0",...]
//	go run ./hack/matrix-cells -readme=README.md   # rewrite the block in place
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/monedula-dev/monedula-gitops/internal/matrix"
)

const (
	beginMarker = "<!-- BEGIN GENERATED: version-matrix -->"
	endMarker   = "<!-- END GENERATED: version-matrix -->"
)

func main() {
	tier := flag.String("tier", "", "print the cell ids for this tier as a JSON array (integration|e2e)")
	readme := flag.String("readme", "", "rewrite the generated version-matrix block in this markdown file")
	flag.Parse()

	switch {
	case *tier != "":
		out, err := tierJSON(*tier)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(out)
	case *readme != "":
		if err := updateReadme(*readme); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
}

// tierJSON renders the tier's cell ids as a single-line JSON array, the shape
// GitHub Actions' fromJSON expects in a job output.
func tierJSON(tier string) (string, error) {
	cells := matrix.ForTier(tier)
	if len(cells) == 0 {
		return "", fmt.Errorf("matrix-cells: no cells in tier %q", tier)
	}
	ids := make([]string, 0, len(cells))
	for _, c := range cells {
		ids = append(ids, c.ID)
	}
	b, err := json.Marshal(ids)
	if err != nil {
		return "", fmt.Errorf("matrix-cells: marshalling tier %q: %w", tier, err)
	}
	return string(b), nil
}

// readmeTable renders every cell as a markdown table. No trailing newline: the
// splice adds the line breaks around the block.
func readmeTable() string {
	var b strings.Builder
	b.WriteString("| Cell | Broker image | Schema Registry image | Tiers | Capabilities |\n")
	b.WriteString("|---|---|---|---|---|\n")
	for _, c := range matrix.Cells() {
		sr := c.SchemaRegistryImage
		if sr == "" {
			sr = "—"
		}
		fmt.Fprintf(&b, "| `%s` | `%s` | `%s` | %s | %s |\n",
			c.ID, c.Image, sr,
			strings.Join(c.Tiers, ", "),
			strings.Join(c.Capabilities, ", "))
	}
	return strings.TrimRight(b.String(), "\n")
}

// spliceBlock replaces the content between the markers, leaving the markers and
// everything around them untouched.
func spliceBlock(doc, block string) (string, error) {
	start := strings.Index(doc, beginMarker)
	if start < 0 {
		return "", fmt.Errorf("matrix-cells: marker %q not found", beginMarker)
	}
	end := strings.Index(doc, endMarker)
	if end < 0 {
		return "", fmt.Errorf("matrix-cells: marker %q not found", endMarker)
	}
	if end < start {
		return "", fmt.Errorf("matrix-cells: %q appears before %q", endMarker, beginMarker)
	}
	return doc[:start+len(beginMarker)] + "\n" + block + "\n" + doc[end:], nil
}

func updateReadme(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("matrix-cells: reading %s: %w", path, err)
	}
	out, err := spliceBlock(string(raw), readmeTable())
	if err != nil {
		return err
	}
	if out == string(raw) {
		return nil // already current; don't touch the mtime
	}
	if err := os.WriteFile(path, []byte(out), 0o644); err != nil {
		return fmt.Errorf("matrix-cells: writing %s: %w", path, err)
	}
	return nil
}
