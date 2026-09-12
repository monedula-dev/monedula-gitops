package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/matrix"
)

func TestTierJSONIsCompactArray(t *testing.T) {
	got, err := tierJSON(matrix.TierIntegration)
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(got, `["`), "want a JSON array, got %q", got)
	assert.True(t, strings.HasSuffix(got, `"]`), "want a JSON array, got %q", got)
	assert.NotContains(t, got, "\n", "GITHUB_OUTPUT values must be single-line")
	assert.Contains(t, got, `"cp-8.3"`)
}

func TestTierJSONUnknownTierErrors(t *testing.T) {
	_, err := tierJSON("nonsense")
	require.Error(t, err)
}

func TestReadmeTableListsEveryCell(t *testing.T) {
	table := readmeTable()
	for _, c := range matrix.Cells() {
		assert.Contains(t, table, c.Image, "cell %q image missing from the table", c.ID)
	}
	assert.True(t, strings.HasPrefix(table, "|"), "want a markdown table, got %q", table[:20])
}

func TestSpliceBlockReplacesBetweenMarkers(t *testing.T) {
	doc := "before\n" + beginMarker + "\nstale\n" + endMarker + "\nafter\n"
	got, err := spliceBlock(doc, "fresh")
	require.NoError(t, err)

	assert.Equal(t, "before\n"+beginMarker+"\nfresh\n"+endMarker+"\nafter\n", got)
	assert.NotContains(t, got, "stale")
}

func TestSpliceBlockMissingMarkersErrors(t *testing.T) {
	_, err := spliceBlock("no markers here", "fresh")
	require.Error(t, err)
	assert.Contains(t, err.Error(), beginMarker)
}

func TestSpliceBlockIsIdempotent(t *testing.T) {
	doc := "before\n" + beginMarker + "\nstale\n" + endMarker + "\nafter\n"
	once, err := spliceBlock(doc, "fresh")
	require.NoError(t, err)
	twice, err := spliceBlock(once, "fresh")
	require.NoError(t, err)
	assert.Equal(t, once, twice)
}
