# Releasing

Releases are fully automated. Pushing a semver tag of the form `vX.Y.Z` triggers
the [`release.yaml`](../.github/workflows/release.yaml) workflow.

## Cutting a release

```bash
git tag v0.20.0
git push origin v0.20.0
```

This runs three jobs:

1. **`image`** — builds the Docker image (linux/amd64 + linux/arm64) and pushes it to
   `ghcr.io/monedula-dev/monedula-gitops` (tagged with the semver version and
   `latest`), with the version, commit, and build date stamped in via
   Docker build-args passed through to `-ldflags -X main.*`.
2. **`chart`** — lints, templates (sanity check), packages, and pushes the Helm
   chart to `oci://ghcr.io/monedula-dev/charts`. The chart's `version` and
   `appVersion` are set to the tag (not the values in `Chart.yaml`), so the
   published chart version and its default image tag always match the release.
3. **`binaries`** — runs [GoReleaser](https://goreleaser.com) to:
   - cross-compile `monedula-gitops` binaries for `linux` and `darwin`
     (`amd64` + `arm64`), with the version, commit, and build date stamped in
     via `-ldflags -X main.*`;
   - produce `tar.gz` archives plus a `checksums.txt`, attached to the
     GitHub Release;
   - generate the release changelog from GitHub commits; and
   - update the Homebrew cask in the `monedula-dev/homebrew-tap` repo.

After a release, users can install the CLI via:

```bash
brew install monedula-dev/tap/monedula-gitops
# or download a binary archive from the GitHub Release page
```

Verify a stamped build with `monedula-gitops version` (or `monedula-gitops --version`),
which prints `monedula-gitops <version> (commit <sha>, built <date>)`.

## One-time maintainer setup

The Homebrew step needs a tap repository and a token:

1. **Create the tap repo.** Create a public repository named
   `homebrew-tap` under the `monedula-dev` org (i.e. `monedula-dev/homebrew-tap`).
   It can start empty; GoReleaser commits the generated formula to it.
2. **Add the `HOMEBREW_TAP_TOKEN` secret.** In the `monedula-gitops` repo,
   add a repository secret named `HOMEBREW_TAP_TOKEN`. It must be a token with
   `contents:write` permission on the `homebrew-tap` repo:
   - a **fine-grained PAT** scoped to `monedula-dev/homebrew-tap` with
     *Contents: Read and write*, or
   - a **classic PAT** with the `repo` scope.

   The built-in `GITHUB_TOKEN` cannot write to a different repository, which is
   why a dedicated token is required for the tap.

## Local validation

The GoReleaser config is validated in CI, but you can check it locally during
development:

```bash
goreleaser check          # validates .goreleaser.yaml
goreleaser release --snapshot --clean   # dry-run a full build (no publish)
```
