# Contributing

Thanks for your interest in improving monedula-gitops!

## Development

- Go 1.26+ is required.
- Build: `go build ./...`
- Unit/acceptance tests: `go test ./...`
- CLI end-to-end tests (Docker): `go test -tags e2e ./test/e2e/cli/`
- Kubernetes end-to-end tests (kind + bats): `make e2e-k8s`
- Lint: `go vet ./...` and `gofmt -l .` (no output expected).

## Pull requests

- Branch from `main`; keep PRs focused.
- Ensure `go build ./...`, `go vet ./...`, and `go test ./...` pass.
- Update `CHANGELOG.md` under `## [Unreleased]` for user-facing changes.
- Match the existing code style; run `gofmt`.

## Reporting bugs / requesting features

Use the GitHub issue templates. For security issues, see [SECURITY.md](SECURITY.md).
