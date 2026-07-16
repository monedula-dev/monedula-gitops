# syntax=docker/dockerfile:1

# Build the monedula-gitops binary. The build stage always runs on the
# BUILDPLATFORM (native host arch, e.g. linux/amd64 on GitHub-hosted
# runners) rather than the requested TARGETPLATFORM: combined with
# CGO_ENABLED=0 and GOOS/GOARCH set explicitly on `go build`, this makes
# multi-arch builds fast cross-compilation instead of slow QEMU-emulated
# native builds. Go's toolchain cross-compiles natively, so nothing else
# needs to run under emulation.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
    -o /out/monedula-gitops ./cmd/monedula-gitops

# Minimal non-root runtime image.
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/monedula-gitops /usr/local/bin/monedula-gitops
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/monedula-gitops"]
# Default to operator mode; override `args` (e.g. ["validate","-f","..."]) for CLI use.
CMD ["operator"]
