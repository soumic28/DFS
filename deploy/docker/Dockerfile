# One parameterised Dockerfile builds all four binaries. Pass SERVICE to pick.
#
#   docker build -f deploy/docker/Dockerfile --build-arg SERVICE=dfs-node .
#
# The result is a static binary on distroless: no shell, no package manager,
# no libc, running as a non-root user. Images land around 15 MB, which matters
# when six node containers share a 100 GB disk.

ARG GO_VERSION=1.24

# --- build stage ---------------------------------------------------------
FROM golang:${GO_VERSION}-alpine AS build

ARG SERVICE
ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /src

# Dependency layer: changes only when go.mod/go.sum change, so editing Go
# source does not re-download the module graph.
COPY go.* ./
RUN go mod download

COPY . .

# Keeps the build working before the first `make tidy` has committed a go.sum.
# Once go.sum is in the repo this becomes a no-op and can be deleted.
RUN go mod tidy

RUN set -eu; \
    if [ -z "${SERVICE}" ]; then \
      echo "build arg SERVICE is required (dfs-meta|dfs-node|dfs-gateway|dfsctl)" >&2; \
      exit 1; \
    fi; \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w \
        -X github.com/soumi/dfs/internal/app.Version=${VERSION} \
        -X github.com/soumi/dfs/internal/app.Commit=${COMMIT}" \
      -o /out/service ./cmd/${SERVICE}

# Staged so the runtime image contains a /data owned by the non-root user.
# Docker seeds a new named volume from the image path's ownership, so without
# this the volume would be root-owned and a non-root storage node could not
# write a single chunk to it.
RUN mkdir -p /out/data

# --- runtime stage -------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

ARG SERVICE
ARG VERSION=dev
ARG COMMIT=unknown

LABEL org.opencontainers.image.title="${SERVICE}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.source="https://github.com/soumi/dfs"

COPY --from=build /out/service /service
COPY --from=build --chown=nonroot:nonroot /out/data /data

USER nonroot:nonroot
EXPOSE 8080 9090 9091 9100

ENTRYPOINT ["/service"]
