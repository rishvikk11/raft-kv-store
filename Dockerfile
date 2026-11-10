# syntax=docker/dockerfile:1
#
# A multi-stage build: two stages, and only the second one becomes the image
# you run. The first stage is a big kitchen (the whole Go toolchain, hundreds of
# megabytes) used to cook the program. The second stage is an empty plate that
# receives only the finished program. Nothing from the kitchen comes along.

# ---------- Stage 1: the kitchen (build the program) ----------
# If the build complains the Go toolchain is too old, raise this to match the
# "go" line in go.mod (run `go version` on your machine).
ARG GO_VERSION=1.27
FROM golang:${GO_VERSION} AS build
WORKDIR /src

# Each instruction below creates one LAYER. Docker remembers a layer and reuses
# it next time if nothing that went into it changed, so rebuilds are fast.
# That is why the things that change least come first.
COPY go.mod ./
COPY raft ./raft
COPY kv ./kv
COPY cmd/kvserver ./cmd/kvserver

# CGO_ENABLED=0 makes a fully self-contained program (it needs no operating
# system libraries), which is what lets the final image be completely empty.
# -s -w drop debugging information to make the file smaller.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/kvserver ./cmd/kvserver

# An empty folder to hold the node's saved state (see the volume below).
RUN mkdir /out/data

# ---------- Stage 2: the plate (what actually runs) ----------
# "scratch" is an empty image: no shell, no tools, no operating system files.
FROM scratch

# 65532 is an ordinary user number, not the all-powerful root user. If someone
# broke into this program they would have very little power inside the container.
COPY --from=build --chown=65532:65532 /out/data /data
COPY --from=build /out/kvserver /kvserver
USER 65532:65532

# The node's saved state (term, vote, ledger) lives in /data. When a container is
# started with a named volume mounted here, that volume is empty the first time,
# and Docker fills it with this folder, including who owns it, so the program can
# write there.
VOLUME /data

# The port the server listens on inside the container.
EXPOSE 8080

# The flags (-id, -peers, ...) are supplied by docker-compose.yml.
ENTRYPOINT ["/kvserver"]