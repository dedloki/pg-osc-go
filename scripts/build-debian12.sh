#!/usr/bin/env bash
# Static linux binaries (amd64, arm64) and .deb packages for Debian 12, cross-compiled
# with zig as the C compiler (pg_query_go is cgo) against musl: no runtime dependencies.
# Needs: go, zig, nfpm (go install github.com/goreleaser/nfpm/v2/cmd/nfpm@latest).
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=$(sed -nE 's/^var Version = "([^"]+)"/\1/p' internal/pgosc/log.go)
mkdir -p dist

for pair in amd64:x86_64 arm64:aarch64; do
  goarch=${pair%%:*}
  zigarch=${pair##*:}
  echo "== linux/$goarch"
  CGO_ENABLED=1 GOOS=linux GOARCH=$goarch \
    CC="zig cc -target $zigarch-linux-musl" \
    go build -trimpath -tags netgo,osusergo \
      -ldflags "-s -w -linkmode external -extldflags '-static -s'" \
      -o "dist/pg-online-schema-change-linux-$goarch" ./cmd/pg-online-schema-change
  mkdir -p dist/stage && cp "dist/pg-online-schema-change-linux-$goarch" dist/stage/pg-online-schema-change
  ARCH=$goarch VERSION=$VERSION nfpm package --packager deb --config nfpm.yaml --target dist/
done
rm -r dist/stage

ls -la dist/
