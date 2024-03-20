#!/bin/sh

out=$1

src="$PWD"

vv="`git describe --tags --abbrev=0`"
tt="`TZ=UTC date +'%Z %F'`"

for arch in amd64 arm64; do
	arch_out="mochi-${vv}-linux-${arch}"
	mkdir -p "$out/$arch_out"
	cp dist/example_config*.yaml "$out/$arch_out/"
	CGO_ENABLED=0 GOARCH=${arch} go build -trimpath -o "$out/$arch_out/mochi" ./cmd/mochi
	strip "$out/$arch_out/mochi"
	cd "$out"
	tar -czf "${arch_out}".tar.gz --owner=root --group=root --numeric-owner --mtime="${tt}" "$arch_out"
	cd "$src"
done

