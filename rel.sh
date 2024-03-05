#!/bin/sh

vv="`git describe --tags --abbrev=0`"
tt="`TZ=UTC date +'%Z %F'`"

tar -czf /out/"mochi-${vv}-linux-amd64.tar.gz" --owner=root --group=root --numeric-owner --mtime="${tt}" "mochi-${vv}-linux-amd64"
tar -czf /out/"mochi-${vv}-linux-aarch64.tar.gz" --owner=root --group=root --numeric-owner --mtime="${tt}" "mochi-${vv}-linux-aarch64"
