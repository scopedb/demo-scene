#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
    echo 'Usage: ./scripts/sync-bluesky.sh /path/to/scopedb-site' >&2
    exit 1
fi

site=$(cd "$1" && pwd)
scene=$(CDPATH= cd -- "$(dirname -- "$0")/../bluesky-events" && pwd)
source_dir="$site/public/examples/bluesky-events"

# Keep the snapshot attributable to a committed, reviewable public source.
git -C "$site" diff --exit-code HEAD -- public/examples/bluesky-events
for name in main.go main_test.go go.mod go.sum sample.ndjson; do
    test -f "$source_dir/$name"
done
for name in main.go main_test.go go.mod go.sum sample.ndjson; do
    cp "$source_dir/$name" "$scene/$name"
done
git -C "$site" rev-parse HEAD > "$scene/upstream-revision.txt"
printf 'Updated the Bluesky program and sample from %s\n' "$site"
