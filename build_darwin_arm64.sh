#!/usr/bin/env bash

set -e
scriptdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"

# vcpkg install --x-install-root="$VCPKG_ROOT/artifacts"

mkdir -p build
cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -G Ninja -B ./build -S .
