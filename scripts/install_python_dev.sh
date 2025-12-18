#!/usr/bin/env bash
set -euo pipefail

# This helper builds a CPython interpreter with --enable-shared so PyO3 can link
# against libpython when running tc-server tests locally. Use it when your
# distro Python (or venv) does not provide libpython3.X.so.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PY_VERSION="${TC_PY_VERSION:-3.12.4}"
PREFIX="${TC_PYO3_PY_PREFIX:-$REPO_ROOT/python312-shared}"
SRC_DIR="${TC_PY_SRC_DIR:-/tmp/Python-$PY_VERSION}"
TARBALL="Python-$PY_VERSION.tgz"
URL="https://www.python.org/ftp/python/$PY_VERSION/$TARBALL"

echo ">>> Installing build prerequisites (sudo)"
sudo apt-get update
sudo apt-get install -y build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev libffi-dev \
    liblzma-dev tk-dev wget

mkdir -p /tmp
cd /tmp

if [ ! -d "$SRC_DIR" ]; then
    echo ">>> Downloading CPython $PY_VERSION"
    curl -LO "$URL"
    tar -xf "$TARBALL"
fi

cd "$SRC_DIR"

echo ">>> Configuring CPython (prefix: $PREFIX)"
./configure --enable-shared --enable-optimizations --prefix="$PREFIX"

echo ">>> Building CPython"
make -j"$(nproc)"

echo ">>> Installing to $PREFIX (sudo may be required for prefix outside \$HOME)"
make install

cat <<EOF

CPython with shared libpython installed at: $PREFIX

Add the following to your shell before running \`cargo test --features pyo3\`:

    export PYO3_PYTHON="$PREFIX/bin/python${PY_VERSION%.*}"
    export LD_LIBRARY_PATH="$PREFIX/lib:\$LD_LIBRARY_PATH"

You can override the install prefix by setting TC_PYO3_PY_PREFIX, or the source
version via TC_PY_VERSION before running this script.
EOF
