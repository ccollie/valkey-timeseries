#!/usr/bin/env bash

# Sometimes processes are left running when test is cancelled.
# Therefore, before build start, we kill all running test processes left from previous test run.
echo "Kill old running test"
pkill -9 -x Pytest || true
pkill -9 -f "valkey-server.*:" || true
pkill -9 -f Valgrind || true
pkill -9 -f "valkey-benchmark" || true

os_type=$(uname)
MODULE_EXT=".so"
if [[ "$os_type" == "Darwin" ]]; then
  MODULE_EXT=".dylib"
elif [[ "$os_type" == "Linux" ]]; then
  MODULE_EXT=".so"
elif [[ "$os_type" == "Windows" ]]; then
  MODULE_EXT=".dll"
else
  echo "Unsupported OS type: $os_type"
  exit 1
fi

BUILD=${BUILD:-debug}
VALKEY_VERSION=${VALKEY_VERSION:-unstable}
PROGNAME="${BASH_SOURCE[0]}"
CWD="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
BINARY_PATH="$CWD/.build/binaries/$VALKEY_VERSION/valkey-server"
PORT=${PORT:-6379}
ROOT=$(cd $CWD/.. && pwd)

export MODULE_PATH="$ROOT/target/$BUILD/libvalkey_timeseries${MODULE_EXT}"


REPO_URL="https://github.com/valkey-io/valkey.git"

# If environment variable SERVER_VERSION is not set, default to "unstable"
if [ -z "$SERVER_VERSION" ]; then
    echo "SERVER_VERSION environment variable is not set. Defaulting to \"unstable\"."
    export SERVER_VERSION="unstable"
fi

if [ -f "$BINARY_PATH" ] && [ -x "$BINARY_PATH" ]; then
    echo "valkey-server binary '$BINARY_PATH' found."
else
    echo "valkey-server binary '$BINARY_PATH' not found."
    mkdir -p ".build/binaries/$SERVER_VERSION"
    cd .build
    rm -rf valkey
    git clone "$REPO_URL"
    cd valkey
    git checkout "$SERVER_VERSION"
    make -j
    cp src/valkey-server ../binaries/$SERVER_VERSION/
fi

# cd to the current directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "${DIR}"

export SOURCE_DIR=$2
echo "Running integration tests against Valkey version $SERVER_VERSION"

if [[ ! -f "${BINARY_PATH}" ]] ; then
    echo "${BINARY_PATH} missing"
    exit 1
fi

if [[ ! -z "${TEST_PATTERN}" ]] ; then
    export TEST_PATTERN="-k ${TEST_PATTERN}"
fi

if [[ ! -z "${TEST_PATTERN}" ]] ; then
    pytest --cache-clear -v ${TEST_FLAG} ./ ${TEST_PATTERN}
else
    pytest --cache-clear -v ${TEST_FLAG} ./
fi
