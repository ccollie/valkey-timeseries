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
# If environment variable SERVER_VERSION is not set, default to "unstable"
if [ -z "$SERVER_VERSION" ]; then
    echo "SERVER_VERSION environment variable is not set. Defaulting to \"unstable\"."
    export SERVER_VERSION="unstable"
fi
PROGNAME="${BASH_SOURCE[0]}"
CWD="$(cd "$(dirname "$PROGNAME")" &>/dev/null && pwd)"
BINARY_PATH="$CWD/build/binaries/$SERVER_VERSION/valkey-server"
PORT=${PORT:-6379}
ROOT=$(cd $CWD/.. && pwd)

export MODULE_PATH="$ROOT/target/$BUILD/libvalkey_timeseries${MODULE_EXT}"


REPO_URL="https://github.com/valkey-io/valkey.git"

# Rebuild the "unstable" binary when it is older than this many days; release
# versions (e.g. 9.0.4) are immutable and are only built when missing.
UNSTABLE_MAX_AGE_DAYS=${UNSTABLE_MAX_AGE_DAYS:-7}

NEEDS_BUILD=false
if [ -f "$BINARY_PATH" ] && [ -x "$BINARY_PATH" ]; then
    echo "valkey-server binary '$BINARY_PATH' found."
    if [ "$SERVER_VERSION" = "unstable" ]; then
        if [[ "$os_type" == "Darwin" ]]; then
            BINARY_MTIME=$(stat -f %m "$BINARY_PATH")
        else
            BINARY_MTIME=$(stat -c %Y "$BINARY_PATH")
        fi
        BINARY_AGE_DAYS=$(( ($(date +%s) - BINARY_MTIME) / 86400 ))
        if [ "$BINARY_AGE_DAYS" -ge "$UNSTABLE_MAX_AGE_DAYS" ]; then
            echo "Binary is $BINARY_AGE_DAYS days old (max $UNSTABLE_MAX_AGE_DAYS for \"unstable\"); rebuilding."
            NEEDS_BUILD=true
        fi
    fi
else
    echo "valkey-server binary '$BINARY_PATH' not found."
    NEEDS_BUILD=true
fi

if [ "$NEEDS_BUILD" = true ]; then
    mkdir -p "$CWD/build/binaries/$SERVER_VERSION"
    mkdir -p "$CWD/.build"
    cd "$CWD/.build"
    rm -rf valkey
    git clone "$REPO_URL"
    cd valkey
    git checkout "$SERVER_VERSION"
    make -j
    cp src/valkey-server "$CWD/build/binaries/$SERVER_VERSION/"
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
    python -m pytest --cache-clear -vvv ${TEST_FLAG} ./ ${TEST_PATTERN}
else
    python -m pytest --cache-clear -vvv ${TEST_FLAG} ./
fi
