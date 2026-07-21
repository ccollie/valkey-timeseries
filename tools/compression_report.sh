#!/usr/bin/env bash

# Builds and runs the `compression_report` binary, which writes an
# encoding x workload x timestamp-model x chunk-size compression matrix to
# target/bench-reports/.
#
#   tools/compression_report.sh                    # write the report
#   tools/compression_report.sh --check            # fail if ratios regressed >5%
#   tools/compression_report.sh --save-baseline    # write, then record as baseline
#   tools/compression_report.sh --baseline p.csv --check
#   tools/compression_report.sh --by-workload      # extra pivot: rows workload/ts_model,
#                                                  # columns encoding, cells = ratio
#   tools/compression_report.sh --by-workload capacity      # or bytes-per-sample
#
# The `test-utils` feature exposes src/tests (data generators, chunk helpers) to
# the binary; `enable-system-alloc` is required by every target that links the
# crate's global allocator outside a running Valkey.

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT"

FEATURES="enable-system-alloc,test-utils"
CSV="target/bench-reports/compression.csv"
MD="target/bench-reports/compression.md"
BASELINE="benches/baselines/compression_baseline.csv"
CHECK=false
SAVE_BASELINE=false
BY_WORKLOAD=""

usage() {
    sed -n '3,16p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
    case "$1" in
        --check)
            CHECK=true
            ;;
        --save-baseline)
            SAVE_BASELINE=true
            ;;
        --baseline)
            if [ $# -lt 2 ]; then
                echo "error: --baseline requires a path" >&2
                exit 1
            fi
            BASELINE="$2"
            shift
            ;;
        --by-workload)
            # Optional metric argument; the binary defaults to `ratio`.
            if [ $# -ge 2 ] && [ "${2#--}" = "$2" ]; then
                BY_WORKLOAD="$2"
                shift
            else
                BY_WORKLOAD="ratio"
            fi
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown option '$1'" >&2
            usage >&2
            exit 1
            ;;
    esac
    shift
done

if [ "$CHECK" = true ] && [ "$SAVE_BASELINE" = true ]; then
    echo "error: --check and --save-baseline are mutually exclusive" >&2
    exit 1
fi

REPORT_ARGS=(--baseline "$BASELINE")
if [ "$CHECK" = true ]; then
    if [ ! -f "$BASELINE" ]; then
        echo "error: --check needs a baseline; '$BASELINE' does not exist." >&2
        echo "       Generate one first with: $0 --save-baseline" >&2
        exit 2
    fi
    REPORT_ARGS+=(--check)
fi
if [ -n "$BY_WORKLOAD" ]; then
    REPORT_ARGS+=(--by-workload "$BY_WORKLOAD")
fi

echo "Running compression report (release, features: $FEATURES)..."
cargo run --release --features "$FEATURES" --bin compression_report -- "${REPORT_ARGS[@]}"

if [ "$SAVE_BASELINE" = true ]; then
    mkdir -p "$(dirname "$BASELINE")"
    cp "$CSV" "$BASELINE"
    echo "Baseline saved to $BASELINE"
fi

echo "Report: $REPO_ROOT/$CSV"
echo "        $REPO_ROOT/$MD"
if [ -n "$BY_WORKLOAD" ]; then
    echo "        $REPO_ROOT/target/bench-reports/compression_by_workload_$BY_WORKLOAD.csv"
    echo "        $REPO_ROOT/target/bench-reports/compression_by_workload_$BY_WORKLOAD.md"
fi
