#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Uzycie: run_mr.sh input_dir1 output_dir3" >&2
  exit 1
fi

INPUT_DIR1="$1"
OUTPUT_DIR3="$2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

hadoop fs -rm -r -f "${OUTPUT_DIR3}"

mapred streaming \
  -D stream.map.output.field.separator=$'\t' \
  -D stream.num.map.output.key.fields=2 \
  -D stream.num.reduce.output.key.fields=2 \
  -files "${SCRIPT_DIR}/mapper.py,${SCRIPT_DIR}/combiner.py,${SCRIPT_DIR}/reducer.py" \
  -mapper "python mapper.py" \
  -combiner "python combiner.py" \
  -reducer "python reducer.py" \
  -input "${INPUT_DIR1}" \
  -output "${OUTPUT_DIR3}"
