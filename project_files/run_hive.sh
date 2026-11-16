#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "Uzycie: run_hive.sh input_dir3 input_dir4 output_dir6" >&2
  exit 1
fi

INPUT_DIR3="$1"
INPUT_DIR4="$2"
OUTPUT_DIR6="$3"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -x /opt/hive/bin/beeline ]]; then
  BEELINE=/opt/hive/bin/beeline
else
  BEELINE=beeline
fi

find_jar() {
  local pattern="$1"
  local dir
  for dir in /opt/hive/lib /usr/lib/hive/lib; do
    if compgen -G "${dir}/${pattern}" > /dev/null; then
      compgen -G "${dir}/${pattern}" | head -n 1
      return 0
    fi
  done
  return 1
}

OPENCSV_JAR="$(find_jar 'opencsv-*.jar')"
HCATALOG_JAR="$(find_jar 'hive-hcatalog-core-*.jar')"
if [[ -z "${OPENCSV_JAR}" || -z "${HCATALOG_JAR}" ]]; then
  echo "Nie znaleziono opencsv albo hive-hcatalog-core" >&2
  exit 1
fi

"${BEELINE}" -n "$(id -un)" \
  -u "jdbc:hive2://localhost:10000/default" \
  --hivevar input_dir3="${INPUT_DIR3}" \
  --hivevar input_dir4="${INPUT_DIR4}" \
  --hivevar output_dir6="${OUTPUT_DIR6}" \
  --hivevar opencsv_jar="${OPENCSV_JAR}" \
  --hivevar hcatalog_jar="${HCATALOG_JAR}" \
  -f "${SCRIPT_DIR}/hive.hql"
