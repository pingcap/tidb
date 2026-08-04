#!/usr/bin/env bash
#
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
#
# Bash is required: the chunk-iteration loop below uses `read -r -d ''`,
# which is a bash extension that POSIX /bin/sh (e.g. dash) does not implement.
#
# Larger-scale regression test for string-based (composite) primary-key
# chunking. It proves end-to-end correctness by:
#
#   1. Loading N rows into MySQL with composite string PKs that exercise
#      cursor-based WHERE boundaries (shared prefix, unicode, escapes,
#      3-column keys).
#   2. Dumping via dumpling with --rows small enough to produce many
#      chunks (streaming path in concurrentDumpStringFields).
#   3. Asserting the chunk count matches what the new streaming loop
#      should produce.
#   4. Re-importing the dump into TiDB with tidb-lightning.
#   5. Using sync_diff_inspector to byte-diff MySQL (source) vs TiDB
#      (target). Any row loss or duplication at chunk boundaries will
#      surface here as a checksum mismatch.
#
# This complements dumpling/tests/composite_string_key (small fixture
# byte-diff). Fixture diff catches format regressions; this test catches
# data-loss/ordering regressions that fixture diff can't detect at scale.

set -eu
cur=$(cd "$(dirname "$0")"; pwd)

DB_NAME="composite_string_key_large"

# Ensure UTF8MB4 so unicode PK values round-trip.
export DUMPLING_TEST_PORT=3306
run_sql "set global character_set_server=utf8mb4"
run_sql "set global collation_server=utf8mb4_bin"

# Drop and recreate on both sides.
run_sql "drop database if exists \`$DB_NAME\`;"
export DUMPLING_TEST_PORT=4000
run_sql "drop database if exists \`$DB_NAME\`;"

export DUMPLING_TEST_PORT=3306
run_sql "create database \`$DB_NAME\` DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
export DUMPLING_TEST_DATABASE="$DB_NAME"

# --- Table A: 2-column composite PK with heavy shared prefix ---------------
# Exercises the cursor WHERE clause: (tenant = v1 AND user > v2) OR tenant > v1
run_sql "CREATE TABLE \`$DB_NAME\`.\`events\` (
  tenant VARCHAR(32) NOT NULL,
  event_id VARCHAR(32) NOT NULL,
  payload VARCHAR(128) NOT NULL,
  PRIMARY KEY (tenant, event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

# 10 tenants x 50 events = 500 rows. All tenants share a long prefix so the
# cursor boundary often lands inside a run of identical tenant values — the
# exact regression surface that plain single-column boundaries miss. Generate
# via python into a tempfile, then load with the harness's run_sql_file so we
# pick up $DUMPLING_TEST_USER / port / future auth changes automatically.
events_sql=$(mktemp)
python3 - >"$events_sql" <<'PY'
rows = []
for t in range(10):
    tenant = f"tenant-prefix-shared-{t:03d}"
    for e in range(50):
        eid = f"evt-{e:04d}"
        # Exercise SQL escaping: single/double quotes and a backslash.
        # Double single-quotes and double backslashes so the literal is valid
        # under MySQL's default sql_mode (which treats backslash as escape).
        raw = f"payload for tenant {t} event {e} with 'single' and \"double\" quotes and a backslash \\."
        payload = raw.replace("\\", "\\\\").replace("'", "''")
        rows.append(f"('{tenant}','{eid}','{payload}')")
print("INSERT INTO events (tenant,event_id,payload) VALUES")
print(",\n".join(rows) + ";")
PY
run_sql_file "$events_sql"
rm -f "$events_sql"

# --- Table B: 3-column composite PK with unicode and NULL-able body ---------
run_sql "CREATE TABLE \`$DB_NAME\`.\`translations\` (
  locale VARCHAR(8) NOT NULL,
  namespace VARCHAR(32) NOT NULL,
  msg_key VARCHAR(64) NOT NULL,
  body TEXT NULL,
  PRIMARY KEY (locale, namespace, msg_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

translations_sql=$(mktemp)
python3 - >"$translations_sql" <<'PY'
locales = ["en", "ja", "zh", "de", "fr"]
namespaces = ["auth", "billing", "profile"]
bodies = {
    "en": "Hello, world!",
    "ja": "こんにちは、世界！",
    "zh": "你好，世界！",
    "de": "Hallo, Welt!",
    "fr": "Bonjour, le monde !",
}
rows = []
for loc in locales:
    for ns in namespaces:
        for k in range(20):
            msg_key = f"key_{k:03d}"
            body = bodies[loc].replace("'", "''")
            rows.append(f"('{loc}','{ns}','{msg_key}','{body}')")
print("INSERT INTO translations (locale,namespace,msg_key,body) VALUES")
print(",\n".join(rows) + ";")
PY
run_sql_file "$translations_sql"
rm -f "$translations_sql"

# Analyze so EXPLAIN-based estimation is accurate.
run_sql "analyze table \`$DB_NAME\`.\`events\`;"
run_sql "analyze table \`$DB_NAME\`.\`translations\`;"

# --- Dump ------------------------------------------------------------------
# --rows 50 on a 500-row events table => ~10 chunks; 300-row translations => ~6.
run_dumpling --rows 50 --loglevel info

# --- Assert chunk counts ---------------------------------------------------
events_chunks=$(find "$DUMPLING_OUTPUT_DIR" -maxdepth 1 -name "$DB_NAME.events.[0-9]*.sql" | wc -l | tr -d ' ')
translations_chunks=$(find "$DUMPLING_OUTPUT_DIR" -maxdepth 1 -name "$DB_NAME.translations.[0-9]*.sql" | wc -l | tr -d ' ')

echo "events chunks: $events_chunks"
echo "translations chunks: $translations_chunks"

# A correct streaming chunker must produce > 1 chunk here (proves parallelism
# was engaged). Exact count depends on estimation rounding; require >= 2.
if [ "$events_chunks" -lt 2 ]; then
  echo "FAIL: events produced $events_chunks chunks, expected >= 2 (string chunking disabled?)"
  exit 1
fi
if [ "$translations_chunks" -lt 2 ]; then
  echo "FAIL: translations produced $translations_chunks chunks, expected >= 2"
  exit 1
fi

# Each data chunk must be a self-contained INSERT (per lance6716 review).
# The [0-9]* pattern excludes the -schema.sql meta files (which have no
# INSERT statement). NUL-delimited find/while loop is robust to any path.
find "$DUMPLING_OUTPUT_DIR" -maxdepth 1 -type f \
    \( -name "$DB_NAME.events.[0-9]*.sql" -o -name "$DB_NAME.translations.[0-9]*.sql" \) \
    -print0 | while IFS= read -r -d '' chunk; do
  if ! grep -q "^INSERT INTO" "$chunk"; then
    echo "FAIL: $chunk missing INSERT prefix"
    exit 1
  fi
  # The last non-empty line must end with ';' (a trailing '\n' after it is
  # fine — tail -n 1 strips a single trailing newline). Looser checks such
  # as `tail -c 3 | grep ';'` also match things like ');' followed by more
  # bytes, which isn't the contract we want to assert.
  last_line=$(tail -n 1 "$chunk")
  case "$last_line" in
    *";") ;;
    *) echo "FAIL: $chunk last line does not end with ';' (got: $last_line)"; exit 1 ;;
  esac
done

# --- Round-trip: lightning imports chunks into TiDB ------------------------
run_lightning "$cur/conf/lightning.toml"

# --- sync_diff: MySQL source vs TiDB target must match exactly -------------
check_sync_diff "$cur/conf/diff_config.toml"

echo "composite_string_key_large: OK"
