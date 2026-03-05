#!/usr/bin/env bash
# benchmark-global-stats.sh — Multi-table deterministic benchmark comparing
# tidb_enable_sample_based_global_stats ON vs OFF.
#
# Benchmarks multiple partitioned/non-partitioned table pairs. Each run gets
# a clean TiKV state (playground restart + data wipe) to eliminate LSM-tree
# compaction cross-contamination. Run order uses counterbalanced ABBAAB.
#
# Prerequisites:
#   - Full BR backup with stats (so stats_* tables and their regions are pre-populated):
#     tiup br:nightly backup full --ignore-stats=false -s "local:///path/to/backup" --pd ...
#   - analyze-profile binary built:
#     cd ~/repos/tidb-dev-hacks/analyze-profile && go build -o analyze-profile .
#
# Usage:
#   BACKUP_PATH=./my-backup ./benchmark-global-stats.sh
#   REPS=1 BACKUP_PATH=./my-backup ./benchmark-global-stats.sh    # dry-run (2 runs)
#   REPS=5 BACKUP_PATH=./my-backup ./benchmark-global-stats.sh    # high-confidence

set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Configuration (override via environment)
# ──────────────────────────────────────────────────────────────────────────────

REPS="${REPS:-3}"                                       # Repetitions per mode
PLAYGROUND_TAG="${PLAYGROUND_TAG:-bench-global-stats}"
BACKUP_PATH="${BACKUP_PATH:-}"                          # BR backup directory (required)
DB="${DB:-analyze_profile}"

# Table pairs: PARTITIONED_TABLE:NON_PARTITIONED_TABLE:SINGLE_PARTITION
# Modify this array to benchmark specific pairs, or set TABLE_PAIRS in env.
if [[ -z "${TABLE_PAIRS+x}" ]]; then
    TABLE_PAIRS=(
        "tp8000CM50I3R30M:tCM50I3R30M:p5"
        "tSSp256CM50I3R10M:tSSCM50I3R10M:p5"
        "tp256CM20MS8kI3R10M:tCM20MS8kI3R10M:p5"
        "tp8000CI16R30M:tCI16R30M:p5"
    )
fi

# When to analyze non-partitioned (ground truth) tables: "every", "first", "skip"
ANALYZE_GROUND_TRUTH="${ANALYZE_GROUND_TRUTH:-first}"

AP="${AP:-$HOME/repos/tidb-dev-hacks/analyze-profile/analyze-profile}"
OUTPUT_BASE="${OUTPUT_BASE:-./benchmark-output}"
PD_ADDR="${PD_ADDR:-127.0.0.1:2379}"
TIDB_HOST="${TIDB_HOST:-127.0.0.1}"
TIDB_PORT="${TIDB_PORT:-4000}"
PD_TIMEOUT="${PD_TIMEOUT:-120}"                          # Max seconds to wait for PD/TiDB
TIKV_STATUS="${TIKV_STATUS:-127.0.0.1:20180}"            # TiKV status port (metrics)
COOLDOWN="${COOLDOWN:-15}"                               # Seconds to wait between runs

# ANALYZE concurrency tuning (i7-13700: 16c/24t, 64GB)
# These are set as GLOBAL variables before each run.
ANALYZE_PARTITION_CONCURRENCY="${ANALYZE_PARTITION_CONCURRENCY:-16}"
BUILD_STATS_CONCURRENCY="${BUILD_STATS_CONCURRENCY:-8}"
BUILD_SAMPLING_STATS_CONCURRENCY="${BUILD_SAMPLING_STATS_CONCURRENCY:-8}"
MERGE_PARTITION_STATS_CONCURRENCY="${MERGE_PARTITION_STATS_CONCURRENCY:-16}"
ANALYZE_DISTSQL_SCAN_CONCURRENCY="${ANALYZE_DISTSQL_SCAN_CONCURRENCY:-15}"

# ──────────────────────────────────────────────────────────────────────────────
# Derived / internal
# ──────────────────────────────────────────────────────────────────────────────

TIUP_DATA_DIR="$HOME/.tiup/data/${PLAYGROUND_TAG}"
MYSQL_CMD="mysql -h ${TIDB_HOST} -P ${TIDB_PORT} -u root --batch --skip-column-names"
SCRIPT_START=""
RUN_ORDER=()
TIDB_LOG=""
RUN_INDEX=0

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { echo "FATAL: $*" >&2; exit 1; }

elapsed_since() {
    local start_epoch="$1"
    local now_epoch
    now_epoch=$(date +%s)
    local diff=$(( now_epoch - start_epoch ))
    printf '%02d:%02d:%02d' $(( diff/3600 )) $(( (diff%3600)/60 )) $(( diff%60 ))
}

# Generate counterbalanced ABBAAB run order.
# For n reps each: ON, OFF, OFF, ON, ON, OFF, OFF, ON, ...
generate_run_order() {
    local n="$1"
    RUN_ORDER=()
    local pair_index=0
    local on_count=0
    local off_count=0
    while (( on_count < n || off_count < n )); do
        if (( pair_index % 2 == 0 )); then
            # AB pair
            if (( on_count < n )); then
                RUN_ORDER+=("ON")
                (( on_count++ ))
            fi
            if (( off_count < n )); then
                RUN_ORDER+=("OFF")
                (( off_count++ ))
            fi
        else
            # BA pair
            if (( off_count < n )); then
                RUN_ORDER+=("OFF")
                (( off_count++ ))
            fi
            if (( on_count < n )); then
                RUN_ORDER+=("ON")
                (( on_count++ ))
            fi
        fi
        (( pair_index++ ))
    done
}

# Poll PD health endpoint until healthy or timeout.
wait_for_pd_healthy() {
    log "Waiting for PD to become healthy (timeout ${PD_TIMEOUT}s)..."
    local deadline=$(( $(date +%s) + PD_TIMEOUT ))
    while true; do
        if curl -sf "http://${PD_ADDR}/pd/api/v1/health" >/dev/null 2>&1; then
            log "PD is healthy."
            return 0
        fi
        if (( $(date +%s) >= deadline )); then
            die "PD did not become healthy within ${PD_TIMEOUT}s"
        fi
        sleep 2
    done
}

# Wait for TiDB to accept connections.
wait_for_tidb() {
    log "Waiting for TiDB to accept connections..."
    local deadline=$(( $(date +%s) + PD_TIMEOUT ))
    while true; do
        if ${MYSQL_CMD} -e "SELECT 1" >/dev/null 2>&1; then
            log "TiDB is ready."
            return 0
        fi
        if (( $(date +%s) >= deadline )); then
            die "TiDB did not become ready within ${PD_TIMEOUT}s"
        fi
        sleep 2
    done
}

# Force TiKV compaction via tikv-ctl, then verify pending bytes are near zero.
# compact-cluster is synchronous — it blocks until compaction finishes.
force_compact() {
    log "Forcing TiKV compaction (all CFs)..."
    local start_epoch
    start_epoch=$(date +%s)

    # Compact all three column families
    for cf in default write lock; do
        log "  Compacting CF=${cf}..."
        tiup ctl:nightly tikv --pd "${PD_ADDR}" compact-cluster \
            -c "${cf}" --bottommost force \
            2>&1 | tail -3 || log "  WARNING: compact-cluster CF=${cf} returned non-zero"
    done

    # Verify pending bytes are low
    local pending
    pending=$(curl -sf "http://${TIKV_STATUS}/metrics" 2>/dev/null \
        | grep 'tikv_engine_pending_compaction_bytes{' \
        | awk -F' ' '{sum += $2} END {printf "%.0f", sum}') || pending="unknown"
    log "Compaction done in $(elapsed_since "$start_epoch"). Pending bytes: ${pending}"
}

# Find TiDB log path for the current playground instance.
detect_tidb_log_path() {
    TIDB_LOG=""
    local log_dir="${TIUP_DATA_DIR}/tidb-0"
    if [[ -d "$log_dir" ]]; then
        TIDB_LOG="${log_dir}/tidb.log"
    else
        # Fallback: search for any tidb log dir
        local found
        found=$(find "${TIUP_DATA_DIR}" -maxdepth 2 -name 'tidb.log' -print -quit 2>/dev/null || true)
        if [[ -n "$found" ]]; then
            TIDB_LOG="$found"
        fi
    fi
    if [[ -n "$TIDB_LOG" ]]; then
        log "TiDB log: ${TIDB_LOG}"
    else
        log "WARNING: Could not detect TiDB log path"
    fi
}

# Kill playground and wipe its data directory.
clean_playground() {
    log "Cleaning playground '${PLAYGROUND_TAG}'..."
    tiup clean "${PLAYGROUND_TAG}" --all 2>/dev/null || true
    # Also kill any lingering tiup-playground process for this tag
    pkill -f "playground.*--tag.*${PLAYGROUND_TAG}" 2>/dev/null || true
    sleep 2
    if [[ -d "${TIUP_DATA_DIR}" ]]; then
        rm -rf "${TIUP_DATA_DIR}"
        log "Wiped ${TIUP_DATA_DIR}"
    fi
}

# Write TiKV config for playground. Tuned for i7-13700 (16c/24t, 64GB).
write_tikv_config() {
    local path="$1"
    cat > "$path" << 'TOML'
[readpool.coprocessor]
# Thread pool serving ANALYZE scans. Default 8 is too low for high-concurrency ANALYZE.
max-concurrency = 16

[server]
grpc-concurrency = 8

[rocksdb]
# Background compaction/flush threads. Helps after BR restore and during ANALYZE writes.
max-background-jobs = 8

[storage]
scheduler-worker-pool-size = 8
TOML
    log "Wrote TiKV config: ${path}"
}

# Start playground in background and wait for it to be ready.
start_playground() {
    local log_file="$1"

    # Generate TiKV config
    local tikv_config="${OUTPUT_BASE}/tikv.toml"
    write_tikv_config "${tikv_config}"

    log "Starting playground (tag=${PLAYGROUND_TAG}, 1 TiDB + 1 TiKV + 1 PD)..."
    tiup playground nightly \
        --tag "${PLAYGROUND_TAG}" \
        --db 1 --kv 1 --pd 1 \
        --without tiflash \
        --kv.config "${tikv_config}" \
        >"${log_file}" 2>&1 &
    PLAYGROUND_PID=$!
    log "Playground PID: ${PLAYGROUND_PID}"

    wait_for_pd_healthy
    wait_for_tidb
}

# Whether to analyze ground truth (non-partitioned) tables in the current run.
should_analyze_ground_truth() {
    case "${ANALYZE_GROUND_TRUTH}" in
        every)  return 0 ;;
        first)  [[ "${RUN_INDEX}" -eq 1 ]] ;;
        skip)   return 1 ;;
        *)      return 0 ;;
    esac
}

# Collect stats_meta row counts for all tables in the database.
collect_accuracy() {
    local outfile="$1"
    log "Collecting stats_meta for all tables -> ${outfile}"
    ${MYSQL_CMD} -e "
        SELECT 'global' AS scope, t.TABLE_NAME, sm.count, sm.modify_count
        FROM mysql.stats_meta sm
        JOIN information_schema.tables t ON sm.table_id = t.TIDB_TABLE_ID
        WHERE t.TABLE_SCHEMA = '${DB}'
        UNION ALL
        SELECT p.PARTITION_NAME AS scope, p.TABLE_NAME, sm.count, sm.modify_count
        FROM mysql.stats_meta sm
        JOIN information_schema.partitions p ON sm.table_id = p.TIDB_PARTITION_ID
        WHERE p.TABLE_SCHEMA = '${DB}'
        ORDER BY 2, 1
    " > "${outfile}" 2>&1 || log "WARNING: accuracy query failed (non-fatal)"
}

# Find profile_result.json inside an analyze-profile output directory.
# The tool creates a run_YYYYMMDD_HHMMSS subdirectory.
find_result_json() {
    find "$1" -name 'profile_result.json' -print -quit 2>/dev/null || true
}

# Extract a field from profile_result.json using dot-separated path.
extract_json() {
    local file="$1"
    local field="$2"
    python3 -c "
import json
with open('$file') as f:
    d = json.load(f)
v = d
for k in '$field'.split('.'):
    if isinstance(v, dict):
        v = v.get(k, 'N/A')
    else:
        v = 'N/A'
        break
print(v)
" 2>/dev/null || echo "N/A"
}

# Print final summary comparing all runs.
print_summary() {
    local summary_file="${OUTPUT_BASE}/summary.txt"
    log "Generating summary -> ${summary_file}"

    {
        echo "========================================================================"
        echo "Benchmark Summary: tidb_enable_sample_based_global_stats ON vs OFF"
        echo "Date: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "Reps per mode: ${REPS}"
        echo "Run order: ${RUN_ORDER[*]}"
        echo "Ground truth: ${ANALYZE_GROUND_TRUTH}"
        echo "========================================================================"
        echo ""

        for pair in "${TABLE_PAIRS[@]}"; do
            IFS=':' read -r ptable nptable partition <<< "$pair"

            # Full ANALYZE on partitioned table
            echo "--- ${ptable} (full ANALYZE) ---"
            printf "%-4s  %-4s  %-20s  %-14s\n" "Run" "Mode" "Duration" "StatsRowCount"
            printf "%-4s  %-4s  %-20s  %-14s\n" "----" "----" "--------------------" "--------------"

            for (( i = 1; i <= ${#RUN_ORDER[@]}; i++ )); do
                local mode="${RUN_ORDER[$((i-1))]}"
                local result_json
                result_json=$(find_result_json "${OUTPUT_BASE}/run_${i}_${mode}/${ptable}" 2>/dev/null)
                if [[ -n "$result_json" && -f "$result_json" ]]; then
                    local dur rc
                    dur=$(extract_json "$result_json" "analyze_duration")
                    rc=$(extract_json "$result_json" "accuracy.row_count.stats_count")
                    printf "%-4s  %-4s  %-20s  %-14s\n" "$i" "$mode" "$dur" "$rc"
                else
                    printf "%-4s  %-4s  %-20s  %-14s\n" "$i" "$mode" "MISSING" "N/A"
                fi
            done
            echo ""

            # Single-partition ANALYZE
            echo "--- ${ptable} (single-partition ${partition}) ---"
            printf "%-4s  %-4s  %-20s  %-14s\n" "Run" "Mode" "Duration" "StatsRowCount"
            printf "%-4s  %-4s  %-20s  %-14s\n" "----" "----" "--------------------" "--------------"

            for (( i = 1; i <= ${#RUN_ORDER[@]}; i++ )); do
                local mode="${RUN_ORDER[$((i-1))]}"
                local result_json
                result_json=$(find_result_json "${OUTPUT_BASE}/run_${i}_${mode}/${ptable}_${partition}" 2>/dev/null)
                if [[ -n "$result_json" && -f "$result_json" ]]; then
                    local dur rc
                    dur=$(extract_json "$result_json" "analyze_duration")
                    rc=$(extract_json "$result_json" "accuracy.row_count.stats_count")
                    printf "%-4s  %-4s  %-20s  %-14s\n" "$i" "$mode" "$dur" "$rc"
                else
                    printf "%-4s  %-4s  %-20s  %-14s\n" "$i" "$mode" "MISSING" "N/A"
                fi
            done
            echo ""

            # Ground truth
            if [[ "${ANALYZE_GROUND_TRUTH}" != "skip" ]]; then
                local gt_json
                gt_json=$(find_result_json "${OUTPUT_BASE}/run_1_${RUN_ORDER[0]}/${nptable}" 2>/dev/null)
                if [[ -n "$gt_json" && -f "$gt_json" ]]; then
                    local dur rc ncols nidx
                    dur=$(extract_json "$gt_json" "analyze_duration")
                    rc=$(extract_json "$gt_json" "accuracy.row_count.stats_count")
                    ncols=$(python3 -c "import json; d=json.load(open('$gt_json')); print(len(d.get('accuracy',{}).get('column_stats',[])))" 2>/dev/null || echo "?")
                    nidx=$(python3 -c "import json; d=json.load(open('$gt_json')); print(len(d.get('accuracy',{}).get('index_stats',[])))" 2>/dev/null || echo "?")
                    echo "  Ground truth (${nptable}): duration=${dur}  rows=${rc}  cols=${ncols}  indexes=${nidx}"
                else
                    echo "  Ground truth (${nptable}): no data"
                fi
                echo ""
            fi
        done

        # Duration averages per table per mode
        echo "--- Duration Averages (full ANALYZE, seconds) ---"
        OUTPUT_BASE="${OUTPUT_BASE}" python3 << 'PYEOF'
import json, glob, re, os, sys

def parse_go_duration(s):
    """Parse Go duration like '1m30.5s' to seconds."""
    total = 0
    for m in re.finditer(r'(\d+(?:\.\d+)?)(h|m|s|ms)', s):
        val, unit = float(m.group(1)), m.group(2)
        if unit == 'h': total += val * 3600
        elif unit == 'm': total += val * 60
        elif unit == 's': total += val
        elif unit == 'ms': total += val / 1000
    return total if total > 0 else None

base = os.environ['OUTPUT_BASE']
# Collect durations grouped by (table_dir_name, mode)
data = {}  # (table, mode) -> [seconds]
for f in glob.glob(f"{base}/run_*_*/*/run_*/profile_result.json"):
    parts = f.split('/')
    # .../run_N_MODE/TABLE/run_TIMESTAMP/profile_result.json
    run_dir = [p for p in parts if p.startswith('run_') and ('_ON' in p or '_OFF' in p)]
    table_dir = [p for p in parts if not p.startswith('run_') and p != 'profile_result.json' and p != base.rstrip('/').split('/')[-1]]
    if not run_dir or not table_dir:
        continue
    mode = 'ON' if '_ON' in run_dir[0] else 'OFF'
    table = table_dir[0] if table_dir else '?'
    # Skip single-partition and ground truth dirs (they have _ suffix like _p5)
    if '_p' in table and table.split('_p')[-1].isdigit():
        continue
    try:
        d = json.load(open(f))
        secs = parse_go_duration(d.get('analyze_duration', ''))
        if secs:
            data.setdefault((table, mode), []).append(secs)
    except:
        pass

if data:
    tables = sorted(set(t for t, m in data))
    for table in tables:
        for mode in ['ON', 'OFF']:
            vals = data.get((table, mode), [])
            if vals:
                print(f"  {table:30s} {mode}: n={len(vals)}  mean={sum(vals)/len(vals):.1f}s  min={min(vals):.1f}s  max={max(vals):.1f}s")
            else:
                print(f"  {table:30s} {mode}: no data")
else:
    print("  No data found")
PYEOF
        echo ""

        echo "Total elapsed: $(elapsed_since "$SCRIPT_START")"

    } | tee "${summary_file}"
}

# ──────────────────────────────────────────────────────────────────────────────
# Preflight checks
# ──────────────────────────────────────────────────────────────────────────────

preflight() {
    [[ -n "${BACKUP_PATH}" ]] || die "BACKUP_PATH is required (set via env)"
    [[ -d "${BACKUP_PATH}" ]] || die "BACKUP_PATH does not exist: ${BACKUP_PATH}"
    command -v tiup >/dev/null || die "tiup not found in PATH"
    command -v mysql >/dev/null || die "mysql client not found in PATH"
    command -v curl >/dev/null || die "curl not found in PATH"
    command -v python3 >/dev/null || die "python3 not found in PATH"
    [[ -x "${AP}" ]] || die "analyze-profile not found or not executable: ${AP}"
    [[ "${REPS}" -ge 1 ]] || die "REPS must be >= 1"
    for pair in "${TABLE_PAIRS[@]}"; do
        IFS=':' read -r pt npt part <<< "$pair"
        [[ -n "$pt" && -n "$npt" && -n "$part" ]] || \
            die "Invalid TABLE_PAIRS entry: ${pair} (expected PARTITIONED:NON_PARTITIONED:PARTITION)"
    done
}

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

main() {
    preflight
    SCRIPT_START=$(date +%s)

    # Create output directory
    mkdir -p "${OUTPUT_BASE}"

    # Generate run order
    generate_run_order "${REPS}"
    local total_runs=${#RUN_ORDER[@]}

    log "============================================================"
    log "Benchmark: sample_based_global_stats ON vs OFF"
    log "Reps per mode: ${REPS}  |  Total runs: ${total_runs}"
    log "Run order: ${RUN_ORDER[*]}"
    log "Backup: ${BACKUP_PATH}"
    log "Output: ${OUTPUT_BASE}"
    log "Ground truth: ${ANALYZE_GROUND_TRUTH}"
    log "Table pairs:"
    for pair in "${TABLE_PAIRS[@]}"; do
        IFS=':' read -r pt npt part <<< "$pair"
        log "  ${pt} <-> ${npt} (single-partition: ${part})"
    done
    log "============================================================"

    for (( RUN_INDEX = 1; RUN_INDEX <= total_runs; RUN_INDEX++ )); do
        local mode="${RUN_ORDER[$((RUN_INDEX-1))]}"
        local run_label="run_${RUN_INDEX}_${mode}"
        local run_dir="${OUTPUT_BASE}/${run_label}"
        local run_start
        run_start=$(date +%s)

        log ""
        log "============================================================"
        log "=== Run ${RUN_INDEX}/${total_runs}: mode=${mode} (elapsed: $(elapsed_since "$SCRIPT_START"))"
        log "============================================================"

        # Create output directories
        mkdir -p "${run_dir}/logs"

        # ── 1. Clean start ────────────────────────────────────────────
        clean_playground
        start_playground "${run_dir}/logs/playground.log"

        # ── 2. Configure ──────────────────────────────────────────────
        log "Configuring TiDB..."
        ${MYSQL_CMD} -e "SET GLOBAL tidb_enable_auto_analyze = OFF"
        ${MYSQL_CMD} -e "SET GLOBAL tidb_gc_run_interval = '999h'"

        # ANALYZE concurrency tuning
        ${MYSQL_CMD} -e "SET GLOBAL tidb_analyze_partition_concurrency = ${ANALYZE_PARTITION_CONCURRENCY}"
        ${MYSQL_CMD} -e "SET GLOBAL tidb_build_stats_concurrency = ${BUILD_STATS_CONCURRENCY}"
        ${MYSQL_CMD} -e "SET GLOBAL tidb_build_sampling_stats_concurrency = ${BUILD_SAMPLING_STATS_CONCURRENCY}"
        ${MYSQL_CMD} -e "SET GLOBAL tidb_merge_partition_stats_concurrency = ${MERGE_PARTITION_STATS_CONCURRENCY}"
        ${MYSQL_CMD} -e "SET GLOBAL tidb_analyze_distsql_scan_concurrency = ${ANALYZE_DISTSQL_SCAN_CONCURRENCY}"
        log "Auto-analyze: OFF, GC: 999h, partition_concurrency=${ANALYZE_PARTITION_CONCURRENCY}, build_stats=${BUILD_STATS_CONCURRENCY}, sampling=${BUILD_SAMPLING_STATS_CONCURRENCY}, merge=${MERGE_PARTITION_STATS_CONCURRENCY}, distsql_scan=${ANALYZE_DISTSQL_SCAN_CONCURRENCY}"

        # ── 3. Restore all tables ─────────────────────────────────────
        log "Restoring full backup (including stats)..."
        local abs_backup_path
        abs_backup_path=$(cd "${BACKUP_PATH}" && pwd)
        tiup br:nightly restore full \
            -s "local://${abs_backup_path}" \
            --pd "${PD_ADDR}" \
            --log-file "${run_dir}/logs/br_restore.log" \
            2>&1 | tail -5
        log "Restore complete. Forcing compaction..."
        force_compact

        detect_tidb_log_path
        local tidb_log_flag=""
        [[ -n "${TIDB_LOG}" ]] && tidb_log_flag="-tidb-log ${TIDB_LOG}"

        # ── 4. Analyze each table pair ────────────────────────────────
        for pair in "${TABLE_PAIRS[@]}"; do
            IFS=':' read -r ptable nptable partition <<< "$pair"

            # 4a. Full ANALYZE on partitioned table
            log ""
            log "--- ${ptable}: full ANALYZE (${mode}) ---"
            mkdir -p "${run_dir}/${ptable}"
            if ! "${AP}" profile \
                -db "${DB}" -table "${ptable}" \
                -analyze-columns all \
                -set-variable "tidb_enable_sample_based_global_stats=${mode}" \
                -check-accuracy \
                -output-dir "${run_dir}/${ptable}" \
                ${tidb_log_flag} \
                2>&1 | tee "${run_dir}/${ptable}/analyze-profile.log"; then
                log "WARNING: analyze-profile failed for ${ptable} (full)"
            fi

            force_compact

            # 4b. Single-partition ANALYZE
            log ""
            log "--- ${ptable}: single-partition ${partition} (${mode}) ---"
            mkdir -p "${run_dir}/${ptable}_${partition}"
            if ! "${AP}" profile \
                -db "${DB}" -table "${ptable}" \
                -analyze-columns all \
                -set-variable "tidb_enable_sample_based_global_stats=${mode}" \
                -partition "${partition}" \
                -check-accuracy \
                -output-dir "${run_dir}/${ptable}_${partition}" \
                ${tidb_log_flag} \
                2>&1 | tee "${run_dir}/${ptable}_${partition}/analyze-profile.log"; then
                log "WARNING: analyze-profile failed for ${ptable} single-partition"
            fi

            # 4c. Ground truth: non-partitioned table
            if should_analyze_ground_truth; then
                log ""
                log "--- ${nptable}: ground truth ANALYZE ---"
                mkdir -p "${run_dir}/${nptable}"
                if ! "${AP}" profile \
                    -db "${DB}" -table "${nptable}" \
                    -analyze-columns all \
                    -check-accuracy \
                    -output-dir "${run_dir}/${nptable}" \
                    ${tidb_log_flag} \
                    2>&1 | tee "${run_dir}/${nptable}/analyze-profile.log"; then
                    log "WARNING: analyze-profile failed for ${nptable}"
                fi
            fi
        done

        # ── 5. Collect accuracy ───────────────────────────────────────
        collect_accuracy "${run_dir}/accuracy.tsv"

        # ── 6. Collect TiDB logs ──────────────────────────────────────
        log "Collecting TiDB logs..."
        local tidb_log_dir
        tidb_log_dir=$(dirname "${TIDB_LOG:-${TIUP_DATA_DIR}/tidb-0/tidb.log}")
        cp "${tidb_log_dir}"/tidb*.log "${run_dir}/logs/" 2>/dev/null || \
            log "WARNING: Could not copy TiDB logs"

        # ── 7. Shutdown ───────────────────────────────────────────────
        clean_playground

        local run_elapsed
        run_elapsed=$(elapsed_since "$run_start")
        log "=== Run ${RUN_INDEX}/${total_runs} (${mode}) complete in ${run_elapsed} ==="

        # Cooldown between runs
        if (( RUN_INDEX < total_runs && COOLDOWN > 0 )); then
            log "Cooldown: ${COOLDOWN}s before next run..."
            sleep "${COOLDOWN}"
        fi
    done

    # ── 8. Summary ────────────────────────────────────────────────────
    log ""
    log "All runs complete. Total elapsed: $(elapsed_since "$SCRIPT_START")"
    log ""
    print_summary
}

main "$@"
