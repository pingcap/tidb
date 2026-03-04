#!/usr/bin/env bash
# benchmark-global-stats.sh — Deterministic benchmark comparing
# tidb_enable_sample_based_global_stats ON vs OFF.
#
# Each run gets a completely clean TiKV state (playground restart + data wipe)
# to eliminate LSM-tree compaction cross-contamination. Run order uses
# counterbalanced ABBAAB to cancel systematic drift.
#
# Usage:
#   ./benchmark-global-stats.sh               # default REPS=3 (6 runs, ~4h)
#   REPS=1 ./benchmark-global-stats.sh        # quick dry-run (2 runs, ~70min)
#   REPS=5 ./benchmark-global-stats.sh        # high-confidence (10 runs, ~6h)

set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Configuration (override via environment)
# ──────────────────────────────────────────────────────────────────────────────

REPS="${REPS:-3}"                                       # Repetitions per mode
PLAYGROUND_TAG="${PLAYGROUND_TAG:-bench-global-stats}"
BACKUP_PATH="${BACKUP_PATH:-}"                          # BR backup directory (required)
DB="${DB:-analyze_profile}"
TABLE="${TABLE:-t_partitioned}"
PARTITION="${PARTITION:-p5}"                             # For single-partition re-analyze
AP="${AP:-$HOME/repos/tidb-dev-hacks/analyze-profile/analyze-profile}"
OUTPUT_BASE="${OUTPUT_BASE:-./benchmark-output}"
SETTLE_WAIT="${SETTLE_WAIT:-90}"                         # Seconds after restore/analyze
PD_ADDR="${PD_ADDR:-127.0.0.1:2379}"
TIDB_HOST="${TIDB_HOST:-127.0.0.1}"
TIDB_PORT="${TIDB_PORT:-4000}"
PD_TIMEOUT="${PD_TIMEOUT:-120}"                          # Max seconds to wait for PD

# ──────────────────────────────────────────────────────────────────────────────
# Derived / internal
# ──────────────────────────────────────────────────────────────────────────────

TIUP_DATA_DIR="$HOME/.tiup/data/${PLAYGROUND_TAG}"
MYSQL_CMD="mysql -h ${TIDB_HOST} -P ${TIDB_PORT} -u root --batch --skip-column-names"
SCRIPT_START=""
RUN_ORDER=()

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

# Find TiDB log path for the current playground instance.
detect_tidb_log_path() {
    local log_dir="${TIUP_DATA_DIR}/tidb-0"
    if [[ -d "$log_dir" ]]; then
        TIDB_LOG="${log_dir}/tidb.log"
    else
        # Fallback: search for any tidb log dir
        local found
        found=$(find "${TIUP_DATA_DIR}" -maxdepth 2 -name 'tidb.log' -print -quit 2>/dev/null || true)
        if [[ -n "$found" ]]; then
            TIDB_LOG="$found"
        else
            log "WARNING: Could not detect TiDB log path; proceeding without --tidb-log"
            TIDB_LOG=""
        fi
    fi
    if [[ -n "$TIDB_LOG" ]]; then
        log "TiDB log: ${TIDB_LOG}"
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

# Start playground in background and wait for it to be ready.
start_playground() {
    log "Starting playground (tag=${PLAYGROUND_TAG})..."
    tiup playground nightly \
        --tag "${PLAYGROUND_TAG}" \
        --without tiflash \
        >"${OUTPUT_BASE}/_playground_${RUN_INDEX}.log" 2>&1 &
    PLAYGROUND_PID=$!
    log "Playground PID: ${PLAYGROUND_PID}"

    wait_for_pd_healthy
    wait_for_tidb
}

# Collect accuracy data: row counts from stats_meta.
collect_accuracy() {
    local outfile="$1"
    log "Collecting accuracy data -> ${outfile}"
    ${MYSQL_CMD} -e "
        SELECT
            t.TABLE_SCHEMA AS db,
            t.TABLE_NAME AS table_name,
            sm.table_id,
            COALESCE(tp.PARTITION_NAME, '(global)') AS partition_name,
            sm.count AS row_count,
            sm.modify_count,
            sm.version
        FROM mysql.stats_meta sm
        JOIN information_schema.tables t
            ON sm.table_id = t.TIDB_TABLE_ID
            AND t.TABLE_SCHEMA = '${DB}'
            AND t.TABLE_NAME = '${TABLE}'
        UNION ALL
        SELECT
            t.TABLE_SCHEMA AS db,
            t.TABLE_NAME AS table_name,
            sm.table_id,
            tp.PARTITION_NAME AS partition_name,
            sm.count AS row_count,
            sm.modify_count,
            sm.version
        FROM mysql.stats_meta sm
        JOIN information_schema.partitions tp
            ON sm.table_id = tp.TIDB_PARTITION_ID
        JOIN information_schema.tables t
            ON tp.TABLE_SCHEMA = t.TABLE_SCHEMA
            AND tp.TABLE_NAME = t.TABLE_NAME
        WHERE t.TABLE_SCHEMA = '${DB}'
          AND t.TABLE_NAME = '${TABLE}'
        ORDER BY partition_name
    " > "${outfile}" 2>&1 || log "WARNING: accuracy query failed (non-fatal)"
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
        echo "Settle wait: ${SETTLE_WAIT}s"
        echo "========================================================================"
        echo ""

        printf "%-6s %-4s %-10s %-14s %-14s %-14s %-14s\n" \
            "Run" "Mode" "Phase" "Duration(s)" "PeakRSS(MB)" "RowCount" "Status"
        printf "%-6s %-4s %-10s %-14s %-14s %-14s %-14s\n" \
            "------" "----" "----------" "--------------" "--------------" "--------------" "--------------"

        for (( i = 1; i <= ${#RUN_ORDER[@]}; i++ )); do
            local mode="${RUN_ORDER[$((i-1))]}"
            local run_label="run_${i}_${mode}"

            for phase in full single; do
                local result_file="${OUTPUT_BASE}/${run_label}_${phase}/profile_result.json"
                if [[ -f "$result_file" ]]; then
                    local duration peak_rss status
                    duration=$(python3 -c "
import json, sys
with open('${result_file}') as f:
    d = json.load(f)
print(f\"{d.get('duration_seconds', 'N/A')}\")
" 2>/dev/null || echo "N/A")
                    peak_rss=$(python3 -c "
import json, sys
with open('${result_file}') as f:
    d = json.load(f)
rss = d.get('peak_rss_mb', d.get('peak_rss_bytes', 'N/A'))
if isinstance(rss, (int, float)) and rss > 10000:
    rss = round(rss / 1024 / 1024, 1)
print(rss)
" 2>/dev/null || echo "N/A")
                    status="OK"
                else
                    duration="N/A"
                    peak_rss="N/A"
                    status="MISSING"
                fi

                # Row count from accuracy
                local row_count="N/A"
                local acc_file="${OUTPUT_BASE}/${run_label}_accuracy.tsv"
                if [[ -f "$acc_file" ]]; then
                    # Global row count (partition = "(global)")
                    row_count=$(grep -i '(global)' "$acc_file" | awk '{print $5}' 2>/dev/null || echo "N/A")
                    [[ -z "$row_count" ]] && row_count="N/A"
                fi

                printf "%-6s %-4s %-10s %-14s %-14s %-14s %-14s\n" \
                    "$i" "$mode" "$phase" "$duration" "$peak_rss" "$row_count" "$status"
            done
        done

        echo ""
        echo "Total elapsed: $(elapsed_since "$SCRIPT_START")"
        echo ""

        # Averages per mode
        for avg_mode in ON OFF; do
            echo "--- ${avg_mode} averages (full ANALYZE) ---"
            python3 -c "
import json, glob, os, sys
durations = []
rss_values = []
for f in sorted(glob.glob('${OUTPUT_BASE}/run_*_${avg_mode}_full/profile_result.json')):
    with open(f) as fh:
        d = json.load(fh)
    dur = d.get('duration_seconds')
    rss = d.get('peak_rss_mb', d.get('peak_rss_bytes'))
    if dur is not None:
        durations.append(float(dur))
    if rss is not None:
        rss = float(rss)
        if rss > 10000:
            rss = rss / 1024 / 1024
        rss_values.append(rss)
if durations:
    print(f'  Duration: mean={sum(durations)/len(durations):.1f}s  min={min(durations):.1f}s  max={max(durations):.1f}s  n={len(durations)}')
else:
    print('  Duration: no data')
if rss_values:
    print(f'  PeakRSS:  mean={sum(rss_values)/len(rss_values):.1f}MB  min={min(rss_values):.1f}MB  max={max(rss_values):.1f}MB')
else:
    print('  PeakRSS:  no data')
" 2>/dev/null || echo "  (could not compute averages)"
            echo ""
        done

        for avg_mode in ON OFF; do
            echo "--- ${avg_mode} averages (single-partition ANALYZE) ---"
            python3 -c "
import json, glob, os, sys
durations = []
rss_values = []
for f in sorted(glob.glob('${OUTPUT_BASE}/run_*_${avg_mode}_single/profile_result.json')):
    with open(f) as fh:
        d = json.load(fh)
    dur = d.get('duration_seconds')
    rss = d.get('peak_rss_mb', d.get('peak_rss_bytes'))
    if dur is not None:
        durations.append(float(dur))
    if rss is not None:
        rss = float(rss)
        if rss > 10000:
            rss = rss / 1024 / 1024
        rss_values.append(rss)
if durations:
    print(f'  Duration: mean={sum(durations)/len(durations):.1f}s  min={min(durations):.1f}s  max={max(durations):.1f}s  n={len(durations)}')
else:
    print('  Duration: no data')
if rss_values:
    print(f'  PeakRSS:  mean={sum(rss_values)/len(rss_values):.1f}MB  min={min(rss_values):.1f}MB  max={max(rss_values):.1f}MB')
else:
    print('  PeakRSS:  no data')
" 2>/dev/null || echo "  (could not compute averages)"
            echo ""
        done

    } | tee "${summary_file}"
}

# ──────────────────────────────────────────────────────────────────────────────
# Preflight checks
# ──────────────────────────────────────────────────────────────────────────────

preflight() {
    [[ -n "${BACKUP_PATH}" ]] || die "BACKUP_PATH is required (set via env or edit script)"
    [[ -d "${BACKUP_PATH}" ]] || die "BACKUP_PATH does not exist: ${BACKUP_PATH}"
    command -v tiup >/dev/null || die "tiup not found in PATH"
    command -v mysql >/dev/null || die "mysql client not found in PATH"
    command -v curl >/dev/null || die "curl not found in PATH"
    command -v python3 >/dev/null || die "python3 not found in PATH"
    [[ -x "${AP}" ]] || die "analyze-profile not found or not executable: ${AP}"
    [[ "${REPS}" -ge 1 ]] || die "REPS must be >= 1"
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
    log "Settle wait: ${SETTLE_WAIT}s"
    log "============================================================"

    for (( RUN_INDEX = 1; RUN_INDEX <= total_runs; RUN_INDEX++ )); do
        local mode="${RUN_ORDER[$((RUN_INDEX-1))]}"
        local run_label="run_${RUN_INDEX}_${mode}"
        local run_start
        run_start=$(date +%s)

        log ""
        log "============================================================"
        log "=== Run ${RUN_INDEX}/${total_runs}: mode=${mode} (elapsed: $(elapsed_since "$SCRIPT_START"))"
        log "============================================================"

        # Create output directories for this run
        mkdir -p "${OUTPUT_BASE}/${run_label}_full"
        mkdir -p "${OUTPUT_BASE}/${run_label}_single"
        mkdir -p "${OUTPUT_BASE}/${run_label}_logs"

        # ── 1. Clean start ────────────────────────────────────────────
        clean_playground
        start_playground

        # ── 2. Configure ──────────────────────────────────────────────
        log "Configuring TiDB session..."
        ${MYSQL_CMD} -e "SET GLOBAL tidb_enable_auto_analyze = OFF"
        ${MYSQL_CMD} -e "SET GLOBAL tidb_gc_run_interval = '999h'"
        log "Auto-analyze: OFF, GC interval: 999h"

        # ── 3. Restore backup ────────────────────────────────────────
        log "Restoring backup from ${BACKUP_PATH}..."
        # Convert relative path to absolute for BR
        local abs_backup_path
        abs_backup_path=$(cd "${BACKUP_PATH}" && pwd)
        tiup br:nightly restore table \
            --db "${DB}" --table "${TABLE}" \
            -s "local://${abs_backup_path}" \
            --pd "${PD_ADDR}" \
            --log-file "${OUTPUT_BASE}/${run_label}_logs/br_restore.log" \
            2>&1 | tail -5
        log "Restore complete. Waiting ${SETTLE_WAIT}s for compaction to settle..."
        sleep "${SETTLE_WAIT}"

        # ── 4. Full ANALYZE ──────────────────────────────────────────
        detect_tidb_log_path
        local tidb_log_flag=""
        [[ -n "${TIDB_LOG:-}" ]] && tidb_log_flag="-tidb-log ${TIDB_LOG}"

        log "Running full ANALYZE (mode=${mode})..."
        "${AP}" profile \
            -analyze-columns all \
            -db "${DB}" \
            -table "${TABLE}" \
            -set-variable "tidb_enable_sample_based_global_stats=${mode}" \
            -truncate-stats \
            -output-dir "${OUTPUT_BASE}/${run_label}_full" \
            ${tidb_log_flag} \
            2>&1 | tee "${OUTPUT_BASE}/${run_label}_full/analyze-profile.log"
        log "Full ANALYZE complete. Waiting ${SETTLE_WAIT}s for compaction to settle..."
        sleep "${SETTLE_WAIT}"

        # ── 5. Single-partition ANALYZE ──────────────────────────────
        log "Running single-partition ANALYZE (partition=${PARTITION}, mode=${mode})..."
        "${AP}" profile \
            -analyze-columns all \
            -db "${DB}" \
            -table "${TABLE}" \
            -set-variable "tidb_enable_sample_based_global_stats=${mode}" \
            -partition "${PARTITION}" \
            -output-dir "${OUTPUT_BASE}/${run_label}_single" \
            ${tidb_log_flag} \
            2>&1 | tee "${OUTPUT_BASE}/${run_label}_single/analyze-profile.log"

        # ── 6. Collect accuracy ──────────────────────────────────────
        collect_accuracy "${OUTPUT_BASE}/${run_label}_accuracy.tsv"

        # ── 7. Collect TiDB logs ─────────────────────────────────────
        log "Collecting TiDB logs..."
        local tidb_log_dir
        tidb_log_dir=$(dirname "${TIDB_LOG:-${TIUP_DATA_DIR}/tidb-0/tidb.log}")
        cp "${tidb_log_dir}"/tidb*.log "${OUTPUT_BASE}/${run_label}_logs/" 2>/dev/null || \
            log "WARNING: Could not copy TiDB logs"

        # ── 8. Shutdown ──────────────────────────────────────────────
        clean_playground

        local run_elapsed
        run_elapsed=$(elapsed_since "$run_start")
        log "=== Run ${RUN_INDEX}/${total_runs} (${mode}) complete in ${run_elapsed} ==="
    done

    # ── 9. Summary ───────────────────────────────────────────────────
    log ""
    log "All runs complete. Total elapsed: $(elapsed_since "$SCRIPT_START")"
    log ""
    print_summary
}

main "$@"
