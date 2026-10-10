#!/usr/bin/env bash
# Copyright 2026 PingCAP, Inc. Licensed under Apache License 2.0.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
COMMAND=${1:-help}
[[ $# == 0 ]] || shift
RUN_DIR= TIDB_BIN=/DATA/disk2/tongzhigao/tidb/bin/tidb-server
TIFLASH_BIN=/DATA/disk2/tongzhigao/tiflash/cmake-build-release/dbms/src/Server/tiflash
TIUP_BIN=/root/.tiup/bin/tiup
PORT_OFFSET=20000 THREADS=8 CPU_LIST= CORPUS=smoke
QUERY_THREADS=
RUN_LIBRARY_PATH=${LD_LIBRARY_PATH:-}
FORWARD=()

usage() {
    cat <<'HELP'
Usage: bash h124.sh COMMAND --run-dir /absolute/path [options]
Commands: start, resume, status, stop, e2e, setup, bench, profile
Start options: --tidb-bin PATH --tiflash-bin PATH --tiup-bin PATH
               --port-offset N --threads N --cpus CPU_LIST
Setup/bench/profile options: --corpus NAME --query-threads N -- SQLBENCH_OPTIONS
Profile defaults: native only, concurrency 1, one trial, 30s profile/32s workload.
Specify one -search (word, phrase, prefix, cjk, miss); default is word.
Examples:
  bash h124.sh start --run-dir /DATA/disk2/tongzhigao/local-match-bench/run-01
  bash h124.sh e2e --run-dir /DATA/disk2/tongzhigao/local-match-bench/run-01
  bash h124.sh setup --run-dir /DATA/disk2/tongzhigao/local-match-bench/run-01 --corpus smoke -- -rows 100000 -bytes 128
  bash h124.sh bench --run-dir /DATA/disk2/tongzhigao/local-match-bench/run-01 --corpus smoke -- -concurrency 1 -iterations 30 -trials 3
No command builds TiDB/TiFlash or modifies their source checkouts.
The bundle must contain Linux amd64 sqlbench and fts-e2e.test binaries.
HELP
}
die() { printf '%s\n' "$*" >&2; exit 1; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --run-dir) RUN_DIR=${2:?}; shift 2 ;;
        --tidb-bin) TIDB_BIN=${2:?}; shift 2 ;;
        --tiflash-bin) TIFLASH_BIN=${2:?}; shift 2 ;;
        --tiup-bin) TIUP_BIN=${2:?}; shift 2 ;;
        --port-offset) PORT_OFFSET=${2:?}; shift 2 ;;
        --threads) THREADS=${2:?}; shift 2 ;;
        --query-threads) QUERY_THREADS=${2:?}; shift 2 ;;
        --cpus) CPU_LIST=${2:?}; shift 2 ;;
        --corpus) CORPUS=${2:?}; shift 2 ;;
        --) shift; FORWARD=("$@"); break ;;
        -h|--help) usage; exit 0 ;;
        *) die "Unknown option: $1" ;;
    esac
done
if [[ $COMMAND == help || $COMMAND == --help || $COMMAND == -h ]]; then usage; exit 0; fi
[[ $RUN_DIR == /* && $RUN_DIR != / && $RUN_DIR != /DATA && $RUN_DIR != /DATA/disk2 ]] || die "Specify a dedicated absolute --run-dir"
[[ $CORPUS =~ ^[A-Za-z0-9_-]+$ ]] || die "Invalid corpus name"
[[ $(uname -s) == Linux && $(uname -m) == x86_64 ]] || die "This bundle targets Linux x86_64"
command -v tmux >/dev/null || die "tmux is required"

binary_fingerprint() { sha256sum "$TIDB_BIN" "$TIFLASH_BIN" "$SCRIPT_DIR/sqlbench" "$SCRIPT_DIR/fts-e2e.test"; }
session_exists() {
    # Some server tmux versions do not support '=' targets for these commands.
    # Verify the resolved name explicitly so a prefix cannot match another run.
    [[ $(tmux display-message -p -t "$SESSION" '#{session_name}' 2>/dev/null) == "$SESSION" ]]
}
assert_owner() {
    session_exists || die "Cluster session is not running"
    [[ $(tmux show-options -v -t "$SESSION" @local_match_run 2>/dev/null) == "$RUN_DIR" ]] || die "tmux session does not belong to this run"
}
load_run() {
    [[ -f $RUN_DIR/run.env ]] || die "No run.env in $RUN_DIR"
    RUN_DIR=$(cd -- "$RUN_DIR" && pwd -P)
    # This file is written exclusively by start, with shell-quoted values.
    source "$RUN_DIR/run.env"
    export LD_LIBRARY_PATH="$RUN_LIBRARY_PATH"
}
launch_session() {
    local launcher waiting ready
    ready="$RUN_DIR/launch.ready-$$-$RANDOM"
    printf -v launcher '%q %q' /usr/bin/bash "$RUN_DIR/launch.sh"
    # Keep a foreground shell until the ownership marker has been installed.
    printf -v waiting 'while [ ! -f %q ]; do sleep 0.2; done; exec %s' "$ready" "$launcher"
    tmux new-session -d -s "$SESSION" "$waiting"
    tmux set-option -t "$SESSION" @local_match_run "$RUN_DIR"
    touch "$ready"
    printf 'Started tmux session %s; log: %s/results/playground.log\n' "$SESSION" "$RUN_DIR"
}
require_e2e() {
    [[ -f $RUN_DIR/results/e2e-binaries.sha256 ]] || die "Run e2e successfully before setup/bench"
    binary_fingerprint > "$RUN_DIR/results/current-binaries.sha256"
    cmp -s "$RUN_DIR/results/e2e-binaries.sha256" "$RUN_DIR/results/current-binaries.sha256" || die "Binaries changed since E2E; restart and rerun e2e"
}

case "$COMMAND" in
    start)
        [[ -x $TIDB_BIN && -x $TIFLASH_BIN && -x $TIUP_BIN && -x $SCRIPT_DIR/sqlbench && -x $SCRIPT_DIR/fts-e2e.test ]] || die "Missing TiDB/TiFlash/TiUP/sqlbench/E2E executable"
        tiup_root="$(dirname -- "$TIUP_BIN")/root.json"
        [[ -r $tiup_root ]] || die "Missing installed TiUP trust manifest: $tiup_root"
        [[ $PORT_OFFSET =~ ^[0-9]+$ && $PORT_OFFSET -ge 10000 && $PORT_OFFSET -le 30000 ]] || die "port-offset must be 10000..30000"
        [[ $THREADS =~ ^[0-9]+$ && $THREADS -ge 1 && $THREADS -le 64 ]] || die "threads must be 1..64"
        [[ -z $CPU_LIST || $CPU_LIST =~ ^[0-9][0-9,-]*$ ]] || die "Invalid CPU list"
        if [[ -n $CPU_LIST ]]; then taskset -c "$CPU_LIST" true || die "Unavailable CPU affinity"; fi
        [[ ! -e $RUN_DIR ]] || die "Run directory already exists; choose a fresh directory"
        # The installed playground probes all component ports during startup.
        # Check the deterministic client/service ports before downloading anything.
        python3 - "$PORT_OFFSET" <<'PY'
import socket, sys
offset = int(sys.argv[1])
for base in (2379,2380,4000,10080,20160,20180,3930,9000,8123,8234,20170,20292):
    with socket.socket() as sock:
        # Match the servers' bind behavior: reject active listeners, not
        # TIME_WAIT sockets left by an otherwise completed graceful stop.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('127.0.0.1', base+offset))
        except OSError as exc:
            raise SystemExit(f'port {base+offset} is unavailable: {exc}')
PY
        mkdir -p "$RUN_DIR"
        RUN_DIR=$(cd -- "$RUN_DIR" && pwd -P)
        mkdir -p "$RUN_DIR/results"
        # A fresh TIUP_HOME requires the installed trusted root manifest before
        # any mirror/component command can initialize its repository metadata.
        mkdir -p "$RUN_DIR/tiup/bin"
        cp "$tiup_root" "$RUN_DIR/tiup/bin/root.json"
        SESSION="local-match-$(basename "$RUN_DIR" | tr -c 'A-Za-z0-9_-' '_')"
        DB_PORT=$((4000+PORT_OFFSET))
        DSN="root@tcp(127.0.0.1:$DB_PORT)/?charset=utf8mb4"
        session_exists && die "tmux session already exists: $SESSION"
        {
            for name in RUN_DIR SESSION TIDB_BIN TIFLASH_BIN TIUP_BIN PORT_OFFSET THREADS CPU_LIST DB_PORT DSN SCRIPT_DIR RUN_LIBRARY_PATH; do
                printf '%s=%q\n' "$name" "${!name}"
            done
        } > "$RUN_DIR/run.env"
        chmod 600 "$RUN_DIR/run.env"
        cat > "$RUN_DIR/tidb.toml" <<EOF
[performance]
max-procs = $THREADS
EOF
        cat > "$RUN_DIR/tiflash.toml" <<EOF
[profiles.default]
max_threads = $THREADS
EOF
        "$TIDB_BIN" -V > "$RUN_DIR/results/tidb-build.txt" 2>&1
        "$TIFLASH_BIN" version > "$RUN_DIR/results/tiflash-build.txt" 2>&1
        binary_fingerprint > "$RUN_DIR/results/start-binaries.sha256"
        { date -u; uname -a; lscpu; free -h; df -h "$RUN_DIR"; } > "$RUN_DIR/results/host.txt"
        cat > "$RUN_DIR/launch.sh" <<'LAUNCH'
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/run.env"
export LD_LIBRARY_PATH="$RUN_LIBRARY_PATH"
export TIUP_HOME="$RUN_DIR/tiup"
export GOMAXPROCS="$THREADS"
PREFIX=()
if [[ -n $CPU_LIST ]]; then PREFIX=(taskset -c "$CPU_LIST"); fi
exec "${PREFIX[@]}" "$TIUP_BIN" playground:v1.16.4 v8.5.8 \
    --tag local-match-perf --host 127.0.0.1 --port-offset "$PORT_OFFSET" \
    --pd 1 --kv 1 --db 1 --tiflash 1 --perf --without-monitor \
    --db.binpath "$TIDB_BIN" --db.config "$RUN_DIR/tidb.toml" \
    --tiflash.binpath "$TIFLASH_BIN" --tiflash.config "$RUN_DIR/tiflash.toml" \
    --db.timeout 180 --tiflash.timeout 300 > "$RUN_DIR/results/playground.log" 2>&1
LAUNCH
        launch_session
        printf 'After the cluster is ready: bash %q e2e --run-dir %q\n' "$SCRIPT_DIR/h124.sh" "$RUN_DIR"
        ;;
    resume)
        load_run
        if session_exists; then die "Session is already running: $SESSION"; fi
        binary_fingerprint > "$RUN_DIR/results/current-binaries.sha256"
        cmp -s "$RUN_DIR/results/start-binaries.sha256" "$RUN_DIR/results/current-binaries.sha256" || die "Binaries changed; use a fresh run directory"
        if [[ -f $RUN_DIR/results/e2e-binaries.sha256 ]]; then
            mv "$RUN_DIR/results/e2e-binaries.sha256" "$RUN_DIR/results/e2e-binaries.previous-$(date -u +%Y%m%dT%H%M%S)-$$.sha256"
        fi
        if [[ -f $RUN_DIR/results/playground.log ]]; then
            cp "$RUN_DIR/results/playground.log" "$RUN_DIR/results/playground.previous-$(date -u +%Y%m%dT%H%M%S)-$$.log"
        fi
        launch_session
        printf 'Existing tagged data retained; rerun e2e before measurement.\n'
        ;;
    status)
        load_run
        if session_exists; then assert_owner; printf 'Session: %s\nSQL: 127.0.0.1:%s\n' "$SESSION" "$DB_PORT"; else printf 'Session stopped\n'; fi
        tail -20 "$RUN_DIR/results/playground.log"
        "$SCRIPT_DIR/sqlbench" -mode check -dsn "$DSN"
        ;;
    stop)
        load_run
        if ! session_exists; then printf 'Session already stopped; artifacts retained in %s\n' "$RUN_DIR"; exit 0; fi
        assert_owner
        tmux send-keys -t "$SESSION:0.0" C-c
        for ((i=0;i<60;i++)); do
            if ! session_exists; then printf 'Stopped; artifacts retained in %s\n' "$RUN_DIR"; exit 0; fi
            sleep 1
        done
        die "Graceful shutdown still pending; inspect $RUN_DIR/results/playground.log"
        ;;
    e2e)
        load_run; session_exists || die "Cluster session is not running"; assert_owner
        [[ -x $SCRIPT_DIR/fts-e2e.test ]] || die "Missing fts-e2e.test"
        if [[ -f $RUN_DIR/results/e2e-binaries.sha256 ]]; then
            mv "$RUN_DIR/results/e2e-binaries.sha256" "$RUN_DIR/results/e2e-binaries.previous-$(date -u +%Y%m%dT%H%M%S)-$$.sha256"
        fi
        "$SCRIPT_DIR/sqlbench" -mode check -dsn "$DSN" | tee "$RUN_DIR/results/runtime-version.txt"
        binary_fingerprint > "$RUN_DIR/results/current-binaries.sha256"
        cmp -s "$RUN_DIR/results/start-binaries.sha256" "$RUN_DIR/results/current-binaries.sha256" || die "Binaries changed since startup; restart with a fresh run directory"
        TIDB_FTS_E2E_DSN="$DSN" "$SCRIPT_DIR/fts-e2e.test" \
            -test.run '^Test(BooleanMatchTiFlashE2E|LocalMatch.*TiFlashE2E|MatchPlanValidation)$' \
            -test.v -test.timeout 30m | tee "$RUN_DIR/results/e2e-$(date -u +%Y%m%dT%H%M%SZ).log"
        cp "$RUN_DIR/results/start-binaries.sha256" "$RUN_DIR/results/e2e-binaries.sha256"
        ;;
    setup|bench|profile)
        load_run; session_exists || die "Cluster session is not running"; assert_owner; require_e2e
        mode=setup; if [[ $COMMAND != setup ]]; then mode=run; fi
        if [[ -z $QUERY_THREADS ]]; then
            QUERY_THREADS=$THREADS
            if [[ $COMMAND == profile ]]; then QUERY_THREADS=1; fi
        fi
        [[ $QUERY_THREADS =~ ^[0-9]+$ && $QUERY_THREADS -ge 1 && $QUERY_THREADS -le 64 ]] || die "query-threads must be 1..64"
        result="$RUN_DIR/results/$COMMAND-$CORPUS-$(date -u +%Y%m%dT%H%M%S)-$$"
        profile_args=()
        if [[ $COMMAND == profile ]]; then
            profile_args=(-search word -duration 32s -profile-seconds 30)
        fi
        controlled_args=()
        if [[ $COMMAND == profile ]]; then
            controlled_args=(-path native -concurrency 1 -trials 1 -profile-url "http://127.0.0.1:$((20292+PORT_OFFSET))" -profile-file "$result.profile")
        fi
        # Record contention; CPU affinity applies to the cluster only. Query
        # thread limits do not reserve physical cores or cap whole-process RSS.
        { date -u; uptime; free -h; } > "$result-host.txt"
        "$SCRIPT_DIR/sqlbench" "${profile_args[@]}" "${FORWARD[@]}" "${controlled_args[@]}" -mode "$mode" -dsn "$DSN" \
            -manifest "$RUN_DIR/$CORPUS.json" -tiflash-threads "$QUERY_THREADS" \
            > "$result.jsonl" 2> "$result.stderr.log" &
        bench_pid=$!
        trap 'kill -TERM "$bench_pid" 2>/dev/null || true; wait "$bench_pid" 2>/dev/null || true; exit 130' INT TERM
        # These samples cover the dedicated client and descendants of the
        # owned tmux pane, including TiUP's component children.
        python3 "$SCRIPT_DIR/sample.py" "$bench_pid" "$(tmux display-message -p -t "$SESSION:0.0" '#{pane_pid}')" > "$result-processes.jsonl" &
        sampler_pid=$!
        if wait "$bench_pid"; then result_code=0; else result_code=$?; fi
        trap - INT TERM
        wait "$sampler_pid" || true
        cat "$result.jsonl"
        printf 'Artifacts: %s.*\n' "$result"
        exit "$result_code"
        ;;
    *) die "Unknown command: $COMMAND" ;;
esac
