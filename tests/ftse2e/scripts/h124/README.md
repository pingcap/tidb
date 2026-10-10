# h124 Local MATCH performance harness

If a locally built TiFlash needs LLVM shared libraries, set `LD_LIBRARY_PATH` when invoking `start` (on h124: `/DATA/disk2/tongzhigao/llvm-17/lib`). The harness records this path in `run.env` and restores it for the detached launcher and subsequent commands, including `resume`. It does not modify the system loader configuration.

The dedicated TiUP home is bootstrapped with `root.json` beside the supplied installed TiUP executable. This reuses its trusted manifest without changing the default TiUP home or disabling signature verification.

The harness leaves `tidb_allow_mpp`, `tidb_allow_tiflash_cop` and `tidb_enforce_mpp` unchanged and checks their session values are the defaults ON/OFF/OFF. It limits the native read engine to TiFlash without forcing MPP through cost overrides. Each native COUNT plan must place MATCH in an `mpp[tiflash]` Selection with an MPP TableFullScan or TableRangeScan; cop, batchCop, TiCI and root-side MATCH are rejected. The E2E statement-summary decoder labels MPP tasks as `cop[tiflash]` on this branch, so sampled executed prepared plans additionally require an MPP TableReader/ExchangeSender envelope before accepting those decoded labels.

This harness uses a dedicated TiUP home, tagged data directory, localhost ports and an owned tmux session. It never builds TiDB/TiFlash, checks out branches or deletes benchmark data. Supply matching Local MATCH binaries before starting. The server's `build-release.sh` currently uses RelWithDebInfo with tests/failpoints disabled; retain the actual CMake settings with the results.

Build the standalone Linux amd64 bundle from the paired TiDB checkout, reusing its Go cache:

```bash
bash tests/ftse2e/scripts/h124/package.sh /private/tmp/new-local-match-h124-bundle
```

Copy the complete bundle to a new directory on h124. It contains the SQL runner and compiled E2E executable, so running the scripts does not require a server-side Go build. `source-revision.txt`, `source-status.txt`, `build-info.txt` and `SHA256SUMS` identify the bundle; sqlbench source and corpus source are included. Check `sha256sum -c SHA256SUMS` after transfer.

Run the following on h124, replacing the bundle path and choosing a fresh run directory:

```bash
bundle=/DATA/disk2/tongzhigao/local-match-bench/scripts-20261010
run=/DATA/disk2/tongzhigao/local-match-bench/run-20261010-01
bash "$bundle/h124.sh" start --run-dir "$run" --tidb-bin /path/to/local-match/tidb-server
bash "$bundle/h124.sh" status --run-dir "$run"
bash "$bundle/h124.sh" e2e --run-dir "$run"
bash "$bundle/h124.sh" setup --run-dir "$run" --corpus smoke -- -rows 100000 -bytes 128
bash "$bundle/h124.sh" bench --run-dir "$run" --corpus smoke -- -concurrency 1 -iterations 30 -trials 3
bash "$bundle/h124.sh" stop --run-dir "$run"
```

Start defaults to TiDB `/DATA/disk2/tongzhigao/tidb/bin/tidb-server` and TiFlash `/DATA/disk2/tongzhigao/tiflash/cmake-build-release/dbms/src/Server/tiflash`; overrides are accepted. Port offset 20000 gives SQL port 24000. `--threads 8` sets TiDB max-procs/GOMAXPROCS and TiFlash profile max_threads. Setup/bench inherit it for session tidb_max_tiflash_threads; `--query-threads N` independently overrides the session limit without throttling TiDB. Profile defaults to query threads 1. These settings are not reservations of physical cores or whole-process memory limits, nor a guarantee of one OS thread across all MPP tasks. Supply a verified CPU set with `--cpus`, which binds the whole cluster; per-component CPU/memory isolation remains a prerequisite for controlled comparisons. Wait for competing builds and background storage work to settle before timing. The client runs outside the cluster CPU affinity; choose its affinity separately when required.

The start command returns after creating a tmux session; cluster readiness is reported in `results/playground.log`, and status checks runtime `tidb_version()` and the Local MATCH system variable. It must pass E2E before setup or measurement. E2E waits for each test table's replica AVAILABLE=1 and removes its unique temporary schemas. The successful gate records server/tool binary checksums; replacing a binary requires a fresh cluster and E2E. A failed E2E rerun invalidates the previous gate. Build/host evidence is saved at startup. Stop sends Ctrl-C only to the tmux session whose ownership marker matches the run directory, waits up to 60 seconds and retains all data/logs. Resume reuses the tagged data with identical binaries/settings, archives the previous playground log and requires E2E again before measurement. Invoke `bash "$bundle/h124.sh" resume --run-dir "$run"`; existing corpus manifests can then be reused. The commands never force-kill or clean other clusters.

Setup creates a unique `fts_bench_` database, paired BIGINT-id/MEDIUMTEXT tables with public FULLTEXT definitions, inserts identical data, and waits for the native replica AVAILABLE=1. The no-replica table stays on TiKV. Its manifest records row count, document size, parser/column collation, stopword collation (`collation_server`), runtime version and analyzer token sizes. Run rejects changed GLOBAL analyzer settings and pins/verifies the recorded stopword collation on every worker session. Manifests without stopword collation are rejected; create a new corpus rather than guessing the missing setting. Setup never changes GLOBAL variables. A failed setup retains a `creating` manifest identifying the partially populated owned database; run refuses it, and setup refuses to overwrite a manifest. Use a new corpus name after diagnosing the failure. There is no automatic database deletion.

The corpus repeats the existing four row units (match, excluded match, uppercase and miss). It is highly compressible and represents a baseline only. Create separate corpora for each requested size/parser/collation:

```bash
bash "$bundle/h124.sh" setup --run-dir "$run" --corpus standard-4k -- -rows 1000000 -bytes 4096 -parser standard -collation utf8mb4_bin
bash "$bundle/h124.sh" setup --run-dir "$run" --corpus ngram-4k-ci -- -rows 1000000 -bytes 4096 -parser ngram -collation utf8mb4_general_ci
bash "$bundle/h124.sh" setup --run-dir "$run" --corpus large-text -- -rows 10000 -bytes 262144
bash "$bundle/h124.sh" bench --run-dir "$run" --corpus standard-4k -- -concurrency 4 -duration 60s -trials 3 -stopwords=true
```

NGRAM size uses the dedicated cluster's current GLOBAL setting; the tool does not switch it. Prepare a separate configuration/run for size 3, rerun E2E and use fresh corpora. A manifest cannot be reused after token-size changes. Nonrepetitive corpora, selectivity control, representative customer data, range-performance workloads and full server metrics remain subsequent extensions; existing correctness E2E includes range scans. Small corpora may have too few regions/read tasks/blocks to exercise the configured thread limit: inspect EXPLAIN ANALYZE and storage task counts before interpreting a 1-versus-8-thread speedup as parallel scaling.

Run verifies table counts and replica placement, checks each COUNT query's plan for a TiFlash MATCH Selection or root TiDB MATCH, and compares SHA256 fingerprints of the complete ordered matching row IDs before timing. This verification uses ORDER BY and is separate from COUNT timing. Four searches cover required/excluded terms, phrase, prefix and Chinese NGRAM. All workload connections are persistent; warmup and correctness checks are outside the timed interval. Every completed query must return the validated count. Trial order alternates TiDB/TiFlash; errors or changed results stop the run with a nonzero exit status. Prepared statements are not used by this initial performance runner.

Iterations are the total successful query attempts across workers for each path/trial; duration instead specifies how long workers may begin queries. In-flight queries finish within the per-query timeout, so elapsed time can exceed duration. Each worker has its own connection and thread settings. JSONL records counts, errors, elapsed time, QPS and nearest-rank p50/p95/p99 per trial. Thirty samples suffice for smoke, not a reliable tail estimate; use longer duration for acceptance. Derive speedups from matching query/config/trial pairs without combining microbenchmark rates and SQL latency. Both table layouts and scan engines contribute to the end-to-end difference.

Each setup/bench invocation also samples /proc cumulative CPU time, RSS and available I/O counters for the owned cluster tree and client once per second. Host load and available memory are recorded. These samples do not collect full component metrics or exact peaks between samples. Raw measurement, verification plans, stderr and process samples live under `results/`. Credentials are not included in JSON output; the default DSN is local root without a password.

## Bounded token-memory stress

Run `TestLocalMatchTokenMemoryTiFlashE2E` separately with `TIDB_FTS_MEMORY_STRESS=1` and a local `TIDB_FTS_E2E_DSN`. The opt-in test uses two MATCH columns, STANDARD/NGRAM, binary/general-ci collation, stopwords ON/OFF, large/short/NULL/empty rows, phrase/prefix/miss queries, and four native connections limited to one query thread each. It validates TiDB fallback results and native MPP plans before 48 concurrent native query executions per configuration. Set `TIDB_FTS_MEMORY_BYTES=262144` for a smaller run or omit it for approximately 1 MiB documents. Repeat on a dedicated cluster with NGRAM sizes 1/2/3; the test never changes GLOBAL variables. Size 1 is intentionally demanding and is not included automatically in the default suite.

Sample the test client and the owned tmux pane with `python3 sample.py CLIENT_PID PANE_PID 0.1 > memory-processes.jsonl`. The optional interval must be 0.05..10 seconds and sets the sleep between scans; `/proc` collection adds overhead, so use sample timestamps for the actual interval. The samples include current RSS and Linux VmHWM, and stop if either root PID is replaced. VmHWM is a process-lifetime high-water mark; current RSS includes caches, storage, proxy and background work. Record idle baseline and post-query RSS, and use fresh processes for comparable configurations. Neither sampled RSS nor VmHWM proves a token-specific leak or exact allocator peak. Sanitizer/allocation profiling remains a separate check.

## Single-query CPU profiling

`-path native` or `-path local` selects only the timed path, but both paths still undergo complete matching-ID verification first. `-search word|phrase|prefix|cjk|miss` selects a fixed workload. Measure latency without sampling separately from profiling:

```bash
bash "$bundle/h124.sh" bench --run-dir "$run" --corpus standard-4k --query-threads 1 -- -path native -search word -concurrency 1 -iterations 30 -trials 3
bash "$bundle/h124.sh" profile --run-dir "$run" --corpus standard-4k -- -search word
bash "$bundle/h124.sh" profile --run-dir "$run" --corpus standard-4k -- -search phrase -profile-format svg
go tool pprof -top /path/to/results/profile-standard-4k-TIMESTAMP-PID.profile
go tool pprof -http=127.0.0.1:8080 /path/to/results/profile-standard-4k-TIMESTAMP-PID.profile
```

Profile uses the owned cluster's proxy status port (`20292 + port-offset`), preserves the default MPP settings and enforces one native connection/trial and a single search. It requests 99Hz CPU sampling only after verification and warmup, saves protobuf by default or a ready-to-open SVG with `-profile-format svg`, and refuses to overwrite artifacts. The status endpoint has no sampling-start acknowledgement; the runner allows 300ms for setup and runs workload at least two seconds longer than the requested sampling interval. Defaults are 30s sampling/32s workload; override both `-profile-seconds` and `-duration` together. A failed HTTP profile or changed query result fails the run. Do not launch concurrent profile commands: the service permits one active CPU profile per process. The captured samples cover the whole process, including proxy/background threads; attribute hotspots by stack/thread rather than treating every sample as Local MATCH cost. Profiles have instrumentation overhead and are not acceptance latency measurements. Keep status access on loopback, and do not change system perf permissions.
