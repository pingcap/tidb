# Implement a bounded diagnostic data collection API

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the repository root. This plan must be maintained according to it.

## Purpose / Big Picture

TiDB Cloud diagnostics currently needs selected schema, binding, and statistics metadata, but collecting the corresponding system tables through a diagnostic SQL account couples the feature to SQL privileges, user-visible sessions, and unstable internal table layouts. After this change, an explicitly enabled TiDB status endpoint can expose a small, versioned set of diagnostic datasets through bounded, resumable pages. A cluster-local collector can choose datasets and schedules without giving TiDB Cloud credentials or outbound network responsibilities to the TiDB process.

The observable result is an opt-in HTTP API under `/internal/diagnostics/v1`. `GET /internal/diagnostics/v1/capabilities` describes limits and available datasets. `GET /internal/diagnostics/v1/datasets/{dataset}` returns one page and an opaque continuation cursor. Schema pages are read directly from metadata at one MVCC timestamp. Binding and statistics pages use internal restricted SQL at that same fixed timestamp. Each request is subject to concurrency, duration, page-size, and response-size limits.

The companion collector is maintained outside this repository at `../diagnostic-agent`. It reads a JSON policy, schedules datasets with jitter, retries transient failures, restarts expired snapshots, and atomically commits page chunks plus a SHA-256 manifest to a local spool directory.

## Progress

- [x] (2026-08-19 12:45+08:00) Read the source feasibility document, phase-one scope, data-size analysis, TiDB status handlers, metadata readers, statistics storage readers, binding cache, and TiKV diagnostics/heartbeat paths.
- [x] (2026-08-19 12:45+08:00) Decide the component boundary and the first six safe datasets.
- [x] (2026-08-19 14:05+08:00) Add exclusive keyset iteration primitives for metadata snapshots and focused prefix-boundary/continuation coverage.
- [x] (2026-08-19 14:21+08:00) Add the opt-in configuration, HMAC cursor protocol, six bounded datasets, metrics, API documentation, and endpoint tests.
- [x] (2026-08-19 14:31+08:00) Implement the standalone configuration-driven collector, including scheduling, retry, adaptive page sizing, atomic spool publication, and concurrent disk reservation.
- [x] (2026-08-20 11:15+08:00) Audit TiDB slow-log/TiKV log redaction and the diagnostic payload. Confirm that `ON` normalizes SQL literals, `MARKER` retains source values, and the current diagnostic API still emits metadata identifiers in clear text.
- [x] (2026-08-20 11:45+08:00) Implement versioned diagnostic redaction profiles, cluster-stable HMAC identifier aliases, explicit `omit` policies, recursive field-policy validation, and fail-closed Agent envelope checks.
- [x] (2026-08-20 11:45+08:00) Remove the Agent test dependency on loopback listeners by using an in-memory HTTP transport; the complete Agent test suite now runs in the restricted environment.
- [x] (2026-08-20 12:43+08:00) Complete the Ready gates: rerun all scoped TiDB tests/vet/diff checks, regenerate Bazel metadata with `make bazel_prepare`, run the repository-pinned `make lint`, and rerun the complete Agent tests/vet. Bazel and lint used an ASCII temporary copy because the primary workspace path contains non-ASCII characters; the copy matched the final Go sources and generated metadata was brought back to the primary workspace.
- [x] (2026-08-19 14:52+08:00) Record validation evidence, production risks, GC limits, and intentionally deferred PD/TiKV datasets.

## Surprises & Discoveries

- Observation: TiDB already sets `kv.RequestSourceInternal` and `kv.InternalTxnMeta` on readers returned by `meta.NewReader`, so schema export can use direct metadata snapshots without appearing as user SQL.
  Evidence: `pkg/meta/reader.go` sets both snapshot options before constructing `structure.TxStructure`.

- Observation: `structure.TxStructure` has reverse iteration beginning at a field but no equivalent forward iterator, while `meta.IterDatabases` and `meta.IterTables` always begin at the first hash field.
  Evidence: `pkg/structure/hash.go` exposes `NewHashReverseIterBeginWithField`; `pkg/meta/meta.go` implements both forward methods with `HGetIter`.

- Observation: the existing `TABLE_STORAGE_STATS` and `TIKV_REGION_STATUS` SQL paths can issue broad PD requests. They are unsuitable for a default periodic dataset on very large clusters.
  Evidence: `pkg/executor/infoschema_reader.go` requests Region status per physical table for storage statistics and can scan the full Region keyspace for Region status.

- Observation: TiKV's diagnostics service is a node-local log/system-information service, whereas authoritative cluster Region and Store state is already sent to PD through heartbeats.
  Evidence: `src/server/service/diagnostics/mod.rs` in TiKV serves log and system diagnostics; raftstore PD workers send Region and Store heartbeats.

- Observation: the direct metadata iterator has no context-bearing `Iter` API, but TiDB snapshots support `kv.TiKVClientReadTimeout`.
  Evidence: `pkg/kv/option.go` defines the option and `pkg/store/driver/txn/snapshot.go` applies it to KV read RPCs. The diagnostic handler now sets it from `request-timeout` and also checks the request context between decoded objects.

- Observation: the default cursor TTL is five minutes while the default GC lifetime is ten minutes, and this implementation does not register a service GC safepoint.
  Evidence: a cursor or snapshot that expires returns HTTP 409 and the Agent discards the temporary run before restarting. Large datasets that cannot finish inside the effective window require a later bounded snapshot lease or server-side partitioning.

- Observation: the primary workspace's non-ASCII path is incompatible with part of the local Bazel toolchain, while the earlier sandbox also lacked the required loopback/network access.
  Evidence: the initial `make bazel_prepare` and `make lint` attempts failed before source validation. Repeating both commands in `/private/tmp/tidb-diagnostic-bazel-copy`, whose final Go sources matched the primary workspace, succeeded. Gazelle updated `pkg/config/BUILD.bazel` and `pkg/server/BUILD.bazel`; those generated changes were brought back to the primary workspace.

- Observation: slow-log redaction is not a complete policy for diagnostic payloads. `tidb_redact_log=ON` uses parser normalization for SQL literals, but schema/table/column identifiers remain, while `MARKER` encloses and preserves the original value.
  Evidence: `pkg/parser/digester.go` replaces literals with `?` only in `ON` mode; `pkg/util/redact/redact.go` and the marker branch in the digester retain the source text between `‹` and `›`. Before the diagnostic-specific redactor was added, records in `pkg/server/diagnostic_datasets.go` copied metadata names directly.

- Observation: pooling only the HMAC object left avoidable per-identifier allocation overhead.
  Evidence: the initial benchmark measured 237-295 ns/op, 168 B/op, and 10 allocs/op. Reusing a length-delimited input buffer with the HMAC state reduced five final runs to 144.2-146.1 ns/op, 48 B/op, and one allocation per returned alias on an Apple M4.

- Observation: loopback-bound HTTP tests were an avoidable environmental dependency.
  Evidence: replacing `httptest.Server` with an in-memory `http.RoundTripper` preserved request, retry, status, header, and body behavior while allowing `go test ./...` to pass without opening a local port.

## Decision Log

- Decision: keep scheduling, jitter, retry, byte budgets across runs, disk spooling, and Cloud upload outside TiDB.
  Rationale: TiDB should remain fail-open and should not own Cloud credentials or outbound connectivity. The collector can evolve its policy independently from kernel release cycles.
  Date/Author: 2026-08-19 / Codex

- Decision: expose named, versioned datasets instead of arbitrary system-table names or SQL.
  Rationale: a dataset and field allowlist prevents new internal columns from silently crossing the trust boundary and decouples the protocol from system-table schema changes.
  Date/Author: 2026-08-19 / Codex

- Decision: implement `schema.tables`, `schema.columns`, `schema.indexes`, `schema.partitions`, `binding.summary`, and `stats.health` in the first vertical slice.
  Rationale: these datasets cover the phase-one high-value metadata while excluding raw SQL text, column values, histogram bounds, TopN values, and unbounded Region scans.
  Date/Author: 2026-08-19 / Codex

- Decision: use a new MVCC timestamp for the first page, store it in an HMAC-signed opaque cursor, and repeat every later read at that timestamp.
  Rationale: stateless cursors survive request retries without retaining old InfoSchema objects or long transactions in TiDB memory. Cursors expire quickly and may become invalid after GC, in which case the collector restarts the dataset.
  Date/Author: 2026-08-19 / Codex

- Decision: keep TiKV unchanged in this milestone and defer Region/Store datasets to a PD-backed, filtered API.
  Rationale: querying every TiKV duplicates cluster data, and using current broad SQL virtual tables would violate the source-cost budget. A later design must require table, Region, or Top-K filters.
  Date/Author: 2026-08-19 / Codex

- Decision: reserve each dataset's full `max_run_bytes` before starting a run.
  Rationale: checking only current spool usage allows concurrent runs to pass independently and collectively exceed the disk budget. Reservation makes `spool.max_bytes` a global concurrency invariant; the final directory is then charged by its actual bytes.
  Date/Author: 2026-08-19 / Codex

- Decision: convert binding timestamps from the internal session location to UTC RFC3339Nano.
  Rationale: TiDB `TIMESTAMP` values are interpreted in the session timezone. Formatting their wall-clock fields directly as UTC would shift the represented instant.
  Date/Author: 2026-08-19 / Codex

- Decision: make `strict-v1` the configured production redaction profile and require a cluster-scoped HMAC key plus public key ID whenever the API is enabled under that profile. Keep `metadata-readable-v1` only as an explicit authorization choice.
  Rationale: a keyed alias preserves cross-page and cross-snapshot correlation without exposing low-entropy identifiers to offline dictionary attacks. A process-random cursor key is unsuitable because aliases would change after restart or across TiDB replicas.
  Date/Author: 2026-08-20 / Codex

- Decision: keep redaction independent from `tidb_redact_log` and reject `MARKER` entirely for diagnostic output.
  Rationale: the diagnostic API has a different trust boundary and must not change behavior with a session/global logging variable. `MARKER` is reversible and therefore cannot be treated as outbound sanitization.
  Date/Author: 2026-08-20 / Codex

- Decision: publish additive `redaction_version`, `redaction_key_id`, and per-field policies while retaining the existing protocol major and `fields` list.
  Rationale: existing v1 consumers can ignore additive fields, while the Agent can fail closed on unknown profiles, versions, transforms, unexpected record fields, or key changes inside one snapshot.
  Date/Author: 2026-08-20 / Codex

## Outcomes & Retrospective

The vertical slice is implemented. TiDB exposes six opt-in field-whitelisted datasets with fixed-snapshot pagination, HMAC cursors, mTLS policy, source-side limits, Prometheus metrics, and diagnostic-specific outbound redaction. `strict-v1` pseudonymizes identifiers before JSON encoding and declares selected high-risk source fields as `omit`; `metadata-readable-v1` is an explicit authorization mode. The companion Agent validates exact profile/version semantics, recursively checks emitted and omitted fields, rejects malformed aliases/digests and unknown envelope fields, schedules each dataset independently, retries bounded transient failures, rebuilds HTTP 409 snapshots, shrinks pages after HTTP 413, reserves global spool capacity, and atomically publishes checksummed chunks plus a manifest.

Focused TiDB config, structure, meta, and server tests pass after the final redaction changes, as do scoped TiDB vet, `git diff --check`, Bazel metadata generation, the repository-pinned lint gate, and the complete Agent test/vet suite. Production acceptance still requires scale tests at 10,000, 50,000, and 250,000 tables, front-end SQL p99/CPU gates, real mTLS/network-policy validation, and proof that `schema.columns` completes inside the cursor/GC window.

## Context and Orientation

`pkg/server/http_status.go` creates the status-port Gorilla Mux router. The API is registered there only when `Config.DiagnosticAPI.Enabled` is true. The status listener already uses the cluster TLS configuration from `Config.Security`; production deployment must combine that TLS identity check with a network policy because the status port hosts other administrative endpoints too.

`pkg/config/config.go` owns static TiDB configuration and defaults. A new `[diagnostic-api]` section contains only source-side safety limits: enabled flag, mutual-TLS requirement, allowed datasets, default and maximum page size, maximum concurrent requests, request timeout, cursor time-to-live, and maximum response bytes. Collection frequency does not belong in this section because each TiDB replica would otherwise schedule duplicate cluster-wide work.

`pkg/structure/hash.go` implements the encoded hashes used by TiDB metadata. `pkg/meta/meta.go` maps databases and tables to fields in those hashes. Adding forward iteration after an exact field gives keyset pagination: page N+1 starts immediately after the last field from page N instead of rescanning N pages of metadata. The opaque API cursor carries database ID, table ID, and sub-object ID; IDs are used to reconstruct the exact metadata field, while traversal still follows the hash's encoded key order.

`pkg/domain/domain.go` provides the storage handle and the advanced internal session pool. The schema datasets create a snapshot from `Domain.Store()` at the cursor timestamp, set `kv.TiKVClientReadTimeout`, and pass it to `meta.NewReader`; this preserves the existing internal Meta request tagging while making the configured page timeout effective for KV reads. `binding.summary` selects only digests, status, source, and timestamps from `mysql.bind_info`; it never selects `original_sql`, `bind_sql`, or `default_db`. `stats.health` selects only table ID, version, row count, modify count, snapshot, and histogram version from `mysql.stats_meta`; it never reads TopN or bucket payloads.

A cursor is a URL-safe base64 JSON payload followed by an HMAC-SHA256 signature. The signing key is generated when the handler starts and is never persisted. Consequently, a TiDB restart intentionally invalidates outstanding cursors. The payload contains protocol version, dataset, snapshot timestamp, issue time, and dataset-specific keyset positions. Clients treat HTTP 409 as a signal to discard partial chunks and restart from the first page.

The collector at `../diagnostic-agent` is a separate Go module with only standard-library dependencies. A run writes to a temporary directory, verifies the response envelope and snapshot identity, writes each raw page as a chunk, computes SHA-256, writes a manifest, fsyncs files as practical, and atomically renames the directory only after the final page. An incomplete run is not visible as a completed snapshot.

## Plan of Work

First, add an exclusive forward hash iterator in `pkg/structure/hash.go` and expose `IterDatabasesFrom` and `IterTablesFrom` through `meta.Reader`. Tests will prove that the first page starts at the beginning, a continuation starts strictly after the cursor, unrelated hash fields are skipped, and iteration can stop early without treating the sentinel as a storage failure.

Second, add `DiagnosticAPI` to `pkg/config/config.go`, defaults that keep the feature disabled, and validation for nonzero limits and parseable positive durations. Configuration tests will cover defaults and invalid relationships such as a default page size above the maximum.

Third, add a server-owned diagnostic handler. It will validate the method, dataset allowlist, page size, and cursor before acquiring a request slot. It will apply a request timeout, assign an internal request source, produce typed field-whitelisted records, marshal one bounded response, and emit stable JSON errors. It will return 429 when concurrency is exhausted, 409 for expired/invalid/restarted cursors, 413 when a page exceeds the response limit, and 400/404 for caller errors. Prometheus metrics will track request result, duration, response bytes, and active requests with only bounded dataset/result labels.

Before considering the handler complete, add a diagnostic-specific redactor. `strict-v1` loads a cluster-scoped secret from `redaction-key-file`, identifies it publicly through `redaction-key-id`, and replaces schema/table/column/index/partition names with deterministic HMAC-SHA256 aliases before records reach JSON encoding. `metadata-readable-v1` preserves those names only when explicitly configured. Capabilities and every page declare the profile, redaction version, key ID, and field transforms. No mode may delegate to `tidb_redact_log` or emit marker-wrapped values.

Fourth, register the two routes in `pkg/server/http_status.go`, document them in `docs/tidb_http_api.md`, and add integration-level status-server tests. Tests will show the disabled route is absent, capabilities reflect configured limits, every schema dataset paginates without duplicates, binding responses exclude SQL text, stats health uses stable pages, tampered cursors fail, and a page-size constraint is enforced.

Fifth, implement the collector. Its JSON config will contain source TLS files, output spool limits, global concurrency, retry policy, and independently enabled dataset intervals, jitter, page size, per-run byte limit, and maximum pages. Unit tests use an in-memory `http.RoundTripper` to cover successful pagination, 429/5xx retry, 409 snapshot restart, 413 adaptive page reduction, response-size rejection, and atomic manifest creation without binding a loopback port.

The collector must also require an exact redaction profile and version, validate advertised field class/transform combinations, recursively reject unexpected or missing record fields, validate version-1 alias and digest formats, reject unknown envelope fields and marker content, and record the profile/version/key ID in the immutable manifest. Scheduled runs recheck capabilities so a startup-time mismatch cannot silently fall through to data collection.

Finally, run the repository's required validation. New Go files and changed imports require `make bazel_prepare`. During iteration, use the WIP profile with targeted tests. Before claiming completion, use the Ready profile: the minimum targeted tests plus `make lint`. The standalone collector runs `go test ./...` and `go vet ./...`.

## Concrete Steps

All TiDB commands run from `implementation/tidb`.

1. Format touched Go files:

       gofmt -w <touched Go files>

2. Regenerate Bazel metadata after new files/import changes:

       make bazel_prepare

3. Run focused metadata and server tests using the repository test tags and failpoint procedure from `docs/agents/testing-flow.md`:

       go test -tags=intest,deadlock ./pkg/structure -run 'Test.*Hash.*Iter'
       go test -tags=intest,deadlock ./pkg/meta -run 'Test.*Iter.*From'
       go test -tags=intest,deadlock ./pkg/config -run 'Test.*Diagnostic'
       go test -tags=intest,deadlock ./pkg/server -run 'TestDiagnosticAPI'

4. Run the Ready completion gate:

       make lint

All collector commands run from `implementation/diagnostic-agent`.

       gofmt -w .
       go test ./...
       go vet ./...

Expected successful test commands exit with status 0. Endpoint tests should decode JSON envelopes and compare record IDs rather than depending on object order outside the documented cursor order.

## Validation and Acceptance

With the default configuration, requests below `/internal/diagnostics/v1` must not reveal diagnostic data. With the feature enabled, capabilities must list only configured safe datasets and configured limits.

For a test cluster containing multiple databases, tables, columns, indexes, and partitions, repeatedly requesting `page_size=1` with each returned cursor must visit every expected record exactly once and then return `complete: true` with an empty next cursor. A cursor modified by one byte, used for another dataset, used after its TTL, or created before a handler restart must return 409 and no records.

Binding test data containing distinctive SQL literals must produce a response that contains its SQL and plan digests but does not contain the original SQL, binding SQL, default database, or literal. Statistics health must return metadata counts and versions without accessing or serializing histogram bucket and TopN values.

At most the configured number of dataset requests may execute concurrently. An excess request returns 429 quickly. A request that exceeds its context deadline fails without holding a semaphore token. A marshaled page larger than the configured byte limit returns 413 and the collector retries with a smaller page.

The collector must never publish a completed output directory for a failed or partial run. A successful run must contain numbered JSON chunks and a manifest whose per-chunk SHA-256 values and aggregate byte/record counts match the files. Retry attempts must honor the configured cap and context cancellation.

## Idempotence and Recovery

Code generation, formatting, and tests are safe to rerun. API reads are read-only and cursors do not create server-side state. Repeating a page with the same cursor reads the same MVCC snapshot and produces the same logical records while that snapshot remains above the GC safe point.

If a cursor expires, its signature fails after restart, or the snapshot has been garbage-collected, the client deletes only its own temporary run directory and begins a new snapshot. Existing completed spool directories are never overwritten; a collision receives a unique suffix. If configuration validation fails, both TiDB startup and the collector fail before serving or scheduling work.

Rollback consists of setting `diagnostic-api.enabled = false` and restarting TiDB, then stopping the collector. The implementation does not modify on-disk TiDB metadata or TiKV behavior.

## Artifacts and Notes

Source feasibility and sizing material is outside the TiDB clone in the parent workspace, including `系统表采集-内部API与日志方案可行性分析.md`, `系统表采集第一期最终范围.md`, and `第一阶段落地清单-10000与250000表代价和存储成本估算.md`.

The implementation intentionally does not expose arbitrary `INFORMATION_SCHEMA` or `mysql` tables, raw histogram bounds, TopN values, SQL text, comments, default values, partition boundary values, full Region listings, or direct Cloud upload credentials.

Validation evidence from 2026-08-19 through 2026-08-20 (run from `implementation/tidb` unless noted):

    env GOCACHE=/private/tmp/codex-go-cache go test -tags=intest,deadlock -run '^TestDiagnosticAPIConfigValid$' -count=1 ./pkg/config
    env GOCACHE=/private/tmp/codex-go-cache ./tools/check/failpoint-go-test.sh pkg/meta -run '^(TestMeta|TestIterDatabases)$' -count=1
    env GOCACHE=/private/tmp/codex-go-cache go test -tags=intest,deadlock -run '^TestHash$' -count=1 ./pkg/structure
    env GOCACHE=/private/tmp/codex-go-cache ./tools/check/failpoint-go-test.sh pkg/server -run '^TestDiagnosticAPI$' -count=1
    env GOCACHE=/private/tmp/codex-go-cache go vet -tags=intest,deadlock ./pkg/config ./pkg/meta ./pkg/structure ./pkg/server
    git diff --check

All commands above exited with status 0. From `implementation/diagnostic-agent`, these also exited with status 0:

    env GOCACHE=/private/tmp/codex-agent-go-cache go test ./...
    env GOCACHE=/private/tmp/codex-agent-go-cache go vet ./...

The repository generation and Ready gates also exited with status 0 in `/private/tmp/tidb-diagnostic-bazel-copy`, an ASCII-path copy whose final Go sources were byte-identical to the primary workspace:

    make bazel_prepare
    make lint

Gazelle's generated changes were brought back to the primary workspace: `pkg/config/BUILD.bazel` now has the updated test shard count, and `pkg/server/BUILD.bazel` includes the new `pkg/meta/model` dependency. Full Agent HTTP tests no longer require loopback permission because they use an in-memory transport.

## Interfaces and Dependencies

In `pkg/structure/hash.go`, add an exclusive forward-iteration method equivalent in semantics to the existing reverse iterator's begin-with-field behavior:

    func (t *TxStructure) HGetIterFrom(key, exclusiveStartField []byte, fn func(pair HashPair) error) error

In `pkg/meta/reader.go` and `pkg/meta/meta.go`, add:

    IterDatabasesFrom(exclusiveStartDBID int64, fn func(info *model.DBInfo) error) error
    IterTablesFrom(dbID, exclusiveStartTableID int64, fn func(info *model.TableInfo) error) error

In `pkg/config/config.go`, add `Config.DiagnosticAPI DiagnosticAPI` with TOML/JSON name `diagnostic-api` and a `DiagnosticAPI` struct holding the source-side limits described above.

The HTTP API response has this stable outer envelope:

    {
      "protocol_version": "1.0",
      "dataset": "schema.tables",
      "snapshot_id": "...",
      "snapshot_ts": 123,
      "schema_version": 456,
      "captured_at": "2026-08-19T12:00:00+08:00",
      "sensitivity_level": "L2",
      "redaction_profile": "strict-v1",
      "redaction_version": 1,
      "redaction_key_id": "diag-name-2026-08",
      "records": [],
      "next_cursor": "...",
      "complete": false
    }

Only Go standard-library cryptography and encoding are needed for cursors. The existing Gorilla Mux router, Domain snapshot/meta interfaces, restricted SQL executor, PingCAP error utilities, and Prometheus libraries already present in TiDB are reused. The standalone collector intentionally has no third-party module dependency.

Revision note (2026-08-20 12:43+08:00): updated after the diagnostic-specific redaction implementation and final Ready validation; recorded field-level `omit` policies, stable HMAC aliases, recursive Agent validation, the redaction microbenchmark, successful Bazel/lint gates in an ASCII-path copy, and the removal of the Agent loopback-test dependency.
