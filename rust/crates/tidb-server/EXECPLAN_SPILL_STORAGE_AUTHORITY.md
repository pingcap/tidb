# Make configured spill storage authoritative before serving

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan follows that document and the repository requirement that a bug fix have a failing regression before the production change and a passing regression afterward.

## Purpose / Big Picture

An operator who starts the Rust TiDB node with a TiDB configuration file must get the configured temporary-storage path, quota, and spill-file encryption method. Before the listener reports ready, the node must own a private temporary directory, reject a conflicting second owner, reject a quota larger than available storage, and ensure all spill consumers account into one process authority. Spill files must be private even when encryption is plaintext.

The observable outcome is that `--config` controls real Sort, TopN, HashAgg, and hash-join spills; AES configuration produces encrypted private files, plaintext files are still mode `0600`, the directory is mode `0750`, global quota exhaustion returns MySQL 1105 containing `Out Of Quota For Local Temporary Space!`, and close/error paths remove files and release accounting. This plan does not reproduce Go goroutine timing, panic machinery, global mutable configuration objects, or standard-library allocation behavior.

## Progress

- [x] (2026-08-10) Audited accepted Go commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f` and current Rust checkpoint `186c9a85be0efde5e2e2799cba28454b648a3b97`.
- [x] (2026-08-10) Chose one immutable Rust storage authority rather than wiring the existing mutable path/encryption globals.
- [x] (2026-08-10) Added and ran a deterministic fail-before regression proving `--config` cannot currently configure production spill policy; the exact aggregated server test exited 101 with `Err(UnknownOption("--config"))`.
- [x] (2026-08-10) Implemented secure `SpillStorageSpec` / `SpillStorage` ownership in `tidb-util` and deleted the replaced mutable temp-directory authority.
- [x] (2026-08-10) Loaded and normalized the supported TiDB config surface into `NodeConfig`, with explicit CLI values overriding file values and unsupported leaves failing closed.
- [x] (2026-08-10) Acquired and held the storage authority at the top of `run_configured_node`, before PD, TLS, authentication, or listener work.
- [x] (2026-08-10) Threaded the authority through `Session`, `StmtContext`, `StatementMemory`, and every live spill-store constructor.
- [x] (2026-08-10) Made disk accounting fallible at the common store boundary and preserved the exact storage-quota error text.
- [x] (2026-08-10) Ported focused path, permissions, same-process/subprocess locking, encryption, quota, attachment, and cleanup regressions.
- [x] (2026-08-10) Ran focused behavioral regressions, strict changed-crate Clippy, workspace all-target checking, source-size/format/diff gates, and an independent final P0/P1 review.
- [x] (2026-08-10) Committed the verified WIP checkpoint and pushed the branch to both remotes.

## Surprises & Discoveries

- Observation: current plaintext spill files are created with ordinary `OpenOptions` and directories with ordinary `create_dir_all`, so common Unix umasks leave SQL spill data readable by other local users.
  Evidence: `rust/crates/tidb-chunk/src/chunk_in_disk.rs` creates files without `OpenOptionsExt::mode`; `rust/crates/tidb-util/src/disk/temp_dir.rs` creates directories without `DirBuilderExt::mode`. Accepted Go uses `0600` temporary files and `0750` directories.

- Observation: the Rust config tree already parses and validates all three spill settings, but the executable rejects `--config` and no production path consumes them.
  Evidence: `rust/crates/tidb-config/src/config_tree/config.rs`, `rust/crates/tidb-server/src/node_config.rs`, and `rust/crates/tidb-server/tests/node_config_source.rs`.

- Observation: a startup quota setter alone would be a facade because the four live spill operators create detached unlimited disk roots.
  Evidence: `rust/crates/tidb-executor/src/sort.rs`, `topn.rs`, `hash_agg.rs`, and `join.rs` each create a disk tracker without a process ancestor.

- Observation: Go treats `tmp-storage-path` as a base directory and appends a UID and final host/port/status identity after command-line overrides.
  Evidence: accepted `pkg/config/config.go::UpdateTempStoragePath` and `cmd/tidb-server/main.go::overrideConfig`.

- Observation: POSIX `F_SETLK` alone does not reject a second authority in the same process; record locks are process-owned, and opening/closing another descriptor can release the existing kernel lock.
  Evidence: adversarial review of the first implementation plus accepted `gofslock`'s process-local inode registry. The final authority serializes local acquisition and registers both canonical path and device/inode before any sweep; `same_process_is_refused_before_any_sweep` pins the boundary.

- Observation: `cluster-verify-cn` is an inbound peer-CN allowlist, but this bounded node owns only outbound PD/TiKV/etcd clients and `ClusterSecurity::client_tls_config` cannot enforce it.
  Evidence: `tidb-pd-client/src/security.rs` and a repository-wide consumer scan. Both TOML and CLI forms now fail closed instead of promising an unenforced security restriction.

- Observation: the current Rust capacity probe is implemented for Linux and macOS. Windows deliberately rejects a nonnegative startup quota rather than claiming to validate capacity without an equivalent API; other supported Unix targets retain the prior unlimited-capacity seam.
  Evidence: the target-specific `available_bytes` implementations in `tidb-util/src/disk/spill_storage.rs`. This checkpoint does not claim Windows or whole-package completion.

## Decision Log

- Decision: model one immutable `Arc<SpillStorage>` containing the resolved path, encryption method, global disk tracker, and directory lease.
  Rationale: path, encryption, quota, and ownership must be one atomic policy so paired row-data/offset files cannot split across settings. This is idiomatic Rust and matches observable behavior without duplicating Go globals.
  Date/Author: 2026-08-10, Codex.

- Decision: require the storage authority at physical spill-store construction and pass it through `Session` to `StmtContext` and `StatementMemory`.
  Rationale: optional attachment can silently bypass quota. The existing mandatory statement-memory resource flow is the narrowest path every live spill operator already shares.
  Date/Author: 2026-08-10, Codex.

- Decision: return a typed disk-quota error after accounting instead of panicking.
  Rationale: the Go panic is runtime transport. The SQL-visible contract is generic 1105 with `Out Of Quota For Local Temporary Space!`; a fallible Rust boundary preserves that result and cleanup.
  Date/Author: 2026-08-10, Codex.

- Decision: accept `--config`, map every explicitly configured key that the bounded Rust node already consumes, and reject explicitly present keys outside that executable surface.
  Rationale: silently accepting a known TiDB option that the bounded node ignores would violate its existing fail-closed startup contract. Dedicated `--tmp-storage-*` flags would invent a different public interface.
  Date/Author: 2026-08-10, Codex.

- Decision: combine an in-process canonical-path/device-inode registry with the POSIX/Windows OS lease, and hold both until the authority drops.
  Rationale: the OS lock protects against other processes; the registry closes the process-owned POSIX lock hole without recreating Go's library internals. Lock acquisition still precedes stale-file sweeping.
  Date/Author: 2026-08-10, Codex.

- Decision: reject `cluster-verify-cn` at this executable boundary until an inbound cluster endpoint owns it.
  Rationale: fail-closed security is observable semantics. Carrying the vector in a struct while every live transport ignores it is not implementation parity.
  Date/Author: 2026-08-10, Codex.

## Outcomes & Retrospective

The implementation is in WIP validation; no package-completion claim is made. The original config regression is green (27 config/startup tests), the storage suite proves private unique files plus same-process and subprocess exclusion, all 161 `tidb-chunk` unit tests plus its integration probes passed during this checkpoint, 22 executor spill tests passed, both query and DML statement contexts inherit the startup authority, the exact 1105/HY000 quota transport regression is green, and the workspace all-target check passes. Strict changed-crate Clippy is green after allowing only four byte-unchanged baseline lint classes; a no-allowance run was also attempted and stopped only on those pre-existing classes outside this diff. The package-wide `pkg/util/chunk` receipt and later TypeChunk/cursor/merge/CTE integration remain active goal work.

## Context and Orientation

`tidb-config` owns the source-shaped TOML model. `tidb-server::NodeConfig` is the executable’s fail-closed startup model. `tidb-util::disk` owns filesystem and tracker primitives below configuration. `tidb-chunk` owns the two physical spill stores and checksum/encryption layers. `tidb-executor::StatementMemory` is the mandatory per-statement resource authority used by Sort, TopN, HashAgg, and Join. `tidb-session::Session` creates each statement context.

The accepted Go startup order is configuration file, explicit CLI overrides, validation, final temporary-path derivation, secure directory initialization and lock, capacity check, process disk-quota configuration, then serving. The Rust node must preserve that order. A storage authority is a long-lived object that holds the exclusive lock and creates every file under one fixed policy.

## Plan of Work

First add a server regression that supplies `--config` with the PD path and AES spill setting while keeping Rust-only account/table extensions on the command line. At the current checkpoint it must fail because `--config` is unsupported. Extend it after implementation to assert normalized policy and CLI precedence.

Next add `SpillEncryptionMethod`, `SpillStorageSpec`, and `SpillStorage` under `tidb-util::disk`. Opening storage creates the final directory with private permissions, opens and non-blocking-locks `_dir.lock`, sweeps stale entries except `_dir.lock` and `record`, checks available bytes when quota is nonnegative, and creates mode-`0600` unique files. The held lock file is the RAII lease. The authority owns one global disk tracker and exposes child tracker construction plus a quota check.

Then remove the mutable path/encryption authority from `tidb-util` and `tidb-chunk`. `DiskFileReaderWriter`, `DataInDiskByChunks`, `DataInDiskByRows`, and `RowContainer` receive the same `Arc<SpillStorage>`. Tests build isolated authorities instead of changing process globals.

Add config-file ingestion to `NodeConfig`. Parse the file through `tidb_config::Config::load_str`, validate it, reject explicitly configured unsupported paths, seed supported values from the file, and apply explicit CLI values afterward. Normalize mixed-case encryption and derive the final temp path from the final host/port, status host/port, OS temporary directory or configured base, and effective UID.

At the crate-root `run_configured_node`, open the authority before any mode branch. Hold it for the whole blocking server run. Pass it into `ClusterSessionFactory`; attach it to each `tidb_session::Session`; add `StmtContext::with_spill_storage`; and let `StatementMemory` create statement/operator disk trackers under its global tracker. Migrate Sort, TopN, HashAgg, and Join to `operator_disk_tracker` and pass the authority to their physical stores.

Finally, make each physical store’s accounting call check the storage authority after consuming bytes. Map quota exhaustion through the existing spill error path to MySQL 1105 with the accepted message. Ensure closing releases bytes and removes files even after failure.

## Concrete Steps

Work from `/private/tmp/task325-chunk-ee558/rust` unless stated otherwise.

Run the fail-before server regression and expect exit 101 with `Err(UnknownOption("--config"))`:

    cargo test --offline --locked -j12 -p tidb-server --test all node_config_source::configured_spill_policy_is_loaded_from_the_tidb_config_file -- --exact --nocapture

After the storage primitive exists, run its focused tests:

    cargo test --offline --locked -j12 -p tidb-util disk::spill_storage --lib

Run physical storage and quota tests:

    cargo test --offline --locked -j12 -p tidb-chunk chunk_in_disk --lib
    cargo test --offline --locked -j12 -p tidb-chunk row_in_disk --lib
    cargo test --offline --locked -j12 -p tidb-chunk row_container --lib

Run the four executor spill surfaces and server config/startup tests:

    cargo test --offline --locked -j12 -p tidb-executor sort --lib
    cargo test --offline --locked -j12 -p tidb-executor topn --lib
    cargo test --offline --locked -j12 -p tidb-executor hash_agg --lib
    cargo test --offline --locked -j12 -p tidb-executor join --lib
    cargo test --offline --locked -j12 -p tidb-server --test all node_config_source:: --nocapture

Run strict WIP gates before pushing:

    cargo fmt --all -- --check
    scripts/check-source-size.sh
    cargo clippy --offline --locked -j12 --no-deps -p tidb-util -p tidb-config -p tidb-chunk -p tidb-executor -p tidb-session -p tidb-server --all-targets -- -D warnings -A clippy::needless_update -A clippy::needless_borrow -A clippy::needless_question_mark -A clippy::useless_conversion
    cargo check --offline --locked -j12 --workspace --all-targets

## Validation and Acceptance

Acceptance requires behavioral proof, not just compilation. The original `--config` test must change from red to green. A configured AES authority must create real encrypted row and chunk files; plaintext must remain private. A second authority over the same resolved path must fail until the first drops. A quota above available bytes must fail before a listener or ready event. Sort, TopN, HashAgg, and Join must all account under the one process tracker, and aggregate quota exhaustion must surface as 1105 with the accepted text. Success, quota error, and shutdown must all return tracked bytes to zero and remove spill files.

## Idempotence and Recovery

All test directories are unique scratch paths and may be removed after their authority drops. The storage open operation is fail-closed: if directory creation, locking, sweeping, or capacity validation fails, it returns without starting the server. Re-running tests is safe. If a test process dies while holding a lock, the operating system releases the lock; the next successful open sweeps stale spill entries.

Git changes stay in the isolated branch `codex/task325-util-chunk-package-v1`. Do not edit the main checkout. If upstream advances, commit the coherent checkpoint, fetch, rebase this branch, rerun the affected gates, and push only after both remote refs are verified.

## Artifacts and Notes

Accepted Go authority is commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`. Rust baseline for this plan is `186c9a85be0efde5e2e2799cba28454b648a3b97`.

The exact storage-quota message is:

    Out Of Quota For Local Temporary Space!

## Interfaces and Dependencies

`tidb-util::disk` exposes an immutable `SpillStorageSpec`, `SpillEncryptionMethod`, and `SpillStorage`. `SpillStorage::open` returns an owned authority. `SpillStorage::create_file` is the only physical spill-file creation path. `SpillStorage::global_tracker` owns the process accounting root; operator and physical-store children report quota through `Tracker::consume_and_check_exceed`.

`NodeConfig` will contain a normalized `SpillStartupPolicy` or directly contain the `SpillStorageSpec`; no raw encryption string reaches execution.

`Session` will expose an attachment method used by production factories. `StmtContext` and `StatementMemory` will expose `with_spill_storage`, `spill_storage`, and `operator_disk_tracker`. Physical spill constructors will require the authority; no production constructor may fall back to mutable process state.

Revision note (2026-08-10): initial plan written after the config and storage audits established the dependency-closed authority and security boundary.

Revision note (2026-08-10): corrected the server's aggregated `--test all` invocation and recorded the deterministic `UnknownOption("--config")` fail-before result.

Revision note (2026-08-10): recorded the implemented authority, removal of the mutable legacy path, same-process lease hardening, fail-closed `cluster-verify-cn` boundary, and current WIP gate evidence.
