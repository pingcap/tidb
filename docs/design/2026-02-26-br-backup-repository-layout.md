# BR Backup Repository Layout

- Author(s): GPT-5.2-codex, [@YuJuncen](https://github.com/YuJuncen)
- Discussion PR: #66576
- Tracking Issue: TBD

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This document proposes a BR snapshot backup repository layout (“repo-v1”) that allows multiple backups to share a single storage prefix while avoiding “cold/new prefix” issues in large S3-compatible environments. The design keeps data under stable, hot prefixes, adds a repo marker and guard rail, and uses a PD TSO–allocated backup ID in per-backup data prefixes to support safe, prefix-based cleanup even if metadata is lost.

## Motivation or Background

Today, BR snapshot backup expects `--storage` to be an empty path and writes `backupmeta`, `backup.lock`, and all SSTs directly into that path. In very large S3(-compatible) repositories, creating a brand-new prefix and immediately writing a huge number of objects can trigger throttling or timeouts.

We want to:
- Reuse a single `--storage` prefix across many snapshot backups.
- Keep the design simple and compatible with existing BR/TiKV behavior.
- Provide user-friendly listing and cleanup that does not require a full storage scan for routine operations.
- Guarantee each backup ID is PD-assigned even if the snapshot TS is user-specified.

Non-goals (initially):
- Cross-backup deduplication or compaction.
- Advanced repo-level scheduling/locking beyond preventing backup target conflicts.

## Detailed Design

### Repository Layout

Treat `--storage` as a repository root. Reserve `_meta/` for metadata and `_data/` for large data. This avoids touching user data and legacy single-backup roots.

```
s3://bucket/prefix/
  backup.lock
  _meta/repo.json
  _meta/snapshot/<backup-id>/backupmeta
  _meta/snapshot/<backup-id>/backupmeta.schema.000000001
  _meta/snapshot/<backup-id>/backupmeta.datafile.000000001
  _meta/snapshot/<backup-id>/backupmeta.ddl.000000001
  _meta/snapshot/<backup-id>/backupmeta.schema.stats.000000123
  _meta/snapshot/<backup-id>/checkpoints/...
  _data/snapshot/<store-id>/<backup-id>/... (SSTs written by TiKV using legacy naming under this prefix)
```

Reserved entries at the repo root:
- `_meta/repo.json` identifies a BR repo and records layout metadata such as repo version, repo ID, and creator. It must not contain secrets.
- `backup.lock` is a human-readable guard file. It marks the path as BR-managed repository storage and prevents legacy single-backup BR from treating the repo root as an empty backup directory.

Example `_meta/repo.json`:
```json
{
  "repo_version": 1,
  "repo_id": "b8b9c1b6-7d7b-4a8b-8a5b-2f2e9a2f0c2d",
  "created_at": "2026-02-27T09:30:12Z",
  "created_by": "br vX.Y.Z"
}
```

Key points:
- Data remains sharded by `<store-id>/` under `_data/snapshot/`.
- `<store-id>` must stay as the leading data prefix. Putting `<backup-id>` first would recreate a fresh hot prefix for every backup and reintroduce the cold-prefix problem this design is trying to avoid.
- One backup's SSTs are grouped under `_data/snapshot/<store-id>/<backup-id>/`.
- Within that per-backup subprefix, TiKV keeps the legacy SST object naming format.
- Metadata is namespaced per backup under `_meta/snapshot/<backup-id>/`.

### IDs and Naming

Backup ID:
- Allocate a *fresh* PD TSO as the backup ID, even if the user specifies `backup-ts`/`--backupts`.
- The fresh PD TSO gives each backup run a unique object-key namespace. Reusing a user-specified snapshot TS directly would let repeated backups at the same snapshot TS reuse the same prefix and risk SST key collisions or ambiguous cleanup.
- `backup-id` is therefore distinct from `backup-ts`: `backup-ts` selects the MVCC snapshot to read, while `backup-id` identifies this backup instance for naming, listing, and deletion.
- Encode as lower-case hex, zero-padded to 16 characters (`hex16`) so lexical order matches numeric order.

ID formatting rule:
- `<backup-id>` in repo-v1 paths is fixed-width hex (16 chars, lower-case).
- `<store-id>` and the SST object names under `_data/snapshot/<store-id>/<backup-id>/` keep TiKV's legacy backend-specific formatting.

BR prints `<backup-id>` (hex16) on success.

### TiKV SST Object Keys

Current (legacy) object key pattern (existing decimal ids):
- S3/local: `<store-id>/<region-id>_<region-epoch-version>_<start-key-sha256hex>_<unix-millis>`
- Others: `<store-id>_<region-id>_<region-epoch-version>_<start-key-sha256hex>_<unix-millis>`
- Final SST key appends CF suffix: `<base>_<cf>.sst`

Repo v1 changes:
- BR allocates `<backup-id>` and sets `BackupRequest.file_prefix` for each TiKV backup request.
- `file_prefix` is the protocol hook TiKV uses to prepend a client-provided path prefix before the original SST object name.
- Repo-v1 uses that hook to place SSTs under the repo-managed store-first path `_data/snapshot/<store-id>/<backup-id>/`.
- TiKV keeps its existing SST naming logic and writes legacy object names under that client-provided subprefix.
- This keeps `<store-id>` as the leading prefix, keeps the original SST key format as intact as possible, and still isolates each backup under its own deletable subprefix.

Example (S3/local):
```
_data/snapshot/123/000000000000f00d/456789_42_<sha256hex>_1700000000123_default.sst
_data/snapshot/123/000000000000f00d/456789_42_<sha256hex>_1700000000123_write.sst
```

SST format remains RocksDB SST (per CF), with existing compression/encryption behavior.

### User Experience

#### Backup

Command:
- `br backup full -s <repo> --storage-layout=repo-v1 ...`

Semantics:
- `repo-v1` treats `--storage` as a repository root instead of a legacy single-backup directory.
- On first use, creates `_meta/repo.json` and `backup.lock`.
- Creates a new snapshot backup in the repo and allocates a fresh PD TSO `<backup-id>` for this run.
- Writes metadata under `_meta/snapshot/<backup-id>/` and SSTs under `_data/snapshot/<store-id>/<backup-id>/`.
- Prints `<backup-id>` on success.

#### Restore

Command:
- `br restore full -s <repo> --storage-layout=repo-v1 --backup-id <backup-id> ...`

Semantics:
- Restores the snapshot backup identified by `--backup-id` from the repo.

#### List

Command:
- `br repo snapshot list -s <repo>`

Semantics:
- Lists completed snapshot backups in the repo.
- Outputs `<backup-id>`, snapshot time/TS, backup type, and size.

#### Files of a Backup (Print/Delete)

Command:
- `br repo for-files-of-backup -s <repo> --backup=<backup-id> --do={print|delete}`

Semantics:
- Prints or deletes the SST objects that belong to the specified backup.
- When `--do=delete` is used, also removes `_meta/snapshot/<backup-id>/...` if present.
- Still works if per-backup metadata is missing, by enumerating store shards under `_data/snapshot/` and matching the `<backup-id>` subprefix in each shard.
- `--do=print` is the default.

#### Orphans (Print/Delete)

Command:
- `br repo for-orphans -s <repo> --do={print|delete}`

Semantics:
- Prints or deletes SST objects whose `<backup-id>` is not present under `_meta/snapshot/`.
- Can be implemented by comparing `_meta/snapshot/` entries with `<backup-id>` subprefixes found under each store shard in `_data/snapshot/`.
- This is still expected to be more expensive than listing known backups.

### Compatibility

- BR: new `--storage-layout=repo-v1`, `br repo` subcommands, and layout helper.
- TiKV: must support `BackupRequest.file_prefix` for SST output and expose that support via `Backup.feature_gate`.
- PD: backup ID allocation via TSO.
- Upgrade: legacy layout remains supported; repo-v1 is opt-in.
- Downgrade: avoid writing repo-v1 from older BR; repo marker signals layout.
- External tools: restore/list/cleanup must use repo-v1-aware logic.

### Misc

#### TiKV Backup Subprefix Capability Probe (Guard Rail)

BR should detect whether TiKV supports the repo-v1 backup-subprefix behavior before starting a backup:
- BR calls `Backup.feature_gate(FeatureGateRequest)` on all TiKV nodes before starting the backup.
- TiKV reports `FeatureGateResponse.supports_backup_custom_path`.
- If any TiKV returns `supports_backup_custom_path = false`, BR must fail fast in repo-v1 mode and must not send repo-v1 backup requests to a partially-capable cluster.

This prevents partially-upgraded clusters from producing backups that cannot be cleaned up safely.

#### Future: Snapshot + Log in One Repo

This document does not define a combined snapshot + log repository layout yet.

If we extend the repo in the future, one possible direction would be to keep snapshot and log data in separate namespaces, for example:
- Snapshot metadata: `_meta/snapshot/<backup-id>/...`
- Snapshot data: `_data/snapshot/<store-id>/<backup-id>/...`
- Log data: `_data/log/<task-name>/...` (preserve existing TiKV layout)
- Log metadata if needed: `_meta/log/<task-name>/...`

## Test Design

Scope:
- Required coverage below is for repo-v1 behavior and compatibility with existing BR features.

### Functional Tests

Repo-v1 behavior:
- Verify repo marker and `backup.lock` creation.
- Verify backup-id is PD-assigned and hex16 formatted.
- Verify BR sets `BackupRequest.file_prefix` in repo-v1 mode.
- Verify SST objects are written under `_data/snapshot/<store-id>/<backup-id>/` while keeping legacy TiKV naming within that subprefix.
- Verify `<store-id>` remains the leading data prefix and `<backup-id>` is not moved ahead of it.
- Verify BR calls `Backup.feature_gate` and fails fast in repo-v1 mode if any TiKV reports `supports_backup_custom_path = false`.
- Verify `br repo snapshot list` returns completed backups only.
- Verify `for-files-of-backup --do=print` outputs correct keys.
- Verify `for-files-of-backup --do=delete` deletes matching objects and metadata.
- Verify `for-orphans --do=print` outputs only orphan SSTs.
- Verify `for-orphans --do=delete` deletes only orphan SSTs.
- Verify checkpoint artifacts are stored under `_meta/snapshot/<backup-id>/checkpoints/...`.

### Scenario Tests

Repo-v1 scenarios:
- Multiple backups under one repo; restore older/newer backups.
- Delete one backup while keeping others; verify remaining backups restore.
- Metadata loss for a backup: `for-files-of-backup` still deletes by prefix.
- Orphan scan detects unexpected per-backup subprefixes under store shards (simulate interrupted backup).

### Compatibility Tests

Compatibility coverage:
- BR repo-v1 with different storage backends (S3/GCS/Azure/HDFS/local).
- Upgrade from legacy backup usage without repo-v1.
- Restore with TiKV/TiDB version combinations that support `BackupRequest.file_prefix`.
- Compatibility with BR `--use-backupmeta-v2`.
- Compatibility with external stats.

### Benchmark Tests

- Prefix listing cost for `for-files-of-backup` on large repos.
- Listing cost for `for-orphans` over many store shards and per-backup subprefixes, and its impact on API rate limits.

## Impacts & Risks

Impacts:
- Reduces “cold prefix” throttling by reusing stable prefixes.
- Simplifies retention and cleanup via prefix-based operations.

Risks:
- Incorrect prefix matching could delete wrong objects.
- `for-orphans` still requires storage enumeration and can be expensive on very large repos.
- Repo-v1 depends on TiKV support for `BackupRequest.file_prefix`; mixing versions could break cleanup isolation (mitigated by the `feature_gate` probe).

## Investigation & Alternatives

- Per-backup prefix (legacy): simple but reintroduces cold-prefix issues.
- Metadata-driven delete only: fails when metadata is lost.
- Dedup/compaction: higher complexity, out of scope initially.

## Unresolved Questions

None for now.
