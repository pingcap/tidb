# TiDB Design Documents

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

This document proposes a BR snapshot backup repository layout (“repo-v1”) that allows multiple backups to share a single storage prefix while avoiding “cold/new prefix” issues in large S3-compatible environments. The design keeps data under stable, hot prefixes, adds a repo marker and guard rail, and uses a PD TSO–allocated backup ID in object keys to support safe, prefix-based cleanup even if metadata is lost.

## Motivation or Background

Today, BR snapshot backup expects `--storage` to be an empty path and writes `backupmeta`, `backup.lock`, and all SSTs directly into that path. In very large S3(-compatible) repositories, creating a brand-new prefix and immediately writing a huge number of objects can trigger throttling or timeouts.

We want to:
- Reuse a single `--storage` prefix across many snapshot backups.
- Keep the design simple and compatible with existing BR/TiKV behavior.
- Provide user-friendly listing and cleanup that does not require a full storage scan for routine operations.
- Guarantee each backup ID is PD-assigned even if the snapshot TS is user-specified.

Non-goals (initially):
- Cross-backup deduplication or compaction.
- Advanced repo-level scheduling/locking beyond preventing object key collisions.

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
  _meta/snapshot/<backup-id>/checkpoints/... (optional, for checkpoint mode)
  _data/snapshot/<store-id>/<backup-id>/... (SSTs written by TiKV)
```

Key points:
- Data is still sharded by `<store-id>/` under `_data/snapshot/`.
- `<backup-id>` is placed immediately after `<store-id>` in SST object keys to enable prefix-based deletion without a full scan.
- Metadata is namespaced per backup under `_meta/snapshot/<backup-id>/`.

### TiKV Capability Probe (Guard Rail)

BR should detect whether TiKV supports the repo-v1 object key format before starting a backup:
- A capability flag (to be added) is reported by TiKV; BR checks all TiKV nodes.
- If any TiKV does not support repo-v1 naming (i.e. does not place `unique_id` immediately after `<store-id>`), BR must fail fast in repo-v1 mode.

This prevents partially-upgraded clusters from producing backups that cannot be cleaned up safely.

### IDs and Naming

Backup ID:
- Allocate a *fresh* PD TSO as the backup ID.
- Encode as lower-case hex, zero-padded to 16 characters (`hex16`) so lexical order matches numeric order.

ID formatting rule:
- `<backup-id>` and `<store-id>` in paths are fixed-width hex (16 chars, lower-case).

BR prints `<backup-id>` (hex16) on success.

### TiKV SST Object Keys

Current (legacy) object key pattern (existing decimal ids):
- S3/local: `<store-id>/<region-id>_<region-epoch-version>_<start-key-sha256hex>_<unix-millis>`
- Others: `<store-id>_<region-id>_<region-epoch-version>_<start-key-sha256hex>_<unix-millis>`
- Final SST key appends CF suffix: `<base>_<cf>.sst`

Repo v1 changes:
- BR sets `BackupRequest.unique_id = <backup-id>`.
- TiKV inserts `unique_id` immediately after `<store-id>` (before region/range fields).
- This enables prefix-based deletion per backup and prevents key collisions.

Repo v1 base name (hex IDs, fixed width):
- S3/local: `<store-id-hex>/<backup-id-hex>_<region-id>_<region-epoch-version>_<start-key-sha256hex>_<unix-millis>`
- Others: `<store-id-hex>_<backup-id-hex>_<region-id>_<region-epoch-version>_<start-key-sha256hex>_<unix-millis>`

Example (S3/local):
```
000000000000007b/000000000000f00d_456789_42_<sha256hex>_1700000000123_default.sst
000000000000007b/000000000000f00d_456789_42_<sha256hex>_1700000000123_write.sst
```

SST format remains RocksDB SST (per CF), with existing compression/encryption behavior.

### Repo Marker and Guard Rail

`_meta/repo.json`:
- Identifies a BR repo and records layout + reserved prefixes.
- Must not contain secrets.

Example:
```json
{
  "repo_version": 1,
  "repo_id": "b8b9c1b6-7d7b-4a8b-8a5b-2f2e9a2f0c2d",
  "created_at": "2026-02-27T09:30:12Z",
  "created_by": "br vX.Y.Z"
}
```

`backup.lock` at repo root:
- A human-readable guard rail to prevent accidental legacy `br backup` into the repo root.
- Legacy backup should treat this as a hard error even when the repo is otherwise empty.

### User Experience

Activate repo layout:
- `br backup full -s <repo> --storage-layout=repo-v1 ...`
- Creates `_meta/repo.json` and `backup.lock` if absent.

Backup:
- Allocate PD TSO `<backup-id>`.
- Pass `<backup-id>` to TiKV as `BackupRequest.unique_id`.
- Write metadata to `_meta/snapshot/<backup-id>/`.
- Write SSTs under `_data/snapshot/`.

Restore:
- `br restore full -s <repo> --storage-layout=repo-v1 --backup-id <backup-id> ...`

List:
- `br repo snapshot list -s <repo>`
- Output `<backup-id>`, snapshot time/TS, backup type, size (completed only).

### Files of a Backup (Print/Delete)

Command:
- `br repo for-files-of-backup -s <repo> --backup=<backup-id> --do={print|delete}`

Operation:
- Find objects whose key starts with:
  - S3/local: `<store-id-hex>/<backup-id-hex>_`
  - Others: `<store-id-hex>_<backup-id-hex>_`
- Ensure the key ends with a CF suffix (`_default.sst`, `_write.sst`, or raw-kv CFs).
- If metadata exists, use it to obtain store IDs; otherwise list immediate children under `_data/snapshot/` to discover store shards.
- `--do=print` outputs matched object keys. (The default.)
- `--do=delete` deletes those objects and then removes `_meta/snapshot/<backup-id>/...` if present.

### Orphans (Print/Delete)

Command:
- `br repo for-orphans -s <repo> --do={print|delete}`

Operation:
- Enumerate all known backup IDs from `_meta/snapshot/`.
- Scan SST objects under `_data/snapshot/` and parse `<backup-id>` from keys.
- Output (or delete) SSTs whose `<backup-id>` is not in the known set.

Note: this is a full scan of SST objects and can be expensive for large repos.

### Future: Snapshot + Log in One Repo

Suggested namespace:
- Snapshot metadata: `_meta/snapshot/<backup-id>/...`
- Snapshot data: `_data/snapshot/<store-id>/...`
- Log data: `_data/log/<task-name>/...` (preserve existing TiKV layout)
- Optional log metadata: `_meta/log/<task-name>/...`

### Compatibility

- BR: new `--storage-layout=repo-v1`, `br repo` subcommands, and layout helper.
- TiKV: SST key naming must include `unique_id` immediately after `<store-id>` and report repo-v1 capability.
- PD: backup ID allocation via TSO.
- Upgrade: legacy layout remains supported; repo-v1 is opt-in.
- Downgrade: avoid writing repo-v1 from older BR; repo marker signals layout.
- External tools: restore/list/cleanup must use repo-v1-aware logic.

## Test Design

### Functional Tests

- Verify repo marker and `backup.lock` creation.
- Verify backup-id is PD-assigned and hex16 formatted.
- Verify SST object key naming inserts `<backup-id>` after `<store-id>`.
- Verify BR fails fast in repo-v1 mode if any TiKV lacks the capability flag.
- Verify `br repo snapshot list` returns completed backups only.
- Verify `for-files-of-backup --do=print` outputs correct keys.
- Verify `for-files-of-backup --do=delete` deletes matching objects and metadata.

### Scenario Tests

- Multiple backups under one repo; restore older/newer backups.
- Delete one backup while keeping others; verify remaining backups restore.
- Metadata loss for a backup: `for-files-of-backup` still deletes by prefix.
- Orphan scan detects unexpected SSTs (simulate interrupted backup).

### Compatibility Tests

- BR repo-v1 with different storage backends (S3/GCS/Azure/HDFS/local).
- Compatibility with BR `--use-backupmeta-v2` and external stats.
- Upgrade from legacy backup usage without repo-v1.
- Restore with TiKV/TiDB version combinations that support repo-v1 naming.

### Benchmark Tests

- Prefix listing cost for `for-files-of-backup` on large repos.
- Full scan cost for `for-orphans` and its impact on API rate limits.

## Impacts & Risks

Impacts:
- Reduces “cold prefix” throttling by reusing stable prefixes.
- Simplifies retention and cleanup via prefix-based operations.

Risks:
- Incorrect key parsing could delete wrong objects.
- `for-orphans` requires full scans and can be expensive.
- Repo-v1 depends on TiKV naming changes; mixing versions could break cleanup (mitigated by capability probe).

## Investigation & Alternatives

- Per-backup prefix (legacy): simple but reintroduces cold-prefix issues.
- Metadata-driven delete only: fails when metadata is lost.
- Dedup/compaction: higher complexity, out of scope initially.

## Unresolved Questions

None for now.
