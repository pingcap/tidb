# BR Backup Repository Layout

- Author(s): GPT-5.{2,3,4}, [@YuJuncen](https://github.com/YuJuncen)
- Discussion PR: #66576
- Tracking Issue: #67964

## Table of Contents

- [BR Backup Repository Layout](#br-backup-repository-layout)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Motivation or Background](#motivation-or-background)
  - [Detailed Design](#detailed-design)
    - [Repository Layout](#repository-layout)
    - [IDs and Naming](#ids-and-naming)
    - [Backup Lifecycle](#backup-lifecycle)
    - [Checkpoints](#checkpoints)
    - [TiKV SST Object Keys](#tikv-sst-object-keys)
    - [User Experience](#user-experience)
      - [Backup](#backup)
      - [Controller-Friendly Retry Semantics](#controller-friendly-retry-semantics)
      - [Restore](#restore)
      - [Discard Pending Backup](#discard-pending-backup)
      - [List](#list)
      - [Backupmeta of a Backup](#backupmeta-of-a-backup)
      - [Orphans](#orphans)
    - [Compatibility](#compatibility)
    - [Misc](#misc)
      - [Backend Compatibility of Prefix Rewriting](#backend-compatibility-of-prefix-rewriting)
      - [Future: Snapshot + Log in One Repo](#future-snapshot--log-in-one-repo)
  - [Test Design](#test-design)
    - [Functional Tests](#functional-tests)
    - [Scenario Tests](#scenario-tests)
    - [Compatibility Tests](#compatibility-tests)
    - [Benchmark Tests](#benchmark-tests)
  - [Impacts \& Risks](#impacts--risks)
  - [Investigation \& Alternatives](#investigation--alternatives)
  - [Unresolved Questions](#unresolved-questions)

## Introduction

This document proposes a BR snapshot backup repository layout (“repo”) that allows multiple backups to share a single storage prefix while avoiding “cold/new prefix” issues in large S3-compatible environments. The design keeps data under stable, hot prefixes, adds a repo marker and guard rail, and uses a PD TSO–allocated backup ID in per-backup data prefixes to support safe, prefix-based cleanup even if metadata is lost.

## Motivation or Background

Today, BR snapshot backup expects `--storage` to be an empty path and writes `backupmeta`, `backup.lock`, and all SSTs directly into that path. In very large S3(-compatible) repositories, creating a brand-new prefix and immediately writing a huge number of objects can trigger throttling or timeouts.

We want to:
- Reuse a single `--storage` prefix across many snapshot backups.
- Keep the design simple and compatible with existing BR/TiKV behavior.
- Provide user-friendly listing and cleanup that does not require a full storage scan for routine operations.
- Provide a cheap way to find or discard one unfinished backup without scanning all historical backups in the repo.
- Guarantee each backup ID is PD-assigned even if the snapshot TS is user-specified.

Non-goals (initially):
- Cross-backup deduplication or compaction.
- Advanced repo-level scheduling/locking beyond preventing backup target conflicts.

## Detailed Design

### Repository Layout

Treat `--storage` as a repository root. Reserve `_meta/` for metadata and `_data/` for large data. This avoids touching user data and legacy single-backup roots.

```text
s3://bucket/prefix/
  backup.lock
  _meta/repo.json
  _meta/pending/<config-hash-hex>/<backup-id>.json
  _meta/snapshot/<backup-id>/backupmeta[.XXXXXXX]
  _meta/snapshot/<backup-id>/checkpoints/...
  _data/snapshot/<store-id>/<backup-id>/... (SSTs written by TiKV using legacy naming under this prefix)
```

Reserved entries at the repo root:
- `_meta/repo.json` identifies a BR repo and records layout metadata such as repo version, repo ID, and creator. It must not contain secrets.
- `_meta/pending/` stores repo-level pointers to unfinished snapshot backups. It is the fast-path index for resume, fresh-attempt, or discard decisions and avoids scanning all historical snapshots in `_meta/snapshot/`.
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
- Expose `backup-id` to users as a `uint64`.
- Use fixed-width upper-case hex, zero-padded to 16 characters (`%016X`), only when naming external-storage paths so lexical order matches numeric order.

ID formatting rule:
- `<backup-id>` in repo paths is fixed-width upper-case hex (16 chars).
- `<store-id>` and the SST object names under `_data/snapshot/<store-id>/<backup-id>/` keep TiKV's legacy backend-specific formatting.

BR prints the user-facing `<backup-id>` (`uint64`) on success.

### Backup Lifecycle

Repo models each snapshot backup as one per-backup namespace rooted at `_meta/snapshot/<backup-id>/...`.

Lifecycle:
- A new repo backup begins by selecting one per-backup namespace: with `--use-checkpoint=true`, BR resumes exactly one matching unfinished backup when present and allocates a fresh `<backup-id>` when none exists; with `--use-checkpoint=false`, BR always allocates a fresh `<backup-id>`. The selected namespace stores metadata under `_meta/snapshot/<backup-id>/...` and data under `_data/snapshot/<store-id>/<backup-id>/...`.
- While the backup is running, checkpoint artifacts and temporary metadata accumulate under that per-backup metadata namespace. This namespace is the canonical record of the backup's in-progress state.
- The backup remains unfinished until its final `backupmeta` is durable. Checkpoint cleanup is part of the success path, not a separate logical backup.
- Once the final `backupmeta` is durable and checkpoint cleanup has finished, the backup is complete and becomes a normal historical snapshot in the repo.
- If execution is interrupted before completion, the partially written per-backup metadata and data remain attributable to the same `<backup-id>`, so later resume, inspection, or cleanup can target one concrete backup attempt.
- Current implementation classifies pending state by probing this namespace: final `backupmeta` means a stale pending marker, checkpoint metadata without final `backupmeta` means an unfinished resumable backup, and marker-only leftovers are treated as stale cleanup leftovers rather than as resumable state.

### Checkpoints

Config hash:
- Repo groups unfinished backups by the same logical backup identity using BR's existing backup checkpoint config hash.
- The hash input is the same immutable backup configuration used by today's checkpoint matching logic.
- The on-storage directory name is the full SHA-256 digest encoded as 64 upper-case hexadecimal characters.
- This RFC refers to that value as `<config-hash-hex>`.

Pending index:
- For unfinished backups, BR also keeps a small pointer file at `_meta/pending/<config-hash-hex>/<backup-id>.json`.
- This lets routine startup find relevant unfinished attempts by enumerating `_meta/pending/<config-hash-hex>/` instead of scanning all historical backups.
- Current implementation stores checkpoint state under `_meta/snapshot/<backup-id>/checkpoints/backup/...` and classifies each pending marker by checking `backupmeta` first, then `checkpoints/backup/checkpoint.meta`.
- Stale pending pointers left behind after a completed backup are treated as cleanup leftovers and removed automatically when BR scans that config-hash directory.
- Marker-only pending leftovers are also treated as stale cleanup leftovers: when BR scans that config-hash directory, it removes the marker and any checkpoint debris instead of surfacing a special error.

This keeps “find unfinished backup” cost proportional to the number of unfinished backups, not the number of historical backups in the repo.

### TiKV SST Object Keys

Current (legacy) object key pattern (existing decimal ids):
- S3/local: `<store-id>/<region-id>_<region-epoch-version>_<start-key-sha256hex>_<unix-millis>`
- Others: `<store-id>_<region-id>_<region-epoch-version>_<start-key-sha256hex>_<unix-millis>`
- Final SST key appends CF suffix: `<base>_<cf>.sst`

Repo v1 changes:
- Repo's user-visible requirement is that SSTs land under `_data/snapshot/<store-id>/<backup-id>/`.
- The baseline implementation path is for BR to rewrite the per-store `BackupRequest.StorageBackend` prefix so that each TiKV writes into that store-specific repo location.
- Because the path includes `<store-id>`, the effective target prefix must be set on the per-store backup request, after the target store is known, rather than once on a single shared request template.
- Under this baseline path, TiKV keeps its existing SST naming logic and writes legacy object names relative to the rewritten backend prefix.
- `<store-id>` remains the leading prefix, the original SST key format stays as intact as possible, and each backup is isolated under its own deletable subprefix.

Example (S3/local):
```
_data/snapshot/123/000000000000F00D/456789_42_<sha256hex>_1700000000123_default.sst
_data/snapshot/123/000000000000F00D/456789_42_<sha256hex>_1700000000123_write.sst
```

SST format remains RocksDB SST (per CF), with existing compression/encryption behavior.

### User Experience

#### Backup

Command:
- `br backup full|db|table -s <repo> --storage-layout=repo ...`

Semantics:
- `repo` treats `--storage` as a repository root instead of a legacy single-backup directory.
- On first use, creates `_meta/repo.json` and `backup.lock`.
- Creates a snapshot backup in the repo and selects a PD TSO `<backup-id>` for this run or resumed run.
- With checkpoint mode enabled, computes `<config-hash-hex>` for the current logical backup configuration and creates `_meta/pending/<config-hash-hex>/<backup-id>.json` before issuing TiKV backup requests.
- Writes metadata under `_meta/snapshot/<backup-id>/` and SSTs under `_data/snapshot/<store-id>/<backup-id>/`.
- With checkpoint mode enabled, removes `_meta/pending/<config-hash-hex>/<backup-id>.json` on the success path after final `backupmeta` is durable and checkpoint artifacts are cleaned up.
- Prints `<backup-id>` on success.

Pending backup behavior:
- Current implementation cleans stale pending markers for the same config hash before selecting the backup attempt.
- `--use-checkpoint=true` is the default and acts as the resume policy. If no matching unfinished backup exists, the command starts a new backup. If exactly one matching unfinished backup with checkpoint metadata exists, the command resumes it. If multiple matching unfinished backups exist, the command fails with an ambiguity error.
- `--use-checkpoint=false` acts as the fresh-attempt policy. It always allocates a fresh `<backup-id>` and starts a new backup instead of reusing a matching unfinished backup. Existing unfinished backups are left in place for later inspection or explicit discard. Because checkpoint mode is disabled, this fresh attempt does not create checkpoint or pending-marker state for itself.

#### Controller-Friendly Retry Semantics

Kubernetes-style controllers need declarative, idempotent, non-interactive retry behavior. Repo backup uses the existing checkpoint switch as that retry intent instead of adding a second BR-specific retry API surface.

Suggested controller mapping:
- Automatic retry of the same backup CR should invoke BR with `--use-checkpoint=true` (the default), so one matching unfinished repo backup is continued in place.
- A fresh controller-visible attempt should invoke BR with `--use-checkpoint=false`, so BR allocates a new `<backup-id>` instead of reusing the previous unfinished backup. If that non-checkpointed fresh attempt is retried, the controller should keep using `--use-checkpoint=false` for the same fresh-attempt intent.
- Multiple pending backups under the current config-hash directory still produce an explicit ambiguity error when checkpoint mode is enabled.
- Pending backups under other config-hash directories are unrelated attempts and should not block this one.

This keeps the repo state machine explicit while letting Operator express retry intent with BR's existing checkpoint flag rather than a separate resume/discard protocol.

#### Restore

Command:
- `br restore full|db|table -s <repo> --storage-layout=repo --backup-id <backup-id> ...`
- `br restore point --full-backup-storage <repo> --storage-layout=repo --backup-id <backup-id> ...`

Semantics:
- Restores the snapshot backup identified by the user-facing `backup-id` (`uint64`) from the repo.
- Repo readers resolve metadata from `_meta/snapshot/<backup-id>/backupmeta[.XXXXXXX]` while still using the repo root backend for data files.
- Current implementation validates WalkDir `StartAfter` support for repo snapshot references, so repo restore only works on storage backends that satisfy that capability gate.
- Other snapshot readers such as `br operator checksum-as -s <repo> --storage-layout=repo --backup-id <backup-id>` resolve metadata from the same per-backup namespace.

#### Discard Pending Backup

Command:
- `br repo snapshot pending discard -s <repo> [--backup-id <backup-id>]`

Semantics:
- Discards one repo pending snapshot entry and, for unfinished backups, frees the repo to start a new checkpointed backup.
- If there is exactly one pending backup, `--backup-id` may be omitted.
- If multiple pending backups exist, `--backup-id` is required.
- Current implementation first classifies the target as `stale` or `unfinished`.
- For `stale`, it removes pending markers and leftover checkpoint files only; completed snapshot metadata and data files, if present, are kept.
- For `unfinished`, it removes `_meta/pending/<config-hash-hex>/<backup-id>.json`, per-backup metadata under `_meta/snapshot/<backup-id>/...`, and partial SST data under `_data/snapshot/<store-id>/<backup-id>/...`.
- Discarding unfinished data currently relies on WalkDir `StartAfter` support on the underlying storage.

#### List

Command:
- `br repo snapshot list -s <repo>`

Semantics:
- Lists completed snapshot backups in the repo.
- Outputs the user-facing `backup-id` (`uint64`), physical time of this backup id.
- Current implementation identifies completed backups by scanning `_meta/snapshot/` for `backupmeta[.XXXXXXX]` path names and does not read backupmeta contents.

#### Backupmeta of a Backup

Command:
- `br repo snapshot get -s <repo> --backup-id <backup-id> --view <...>`
- `br repo snapshot delete -s <repo> --backup-id <backup-id>`

Semantics:
- `get` supports `--view basic|tables|files`.
- `get --view basic` prints a summary JSON object derived from `backupmeta`.
- `get --view tables` streams one JSON object per backed-up table and is unavailable for raw backups.
- `get --view files` streams one JSON object per SST/file entry recorded in `backupmeta`.
- `delete` removes the specified backup's snapshot metadata, snapshot data, and any pending markers for that `<backup-id>`.
- `delete` still works if per-backup metadata is missing, by enumerating store shards under `_data/snapshot/` and matching the upper-case hex `<backup-id>` subprefix in each shard. This data scan currently requires WalkDir `StartAfter` support.

#### Orphans

Command:
- `br repo snapshot orphans list -s <repo>`
- `br repo snapshot orphans delete -s <repo>`

Semantics:
- `orphans list` prints SST objects whose `<backup-id>` is not present as a completed snapshot metadata entry under `_meta/snapshot/<backup-id>/backupmeta[.XXXXXXX]`.
- `orphans delete` deletes SST objects whose `<backup-id>` is not present as a completed snapshot metadata entry.
- Current implementation finds orphans by comparing completed snapshot IDs with upper-case hex `<backup-id>` subprefixes found under each store shard in `_data/snapshot/`.
- Unfinished backups without final `backupmeta` therefore look like orphans to this command family; `pending discard` is the more targeted cleanup path when pending markers still exist.
- These commands currently require WalkDir `StartAfter` support.
- This is still expected to be more expensive than listing known backups.

### Compatibility

- BR: new `--storage-layout=repo`, `br repo` subcommands, and layout helper.
- TiKV: repo does not require a new TiKV-side path hook. The baseline deployment path relies on BR rewriting the per-request `StorageBackend` prefix per store.
- PD: backup ID allocation via TSO.
- Current implementation rejects repo snapshot backup on HDFS and noop storage.
- Current implementation recognizes WalkDir `StartAfter` support only for `s3://`, `ks3://`, `gcs://`, and `file://` storages. Repo restore and the data-scanning repo admin paths (`snapshot delete`, unfinished `pending discard`, `orphans list/delete`) are therefore limited to those storages today.
- Upgrade: legacy layout remains supported; repo is opt-in.
- Downgrade: avoid writing repo from older BR; repo marker signals layout.
- External tools: restore/list/delete/orphan-cleanup/pending-discard must use repo-aware logic.

### Misc

#### Backend Compatibility of Prefix Rewriting

The backend-prefix-rewrite compatibility path is not equally suitable for all storage backends.

Current implementation splits compatibility into two layers:
- The backup write path can rewrite per-store prefixes for local, S3/KS3-compatible, GCS, and Azure Blob Storage backends.
- The repo snapshot-reference and data-scanning paths additionally require WalkDir `StartAfter` support from the resolved storage implementation. Today that gate is recognized only for `s3://`, `ks3://`, `gcs://`, and `file://` storages.

Not a good fit today:
- HDFS, because BR's current HDFS storage support is limited and does not provide the full metadata/checkpoint/list/delete capabilities that repo relies on for snapshot backup management.
- Noop storage, because it is not a real persistence target and already disables checkpoint-oriented behavior.
- Azure Blob Storage for repo restore/admin flows, because the current WalkDir `StartAfter` capability gate does not admit Azure-backed snapshot references even though the write-path prefix rewrite is implemented.

Therefore, the current practical scope is narrower than the pure prefix-rewrite design: repo backup writing reaches more backends than repo restore and data-scanning admin commands do.

#### Future: Snapshot + Log in One Repo

This document does not define a combined snapshot + log repository layout yet.

If we extend the repo in the future, one possible direction would be to keep snapshot and log data in separate namespaces, for example:
- Snapshot metadata: `_meta/snapshot/<backup-id>/...`
- Snapshot data: `_data/snapshot/<store-id>/<backup-id>/...`
- Log data: `_data/log/<task-name>/...` (preserve existing TiKV layout)
- Log metadata if needed: `_meta/log/<task-name>/...`

## Test Design

Scope:
- Required coverage below is for repo behavior and compatibility with existing BR features.

### Functional Tests

Repo behavior:
- Verify repo marker and `backup.lock` creation.
- Verify checkpointed backup creates a pending pointer at `_meta/pending/<config-hash-hex>/<backup-id>.json`.
- Verify backup-id is PD-assigned as a user-facing `uint64`.
- Verify the on-storage `<backup-id>` path segment is fixed-width upper-case hex.
- Verify the baseline implementation path can produce `_data/snapshot/<store-id>/<backup-id>/...` by rewriting the per-store `StorageBackend` prefix.
- Verify the rewritten backend prefix preserves `<store-id>` as the leading path component.
- Verify SST objects are written under `_data/snapshot/<store-id>/<backup-id>/` while keeping legacy TiKV naming within that subprefix.
- Verify `<store-id>` remains the leading data prefix and `<backup-id>` is not moved ahead of it.
- Verify `--use-checkpoint=true` resumes exactly one matching unfinished backup and rejects multiple matching unfinished backups as ambiguous.
- Verify `--use-checkpoint=false` starts a fresh backup and leaves existing unfinished backups for later inspection or explicit discard.
- Verify `br repo snapshot list` returns completed backups only.
- Verify unfinished-backup lookup only enumerates `_meta/pending/<config-hash-hex>/` in the normal path.
- Verify a stale pending file for a completed backup is removed instead of forcing a resume.
- Verify marker-only pending leftovers are treated as stale cleanup leftovers rather than as resumable backups.
- Verify `pending discard` distinguishes stale vs unfinished targets and deletes the correct scope for each.
- Verify `snapshot get --view basic|tables|files` returns the documented metadata views.
- Verify `snapshot delete` deletes matching data, metadata, and pending markers, and still works by data-prefix scan when metadata is missing.
- Verify `snapshot orphans list` outputs only orphan SSTs.
- Verify `snapshot orphans delete` deletes only orphan SSTs.
- Verify checkpoint artifacts are stored under `_meta/snapshot/<backup-id>/checkpoints/backup/...`.
- Verify repo reader and destructive admin paths enforce the current WalkDir `StartAfter` capability gate.

### Scenario Tests

Repo scenarios:
- Multiple backups under one repo; restore older/newer backups.
- Delete one backup while keeping others; verify remaining backups restore.
- One failed backup is discarded, then a new checkpointed backup can start in the same repo.
- Multiple pending entries under the same config-hash directory exist due to manual corruption or partial cleanup; checkpointed backup start fails with an explicit ambiguity error.
- Multiple unfinished backups with different config-hash directories coexist in the same repo; each new backup only matches or resumes the entries under its own config-hash directory.
- A controller retries the same failed backup CR through its existing retry mechanism and invokes BR with `--use-checkpoint=true`; the unfinished backup continues instead of being replaced.
- A controller observes a fresh-attempt signal, such as CR recreation or changed retry annotation/token, and invokes BR with `--use-checkpoint=false`; a new backup starts without resuming the old unfinished backup.
- Metadata loss for a backup: `snapshot delete` still deletes by prefix.
- Orphan scan detects unexpected per-backup subprefixes under store shards, including unfinished backups that have data files but no final `backupmeta`.

### Compatibility Tests

Compatibility coverage:
- BR repo backup write path with supported and rejected backends (S3/GCS/Azure/local supported; HDFS and noop rejected).
- Upgrade from legacy backup usage without repo.
- Restore and cleanup behavior when repo SST placement is achieved through per-request backend-prefix rewriting.
- Backend-by-backend validation of the prefix-rewrite compatibility path on S3/KS3-compatible/GCS/Azure/local, with explicit exclusion or documented limitation for HDFS and noop.
- Backend-by-backend validation of the current WalkDir `StartAfter` gate on repo restore/admin flows (`s3://`, `ks3://`, `gcs://`, `file://` supported; Azure currently excluded).
- Compatibility with BR `--use-backupmeta-v2`.
- Compatibility with external stats.

### Benchmark Tests

- Prefix scan cost for `snapshot delete` when metadata is missing and BR must enumerate per-store backup subprefixes.
- Listing cost for `snapshot orphans list` over many store shards and per-backup subprefixes, and its impact on API rate limits.

## Impacts & Risks

Impacts:
- Reduces “cold prefix” throttling by reusing stable prefixes.
- Simplifies retention and cleanup via prefix-based operations.

Risks:
- Incorrect prefix matching could delete wrong objects.
- `snapshot orphans list/delete` still requires storage enumeration and can be expensive on very large repos.
- Incorrect handling of pending markers could either block future backups unnecessarily or discard data from the wrong unfinished backup.
- The baseline backend-prefix-rewrite path relies on backend-specific prefix semantics and correct per-store request construction; mistakes there could place SSTs under the wrong prefix.
- The backend-prefix-rewrite compatibility path may not be supportable on every BR backend; claiming universal support would overstate what current HDFS/noop implementations can do.
- If controllers choose the wrong `--use-checkpoint` value for a retry, operators may see unexpected fresh attempts, unintended resumes, or ambiguity failures after abandoned attempts accumulate.

## Investigation & Alternatives

- Per-backup prefix (legacy): simple but reintroduces cold-prefix issues.
- Metadata-driven delete only: fails when metadata is lost.
- Finding unfinished backups by scanning `_meta/snapshot/*`: simple, but startup cost grows with total historical backups instead of active unfinished backups.
- Pending index keyed only by `<backup-id>`: simpler naming, but requires scanning all unfinished backups in the repo instead of directly narrowing to one logical backup configuration.
- Repo via per-request backend-prefix rewriting: baseline deployment path that avoids a hard dependency on TiKV upgrade, but shifts correctness risk to backend-prefix handling and per-store request mutation.
- Future TiKV-side path hook such as `file_prefix`: could be added later as an implementation optimization without changing the repo storage layout or user-facing semantics.
- Dedup/compaction: higher complexity, out of scope initially.

## Unresolved Questions

None for now.
