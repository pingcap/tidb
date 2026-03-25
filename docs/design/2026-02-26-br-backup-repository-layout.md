# BR Backup Repository Layout

- Author(s): ChatGPT, [@YuJuncen](https://github.com/YuJuncen)
- Discussion PR: #66576
- Tracking Issue: TBD

## Table of Contents

- [BR Backup Repository Layout](#br-backup-repository-layout)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Motivation or Background](#motivation-or-background)
  - [Detailed Design](#detailed-design)
    - [Repository Layout](#repository-layout)
    - [IDs and Naming](#ids-and-naming)
    - [Pending Backup Index and Lifecycle](#pending-backup-index-and-lifecycle)
    - [TiKV SST Object Keys](#tikv-sst-object-keys)
    - [User Experience](#user-experience)
      - [Backup](#backup)
      - [Controller-Friendly Retry Semantics](#controller-friendly-retry-semantics)
      - [Restore](#restore)
      - [Discard Pending Backup](#discard-pending-backup)
      - [List](#list)
      - [Files of a Backup](#files-of-a-backup)
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

This document proposes a BR snapshot backup repository layout (“repo-v1”) that allows multiple backups to share a single storage prefix while avoiding “cold/new prefix” issues in large S3-compatible environments. The design keeps data under stable, hot prefixes, adds a repo marker and guard rail, and uses a PD TSO–allocated backup ID in per-backup data prefixes to support safe, prefix-based cleanup even if metadata is lost.

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

```
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
- `_meta/pending/` stores repo-level pointers to unfinished snapshot backups. It is the fast-path index for resume or discard decisions and avoids scanning all historical snapshots in `_meta/snapshot/`.
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
- `<backup-id>` in repo-v1 paths is fixed-width upper-case hex (16 chars).
- `<store-id>` and the SST object names under `_data/snapshot/<store-id>/<backup-id>/` keep TiKV's legacy backend-specific formatting.

BR prints the user-facing `<backup-id>` (`uint64`) on success.

### Pending Backup Index and Lifecycle

Repo-v1 treats an unfinished snapshot backup as an explicit repo object rather than an implicit conclusion from scanning all historical metadata.

Config hash format:
- Repo-v1 groups unfinished backups by the same logical backup identity using BR's existing backup checkpoint config hash.
- The hash input is the same immutable backup configuration used by today's checkpoint matching logic.
- The on-storage directory name is the full SHA-256 digest encoded as 64 lowercase hexadecimal characters.
- This RFC refers to that value as `<config-hash-hex>`.

Pending index:
- Each unfinished snapshot backup has a pointer file at `_meta/pending/<config-hash-hex>/<backup-id>.json`.
- The directory is the grouping key for one logical backup configuration, and the filename identifies a specific attempt of that configuration.
- The file body stores minimal metadata such as layout version, metadata prefix, and creation time. It may repeat the config hash for debugging, but lookup should rely on the path first.
- BR creates the pending file before sending snapshot backup requests to TiKV.
- BR removes the pending file only after the final per-backup `backupmeta` is durable and checkpoint cleanup has completed.

Lookup rules:
- Routine backup startup computes the current `<config-hash-hex>` and enumerates only `_meta/pending/<config-hash-hex>/`, not `_meta/snapshot/*`.
- If there is no pending entry under that config-hash directory, BR allocates a fresh `<backup-id>` and starts a new backup.
- If there is exactly one pending entry under that config-hash directory, BR can validate or resume that specific backup without scanning the whole repo.
- If there are multiple pending entries under that config-hash directory, BR must fail and require the operator to choose one explicitly.
- Pending entries under other config-hash directories must not block this backup; they represent different logical backup tasks in the same repo.

State interpretation:
- `pending exists` + per-backup checkpoint exists + final `backupmeta` absent: unfinished backup, eligible for resume.
- `pending exists` + final `backupmeta` exists: backup already finished but pending cleanup was incomplete; BR should remove the stale pending entry instead of resuming.
- `pending exists` + neither checkpoint nor final `backupmeta` exists: inconsistent state; BR must report an operator-visible error instead of guessing.

This keeps “find unfinished backup” cost proportional to the number of unfinished backups, not the number of historical backups in the repo.

### TiKV SST Object Keys

Current (legacy) object key pattern (existing decimal ids):
- S3/local: `<store-id>/<region-id>_<region-epoch-version>_<start-key-sha256hex>_<unix-millis>`
- Others: `<store-id>_<region-id>_<region-epoch-version>_<start-key-sha256hex>_<unix-millis>`
- Final SST key appends CF suffix: `<base>_<cf>.sst`

Repo v1 changes:
- Repo-v1's user-visible requirement is that SSTs land under `_data/snapshot/<store-id>/<backup-id>/`.
- The baseline implementation path is for BR to rewrite the per-store `BackupRequest.StorageBackend` prefix so that each TiKV writes into that store-specific repo-v1 location.
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
- `br backup full -s <repo> --storage-layout=repo-v1 ...`

Semantics:
- `repo-v1` treats `--storage` as a repository root instead of a legacy single-backup directory.
- On first use, creates `_meta/repo.json` and `backup.lock`.
- Creates a new snapshot backup in the repo and allocates a fresh PD TSO `<backup-id>` for this run.
- Computes `<config-hash-hex>` for the current logical backup configuration and creates `_meta/pending/<config-hash-hex>/<backup-id>.json` before issuing TiKV backup requests.
- Writes metadata under `_meta/snapshot/<backup-id>/` and SSTs under `_data/snapshot/<store-id>/<backup-id>/`.
- Removes `_meta/pending/<config-hash-hex>/<backup-id>.json` after the backup finishes successfully and its checkpoint artifacts are cleaned up.
- Prints `<backup-id>` on success.

Pending backup behavior:
- If no pending backup exists, the command starts a new backup.
- If one pending backup exists for the same logical backup configuration, BR should not silently guess whether the user wants to continue it or abandon it. The command should require an explicit choice, such as a resume flag or a separate discard command.
- If multiple pending backups exist for the same logical backup configuration, the command must fail and require the operator to resolve the ambiguity first.

#### Controller-Friendly Retry Semantics

The interactive CLI semantics above are not sufficient for Kubernetes-style controllers, which need declarative, idempotent, non-interactive behavior. This RFC should stay close to existing controller designs instead of introducing a second, redundant retry API surface.

In particular, TiDB Operator already has a Backup retry mechanism for failed backup jobs. Repo-v1 should align with that model:
- Automatic retry of the same backup CR should resume the same unfinished repo-v1 backup attempt
- Asking for a fresh attempt should be modeled as a new controller-visible attempt, not as a separate interactive `discard` step

To minimize new CRD semantics, the controller-facing mapping can reuse existing concepts:
- Existing backup retry/backoff configuration controls whether the controller retries a failed backup job
- The controller treats retries of the same logical backup attempt as `resume`
- If the user wants a fresh attempt, the controller can use an existing Kubernetes-level signal such as recreating the CR or changing a controller-recognized retry annotation/token, without requiring a new BR-specific retry field in the CRD

Suggested controller state transition:
- No pending backup exists: start a new backup
- One pending backup exists under the current config-hash directory and the controller determines this is the same attempt: resume that backup
- One pending backup exists under the current config-hash directory and the controller determines this is a fresh attempt request: enter a discard phase, remove the old pending backup artifacts, then start a new backup
- Multiple pending backups exist under the current config-hash directory: fail and surface an explicit ambiguity error
- Pending backups under other config-hash directories are unrelated attempts and should not block this one

Discard phase requirements:
- Deleting pending metadata, checkpoint data, unfinished metadata, and partial SSTs must be idempotent
- Reconciliation after a partial discard must be able to continue the cleanup safely
- Completed backups identified by a durable final `backupmeta` must never be treated as discard targets

This gives human-facing CLI and controller-facing automation different UX layers while keeping the underlying repo state machine consistent and close to today's operator model.

#### Restore

Command:
- `br restore full -s <repo> --storage-layout=repo-v1 --backup-id <backup-id> ...`

Semantics:
- Restores the snapshot backup identified by the user-facing `backup-id` (`uint64`) from the repo.

#### Discard Pending Backup

Command:
- `br repo snapshot pending discard -s <repo> [--backup-id <backup-id>]`

Semantics:
- Discards one unfinished snapshot backup and frees the repo to start a new checkpointed backup.
- If there is exactly one pending backup, `--backup-id` may be omitted.
- If multiple pending backups exist, `--backup-id` is required.
- Removes `_meta/pending/<config-hash-hex>/<backup-id>.json`.
- Removes `_meta/snapshot/<backup-id>/checkpoints/...`.
- Removes unfinished per-backup metadata under `_meta/snapshot/<backup-id>/...` when final `backupmeta` is absent.
- Removes partial SST data under `_data/snapshot/<store-id>/<backup-id>/...`.
- Must not delete a completed backup identified by a durable final `backupmeta`; stale pending files for completed backups should be cleaned as stale metadata rather than treated as discard targets.

#### List

Command:
- `br repo snapshot list -s <repo>`

Semantics:
- Lists completed snapshot backups in the repo.
- Outputs the user-facing `backup-id` (`uint64`), snapshot time/TS, backup type, and size.

#### Files of a Backup

Command:
- `br repo snapshot files list -s <repo> --backup-id <backup-id>`
- `br repo snapshot files delete -s <repo> --backup-id <backup-id>`

Semantics:
- `files list` prints the SST objects that belong to the specified backup.
- `files delete` deletes the SST objects that belong to the specified backup and also removes `_meta/snapshot/<backup-id>/...` if present.
- Still works if per-backup metadata is missing, by enumerating store shards under `_data/snapshot/` and matching the upper-case hex `<backup-id>` subprefix in each shard.

#### Orphans

Command:
- `br repo snapshot orphans list -s <repo>`
- `br repo snapshot orphans delete -s <repo>`

Semantics:
- `orphans list` prints SST objects whose `<backup-id>` is not present under `_meta/snapshot/`.
- `orphans delete` deletes SST objects whose `<backup-id>` is not present under `_meta/snapshot/`.
- Can be implemented by comparing `_meta/snapshot/` entries with upper-case hex `<backup-id>` subprefixes found under each store shard in `_data/snapshot/`.
- This is still expected to be more expensive than listing known backups.

### Compatibility

- BR: new `--storage-layout=repo-v1`, `br repo` subcommands, and layout helper.
- TiKV: repo-v1 does not require a new TiKV-side path hook. The baseline deployment path relies on BR rewriting the per-request `StorageBackend` prefix per store.
- PD: backup ID allocation via TSO.
- Upgrade: legacy layout remains supported; repo-v1 is opt-in.
- Downgrade: avoid writing repo-v1 from older BR; repo marker signals layout.
- External tools: restore/list/delete/orphan-cleanup/pending-discard must use repo-v1-aware logic.

### Misc

#### Backend Compatibility of Prefix Rewriting

The backend-prefix-rewrite compatibility path is not equally suitable for all storage backends.

Expected to work well:
- S3 and S3-compatible backends such as KS3 and OSS, because the backend already models a bucket plus a mutable object prefix.
- GCS, because the backend already models a bucket plus object prefix.
- Azure Blob Storage, because the backend already models a container plus blob prefix.
- Local storage, by treating the rewritten target as a different local root path.

Not a good fit today:
- HDFS, because BR's current HDFS storage support is limited and does not provide the full metadata/checkpoint/list/delete capabilities that repo-v1 relies on for snapshot backup management.
- Noop storage, because it is not a real persistence target and already disables checkpoint-oriented behavior.

Therefore, if repo-v1 relies on backend-prefix rewriting as a strong-compatibility path, the intended practical scope should be the main object-storage backends rather than every backend type accepted by BR.

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
- Verify pending pointer creation at `_meta/pending/<config-hash-hex>/<backup-id>.json`.
- Verify backup-id is PD-assigned as a user-facing `uint64`.
- Verify the on-storage `<backup-id>` path segment is fixed-width upper-case hex.
- Verify the baseline implementation path can produce `_data/snapshot/<store-id>/<backup-id>/...` by rewriting the per-store `StorageBackend` prefix.
- Verify the rewritten backend prefix preserves `<store-id>` as the leading path component.
- Verify SST objects are written under `_data/snapshot/<store-id>/<backup-id>/` while keeping legacy TiKV naming within that subprefix.
- Verify `<store-id>` remains the leading data prefix and `<backup-id>` is not moved ahead of it.
- Verify `br repo snapshot list` returns completed backups only.
- Verify unfinished-backup lookup only enumerates `_meta/pending/<config-hash-hex>/` in the normal path.
- Verify a stale pending file for a completed backup is removed instead of forcing a resume.
- Verify `pending discard` removes pending pointer, checkpoint data, unfinished metadata, and partial SST data.
- Verify `snapshot files list` outputs correct keys.
- Verify `snapshot files delete` deletes matching objects and metadata.
- Verify `snapshot orphans list` outputs only orphan SSTs.
- Verify `snapshot orphans delete` deletes only orphan SSTs.
- Verify checkpoint artifacts are stored under `_meta/snapshot/<backup-id>/checkpoints/...`.

### Scenario Tests

Repo-v1 scenarios:
- Multiple backups under one repo; restore older/newer backups.
- Delete one backup while keeping others; verify remaining backups restore.
- One failed backup is discarded, then a new checkpointed backup can start in the same repo.
- Multiple pending entries under the same config-hash directory exist due to manual corruption or partial cleanup; backup start fails with an explicit ambiguity error.
- Multiple unfinished backups with different config-hash directories coexist in the same repo; each new backup only matches or resumes the entries under its own config-hash directory.
- A controller retries the same failed backup CR through its existing retry mechanism; the unfinished backup continues instead of being replaced.
- A controller observes a fresh-attempt signal, such as CR recreation or changed retry annotation/token; the old unfinished backup is discarded and a new one starts.
- Metadata loss for a backup: `snapshot files delete` still deletes by prefix.
- Orphan scan detects unexpected per-backup subprefixes under store shards (simulate interrupted backup).

### Compatibility Tests

Compatibility coverage:
- BR repo-v1 with different storage backends (S3/GCS/Azure/HDFS/local).
- Upgrade from legacy backup usage without repo-v1.
- Restore and cleanup behavior when repo-v1 SST placement is achieved through per-request backend-prefix rewriting.
- Backend-by-backend validation of the prefix-rewrite compatibility path on S3/KS3/OSS/GCS/Azure, with explicit exclusion or documented limitation for HDFS and noop.
- Compatibility with BR `--use-backupmeta-v2`.
- Compatibility with external stats.

### Benchmark Tests

- Prefix listing cost for `snapshot files list` on large repos.
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
- If the controller-facing mapping between "same attempt" and "fresh attempt" is underspecified, operators may see repeated discard/recreate loops or backups that never advance after a failed attempt.

## Investigation & Alternatives

- Per-backup prefix (legacy): simple but reintroduces cold-prefix issues.
- Metadata-driven delete only: fails when metadata is lost.
- Finding unfinished backups by scanning `_meta/snapshot/*`: simple, but startup cost grows with total historical backups instead of active unfinished backups.
- Pending index keyed only by `<backup-id>`: simpler naming, but requires scanning all unfinished backups in the repo instead of directly narrowing to one logical backup configuration.
- Repo-v1 via per-request backend-prefix rewriting: baseline deployment path that avoids a hard dependency on TiKV upgrade, but shifts correctness risk to backend-prefix handling and per-store request mutation.
- Future TiKV-side path hook such as `file_prefix`: could be added later as an implementation optimization without changing the repo-v1 storage layout or user-facing semantics.
- Dedup/compaction: higher complexity, out of scope initially.

## Unresolved Questions

None for now.
