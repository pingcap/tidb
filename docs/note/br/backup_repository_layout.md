# BR Backup Repository Layout Notes

## Snapshot Repo-V1 Initial Wiring

- `br backup full|db|table --storage-layout=repo-v1` now treats `--storage` as a repository root.
- The first repo-v1 backup initializes `_meta/repo.json` and keeps a root `backup.lock` as the long-lived guard file.
- Snapshot metadata and checkpoint files are written through a prefixed storage view rooted at `_meta/snapshot/<backup-id>/`.
- Snapshot SST requests are rewritten per store so the TiKV target backend prefix becomes `_data/snapshot/<store-id>/<backup-id>/...`.

## Backup ID And Restore Binding

- `backup-id` is allocated from a fresh PD TSO, exposed to users as decimal `uint64`, and encoded as fixed-width upper-case hex only in storage paths.
- `br restore full|db|table --storage-layout=repo-v1 --backup-id <id>` resolves metadata from `_meta/snapshot/<backup-id>/backupmeta` while still using the repo root backend for data files.
- Legacy snapshot backup and restore keep the old root-level `backupmeta` layout unchanged.

## Pending Marker Semantics

- Repo-v1 currently creates pending markers only when snapshot checkpoint mode is enabled.
- Startup scans `_meta/pending/<config-hash>/` for repo-v1 backups of the same immutable backup config. The on-storage config-hash directory name is upper-case hex.
- If a pending entry already has final `backupmeta`, BR removes it as stale metadata.
- If a pending entry has checkpoint metadata but no final `backupmeta`, BR fails fast instead of implicitly resuming.
- If a pending entry has neither checkpoint metadata nor final `backupmeta`, BR treats it as inconsistent repo state and reports an operator-visible error.

## Repo Management Commands

- `br repo snapshot ...` now covers listing, inspecting, deleting completed snapshots, discarding unfinished snapshots, and managing orphan snapshot objects.
- Pending snapshot discard distinguishes stale markers from unfinished checkpoint-backed backups and preserves the same on-storage state classification used by backup startup checks.

## CLI Progress Integration

- Repo snapshot commands now route long-running work through `ConsoleGlue` so CLI users get terminal progress feedback instead of a silent wait.
- `ConsoleGlue` now supports dynamic-total progress bars for workloads that discover more work while running.
- Repo mutation plumbing keeps the public `SnapshotOps` methods as the primary entrypoints and threads progress observation through function options instead of separate `WithCallback` variants.
- Metadata scans use a single-step ConsoleGlue task indicator, while destructive commands (`snapshot delete`, `snapshot pending discard`, `snapshot orphans delete`) grow the progress total as objects are discovered and advance it as each object deletion finishes.
- Dynamic progress currently covers metadata files, snapshot data files, and pending marker files, so the visual progress still converges to the same mutation summary printed after the command completes.
