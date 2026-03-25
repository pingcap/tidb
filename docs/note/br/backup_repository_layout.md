# BR Backup Repository Layout Notes

## Snapshot Repo-V1 Initial Wiring

- `br backup full|db|table --storage-layout=repo-v1` now treats `--storage` as a repository root.
- The first repo-v1 backup initializes `_meta/repo.json` and keeps a root `backup.lock` as the long-lived guard file.
- Snapshot metadata and checkpoint files are written through a prefixed storage view rooted at `_meta/snapshot/<backup-id>/`.
- Snapshot SST requests are rewritten per store so the TiKV target backend prefix becomes `_data/snapshot/<store-id>/<backup-id>/...`.

## Backup ID And Restore Binding

- `backup-id` is allocated from a fresh PD TSO and encoded as fixed-width lower-case hex.
- `br restore full|db|table --storage-layout=repo-v1 --backup-id <id>` resolves metadata from `_meta/snapshot/<backup-id>/backupmeta` while still using the repo root backend for data files.
- Legacy snapshot backup and restore keep the old root-level `backupmeta` layout unchanged.

## Pending Marker Semantics

- Repo-v1 currently creates pending markers only when snapshot checkpoint mode is enabled.
- Startup scans `_meta/pending/<config-hash>/` for repo-v1 backups of the same immutable backup config.
- If a pending entry already has final `backupmeta`, BR removes it as stale metadata.
- If a pending entry has checkpoint metadata but no final `backupmeta`, BR fails fast instead of implicitly resuming.
- If a pending entry has neither checkpoint metadata nor final `backupmeta`, BR treats it as inconsistent repo state and reports an operator-visible error.

## Known Scope Boundary

- This change set intentionally does not add `br repo ...` management commands yet.
- Because repo management is still missing, the current repo-v1 path blocks on unfinished checkpointed backups instead of offering resume or discard from the CLI.
