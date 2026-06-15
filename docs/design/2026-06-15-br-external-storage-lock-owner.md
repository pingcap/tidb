# BR External Storage Lock Execution Metadata

- Author: TiDB contributors
- Tracking Issue: N/A

## Background

BR stream restore and log metadata maintenance use object files in external
storage as locks. The current lock metadata records the lock creation time,
the host, the local process ID, the lock transaction ID, and a free-form hint.

The relevant lock files include:

- `truncating.lock`, used by `br log truncate` as an exclusive lock.
- `v1/LOCK.READ.<random>`, used as a read lock for migration metadata.
- `v1/LOCK.WRIT`, used as a write lock for migration metadata.
- `v1/APPEND_LOCK.WRIT`, used to serialize migration append operations.

These locks do not have a lease or TTL. They are released by deleting the lock
file during normal cleanup. If a process crashes, is killed, or becomes stuck
before cleanup completes, the lock file may remain in external storage forever.
Operators then need to inspect the object manually and decide whether it is
safe to remove.

Today this is hard because `locker_host` and `locker_pid` are often not enough
in Kubernetes or other orchestration environments. The `hint` field is also not
structured and is not consistent across lock types.

## Goals

- Add a generated operation identity to BR-created external storage lock files.
- Ensure all locks created by one BR command execution share the same operation
  identity.
- Add structured metadata that helps operators identify the BR process execution
  attempt that created a lock and the resource protected by that lock.
- Log the operation identity when a command starts and when lock conflicts are
  reported.
- Keep the first version limited to troubleshooting and manual operations.
- Keep backward compatibility with existing lock files and existing callers.

## Non-Goals

- Do not add a new user-facing command-line parameter for lock ownership.
- Do not use the operation identity in lock conflict detection.
- Do not automatically clean up stale locks.
- Do not introduce TTL, lease renewal, heartbeat, or fencing in the first
  version.
- Do not change the current read/write lock compatibility rules.
- Do not allow a later operation to adopt, reuse, or release a lock created by a
  previous operation, even when both operations share the same `RestoreID`.
- Do not record full command-line arguments or command strings in lock metadata.

## Design

### Operation Context

BR generates one operation identity when a top-level command starts. This
identity is internal and is not supplied by the user.

```go
type OperationContext struct {
	OperationID string    `json:"operation_id"`
	StartedAt   time.Time `json:"operation_started_at"`

	// Optional lineage fields.
	RestoreID uint64 `json:"restore_id,omitempty"`
}
```

`OperationID` should be a UUID v4 string. It identifies one local execution
attempt of a BR command. If a user reruns the same command with the same
arguments, the new process receives a new `OperationID`.

`StartedAt` should use `time.Time`, matching the existing `locked_at` field.
The JSON representation should remain human-readable time, not TSO. It is a
process execution timestamp for observability, not a TiDB consistency timestamp.

UUID collision risk is negligible for this use case. The correctness focus is
not UUID uniqueness itself, but ensuring one operation generates the ID once and
propagates it consistently to all locks it creates.

All locks acquired by the same command execution must use the same
`OperationID`.

`RestoreID` is different from `OperationID`. It is the persistent identity of a
resumable restore task tracked by the target TiDB cluster. Checkpoint resume can
therefore create a new operation with a new `OperationID` while keeping the same
`RestoreID`. The lock JSON stores it as a JSON number, matching the existing
`uint64` restore ID used by BR.

Checkpoint resume does not transfer ownership of external storage locks. A
later operation must acquire its own locks. If a lock from an earlier operation
still blocks it, the new metadata helps operators identify the stale candidate,
but the lock conflict rules stay unchanged.

### Lock Metadata

The current lock JSON remains valid. New fields are added to the lock metadata:

```go
type LockMeta struct {
	LockedAt   time.Time `json:"locked_at"`
	LockerHost string    `json:"locker_host"`
	LockerPID  int       `json:"locker_pid"`
	TxnID      []byte    `json:"txn_id"`
	Hint       string    `json:"hint"`

	OperationID        string     `json:"operation_id,omitempty"`
	OperationStartedAt *time.Time `json:"operation_started_at,omitempty"`
	RestoreID          uint64     `json:"restore_id,omitempty"`
	ResourceType       string     `json:"resource_type,omitempty"`
}
```

`OperationStartedAt` should be a pointer in `LockMeta` so minimal metadata such
as `LockMetaInput{Hint: ...}` does not emit a bogus zero timestamp. Tests should
cover that minimal metadata omits operation fields.

`ResourceType` describes the resource protected by this concrete lock, such as:

- `log-truncate-exclusive`
- `migration-read`
- `migration-write`
- `migration-append`

BR should define typed string constants for resource types in
`br/pkg/operation`. They should not use numeric enums because lock files are
operator-facing JSON and must remain readable in external storage. The generic
`pkg/objstore` metadata input stores the field as a string so the object storage
layer does not depend on BR-specific terminology.

The legacy `truncating.lock` file should use
`resource_type=log-truncate-exclusive`. If log truncate also runs migration
maintenance, the migration lock should share the same `operation_id` but use a
separate resource type such as `migration-write`.

Append migration uses two locks. The main `v1/LOCK.READ.<random>` lock should
use `resource_type=migration-read`, because it protects a read view of migration
metadata. The `v1/APPEND_LOCK.WRIT` lock should use
`resource_type=migration-append`, because it serializes append operations. Both
locks share the same `operation_id`.

Log restore should use `resource_type=migration-read` when it locks migration
metadata before loading the migration list.

The current production lock acquisition sites are:

- `br log truncate`, which creates `truncating.lock`.
- `MigrationExt.GetReadLock`, used by log restore to read migration metadata.
- `MigrationExt.AppendMigration`, which creates one migration read lock and one
  append serialization lock.
- `MigrationExt.MergeAndMigrateTo`, used by migration maintenance paths such as
  log truncate cleanup and operator `migrate-to`.

High-level propagation must also cover all callers that reach those lock sites.
In particular, PITR collection can call `MigrationExt.AppendMigration` through
`pitrCollector.prepareMig`, so `PiTRCollDep` and `pitrCollector` must carry the
same operation context as the restore operation.

These fields are for observability. They do not affect lock acquisition or
release semantics.

`Hint` remains a free-form human hint. It should not be used as a stable
classification field, and new structured metadata should not be encoded into
the hint string.

### Runtime Operation Context Propagation

The operation context is a BR runtime concept. It should be stored in BR runtime
configuration, not in user configuration or in the generic object storage layer.
The shared BR types should live in a small package such as `br/pkg/operation`.
`pkg/objstore` owns only the lock metadata wire format and display behavior; BR
owns the meaning, propagation, and validation of fields such as `operation_id`
and `restore_id`.

The common BR task config can carry the operation context as a runtime-only
field:

```go
type Config struct {
	...
	OperationContext operation.Context `json:"-" toml:"-"`
}
```

Top-level command entry points initialize `OperationID` once after config
parsing and before creating clients that may acquire locks.

The implementation should provide an `EnsureOperationContext` style helper so
both common BR config paths and operator commands that do not embed common
config can initialize the operation context consistently.

`OperationID` must be generated before any object or client that can acquire an
external storage lock is created. It does not depend on restore registration,
checkpoint metadata, or external storage access.

If a restore operation registers or resumes a restore task, the resolved
`RestoreID` is filled into the same operation context after registration. If
`cfg.RestoreID` is `0`, the lock metadata omits `restore_id`.

For lock metadata, the operation context should be the source of truth for
`restore_id`. Existing fields such as `LogClient.restoreID` are still used by
restore logic, but they must not diverge from the operation context. When
`SetRestoreID` or equivalent code updates a client, it must also update or
receive the final operation context used for lock metadata.

The operation context must then be propagated explicitly through task objects
and clients that can create locks. In particular:

- `RunStreamTruncate` uses the command-level operation context for
  `truncating.lock`.
- Log restore passes the operation context into `LogClient`.
- `LogClient.GetLockedMigrations` passes the operation context into
  `MigrationExt`.
- PITR collector dependencies pass the operation context into `pitrCollector`,
  which passes it into `MigrationExt.AppendMigration`.
- `MigrationExt.GetReadLock`, `AppendMigration`, and `MergeAndMigrateTo` use
  the operation context when creating migration locks.

`MigrationExtension(storage)` must not generate an operation context by itself.
If it did, locks created through the extension could receive a different
operation ID from the top-level command. Instead, either the operation context is
passed when the extension is created, or the extension exposes an explicit
`WithOperationContext` style method.

This gives the implementation two coverage layers:

- Common BR task config initialization creates an `OperationID` by default for
  normal CLI paths.
- Production code that creates external storage locks must explicitly pass the
  operation context to the lock-capable object or explain why operation metadata
  is intentionally unavailable.

Some operator commands do not embed the common BR task config today. For
example, migration maintenance commands can parse only object storage options
and then call `MigrationExtension` directly. Those paths must create their own
operation context before acquiring locks.

If `MigrationExt` stores operation context as a field, methods that copy the
extension, such as dry-run helpers, must copy the operation context too.

Missing operation metadata must not break the generic lock primitive. The
generic `pkg/objstore` layer should accept empty operation metadata for
compatibility. BR production helpers, however, should reject incomplete
operation metadata and fail the command rather than silently downgrading.

### Lock API

The lock API should avoid overloading the existing free-form `hint` string.
The preferred shape is to make the existing lock functions accept a structured
input. This input belongs to `pkg/objstore`, but it should not own or generate
the BR operation context:

```go
type LockMetaInput struct {
	OperationID        string
	OperationStartedAt *time.Time
	RestoreID          uint64
	ResourceType       string
	Hint               string
}
```

BR code expands its runtime operation context into this input when acquiring a
lock. Tests and non-BR callers use the same API and can pass a minimal
`LockMetaInput{Hint: ...}` when they do not have operation metadata.
This is an explicit downgrade path. Callers should not pass an empty
`LockMetaInput{}` unless the lock truly has no useful hint or operation
metadata.

There should not be a separate hint-only API that is used only by tests or
legacy paths. Compile errors from the signature change are desirable because
they force each lock acquisition site to make an explicit metadata decision.

The generic object storage lock layer should not validate BR-specific metadata
combinations. BR production code should instead build `LockMetaInput` through a
helper in `br/pkg/operation`, for example:

```go
func (c OperationContext) LockMeta(
	res LockResourceType,
	hint string,
) (objstore.LockMetaInput, error)
```

The helper should return an error when production metadata is incomplete, such
as an empty `OperationID` or `ResourceType`. Production callers should return
that error instead of falling back to hint-only metadata. Tests and non-BR
callers that intentionally have no operation metadata can pass
`objstore.LockMetaInput{Hint: ...}` directly. This keeps business validation in
BR while keeping the lock primitive generic.

### Logging

BR must log the generated operation identity when a top-level command starts.
This gives operators a log-side handle before they inspect external storage.
The operation identity does not need to be added to BR summary output in the
first version.

The command-start log should include at least `operation_id`,
`operation_started_at`, local host, local PID, and a non-sensitive command or
subcommand label. It must not include full command-line arguments or storage
URIs. When a restore registration later resolves a non-zero `restore_id`, BR
should log a follow-up record containing both `operation_id` and `restore_id`.

Lock acquisition failures should also include the local operation metadata and,
when available, the remote lock metadata that caused the conflict. In
particular, lock conflict logs and errors should expose `operation_id`,
`operation_started_at`, `restore_id`, and `resource_type` when those fields are
present. This makes it possible to correlate a failed command with a stale lock
file without first opening the object manually.

Conflict logs should distinguish the current command from the blocking lock by
using local and remote field groups, for example `local_operation_id` and
`remote_operation_id`. `LockMeta.String()` should show non-empty core ownership
fields such as operation ID, operation start time, restore ID, resource type,
host, PID, locked time, and hint. It should not include `txn_id` in the normal
string form; the transaction ID is still useful for unlock validation and debug
logging, but it is less useful for operator-facing conflict messages.

Conflict logs should also identify the blocking object path. Write-lock
conflicts can be caused by any file under the lock prefix, including read locks,
so logging only the expected write-lock path can be misleading. When a prefix
check finds multiple blockers, BR should log the blocker count and enough
first-N blocker metadata to identify the likely owner without flooding logs.

## Correctness Considerations

The operation identity is not a lease and is not proof that a lock is stale.
Operators must not treat matching or old operation IDs as sufficient evidence
for automatic deletion.

Lock metadata is self-reported object content. It is useful for troubleshooting
and correlation, but it is not authentication, authorization, liveness proof, or
exclusive evidence that a process has stopped.

The identity is useful because it groups all locks created by one command
execution. For example, an append operation can hold both a migration read lock
and an append write lock. Both lock files should contain the same
`OperationID`, while their `ResourceType` values explain the different lock
roles.

If BR later supports automatic stale-lock recovery, this metadata can become
part of the evidence shown to operators or cleanup tools. A safe automatic
cleanup design would still need additional mechanisms, such as leases,
heartbeat, or fencing.

Likewise, supporting lock adoption for checkpoint resume would require a
separate correctness protocol. A matching `RestoreID` is not enough to prove
that the previous operation has stopped writing.

## Compatibility

Old lock files do not contain the new fields. Reading them should continue to
work because JSON unmarshalling leaves missing fields empty.

New lock files can be read by new BR versions. Older BR versions that only know
the old fields should also ignore the extra JSON fields when reading lock
metadata.

The lock file names, conflict detection rules, retry behavior, and unlock
behavior remain unchanged.

BR production validation applies to local metadata being created for new locks.
It must not reject or fail to display remote old lock files that do not contain
operation metadata.

## Repo Flow / Bazel Impact

The design document itself does not require Bazel metadata regeneration.
Implementation will likely add a new Go package such as `br/pkg/operation` and
may add new top-level Go test functions. Per repository policy, that requires
running `make bazel_prepare` and including generated Bazel metadata changes.

Validation should use targeted package tests with the repository's failpoint
test flow where applicable. Broad integration or RealTiKV tests are not required
for this metadata-only change unless later implementation changes behavior that
depends on SQL integration or real TiKV.

## Testing Plan

- Unit test lock metadata serialization with and without `OperationContext`.
- Unit test that minimal `LockMetaInput{Hint: ...}` still produces valid
  metadata for callers without operation metadata and does not emit zero
  operation fields. Existing `pkg/objstore` primitive tests can use this form
  because they are not BR production paths.
- Unit test old JSON lock metadata unmarshalling into the new struct, and
  `LockMeta.String()` rendering operation/resource fields while excluding
  `txn_id`.
- Unit test that a single migration append operation writes the same
  `OperationID` to both the migration read lock and the append write lock.
- Unit test that checkpoint resume can use the same `RestoreID` with different
  `OperationID` values.
- Add or update BR stream tests that inspect generated lock metadata for
  `ResourceType` and `OperationID`.
- Add targeted local-storage tests for `truncating.lock`, `GetReadLock`,
  `AppendMigration`, `MergeAndMigrateTo`, log restore `GetLockedMigrations`,
  PITR collector append, and operator `migrate-to`.
- Add log-focused coverage or targeted assertions for command-start and
  lock-conflict logging where practical.
- Add coverage for production BR paths that create external storage locks,
  including operator migration maintenance paths that do not use the common BR
  task config.

## Future Work

- Add a lock inspection command that lists lock files and renders the structured
  metadata in a human-readable form.
- Add a guarded manual cleanup command that shows operation metadata before
  deleting selected lock files.
- Design a real stale-lock recovery protocol with lease renewal and fencing if
  fully automatic cleanup becomes necessary.
