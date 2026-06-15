# TiDB BR External Storage Locking

This context describes the language used for BR-owned lock files in external
storage and the operations that create them.

## Language

**Operation**:
A single BR process execution attempt that may create one or more external
storage locks. The main purpose of this term is to identify which execution
attempt created a lock.
_Avoid_: task, job

**Operation ID**:
The identity of one **Operation**, generated internally by BR and shared by all
external storage locks created during that operation.
_Avoid_: task name, lock owner label

**Operation Context**:
The runtime identity envelope for one **Operation**. It contains the operation
ID and may also carry lineage identifiers such as restore ID. This is a BR
runtime concept, not an object storage concept.
_Avoid_: lock owner

**Restore ID**:
The persistent identity of a resumable restore tracked by the target TiDB
cluster. Multiple operations can share one restore ID when checkpoint resume
continues the same restore. It is a lineage marker and does not grant ownership
of locks created by another operation.
_Avoid_: operation ID

**Log Backup Task**:
A persistent log backup task identified by the existing BR `task-name`.
One log backup task may be observed or modified by many operations over time.
_Avoid_: operation

**Lock Resource**:
The resource or role protected by one concrete external storage lock, such as
migration read access, migration write access, append serialization, or truncate
exclusion. This term should stay coarse and operator-facing.
_Avoid_: operation

**Lock Adoption**:
Treating a lock created by a previous operation as held by a later operation,
usually during checkpoint resume. Current external storage locks are not adopted
across operations.
_Avoid_: checkpoint resume

## Example Dialogue

Developer: "This restore point command is one operation, so every lock it
creates should share one operation ID."

Domain expert: "Right. The log backup task name might still be the same across
many restores, but each restore operation gets its own operation ID."

Developer: "The migration read lock and append lock are different lock
resources, but they can belong to the same operation."

Domain expert: "If checkpoint resume continues the same restore, the new BR
command is a new operation with a new operation ID, but it keeps the same
restore ID."

Developer: "Can the new operation reuse the previous operation's lock because
the restore ID is the same?"

Domain expert: "No. That would be lock adoption. Checkpoint resume continues the
restore task, but it does not transfer ownership of external storage locks."
