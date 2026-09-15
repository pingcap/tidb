# Proposal: Globally Restorable Physical Storage Identity

- Author(s): [bb7133](https://github.com/bb7133)
- Discussion PR: https://github.com/pingcap/tidb/pull/69924
- Tracking Issue: https://github.com/pingcap/tidb/issues/69923

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
    * [BR restore currently rewrites table key prefixes](#br-restore-currently-rewrites-table-key-prefixes)
    * [TiDB-X tenant migration needs a portable storage identity](#tidb-x-tenant-migration-needs-a-portable-storage-identity)
* [Detailed Design](#detailed-design)
    * [Definitions](#definitions)
    * [Metadata](#metadata)
    * [UUID Generation](#uuid-generation)
    * [Key Format](#key-format)
    * [DDL Semantics](#ddl-semantics)
    * [BR Restore Semantics](#br-restore-semantics)
    * [TiDB-X Tenant Migration](#tidb-x-tenant-migration)
    * [Component Changes](#component-changes)
    * [Rollout Plan](#rollout-plan)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
    * [MongoDB Collection UUID](#mongodb-collection-uuid)
    * [Preserve Source int64 Physical Table IDs](#preserve-source-int64-physical-table-ids)
    * [Logical UUID Only](#logical-uuid-only)
    * [Globally Allocated int64 Physical IDs](#globally-allocated-int64-physical-ids)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This document proposes introducing a globally restorable physical storage identity for TiDB tables and partitions.
The identity is persisted in table metadata and encoded in a new table key format, so that supported BR restore and TiDB-X tenant migration flows can preserve data key prefixes instead of rewriting the source physical table ID to a target-local physical table ID.

The proposal does not replace all existing `TableInfo.ID` or `PartitionDefinition.ID` usages immediately.
Instead, it introduces a versioned physical storage identity and a versioned key format, allowing old `int64`-based table keys and new UUID-based table keys to coexist.

## Motivation or Background

### BR restore currently rewrites table key prefixes

TiDB currently encodes the physical table ID directly in table row and index keys:

```text
record key: t{physical_table_id}_r{handle}
index key : t{physical_table_id}_i{index_id}...
```

For a non-partitioned table, the physical table ID is the table ID.
For a partitioned table, each partition definition ID is a physical table ID.

These IDs are allocated from TiDB metadata in the target cluster.
When BR restores a table into another cluster, the target table commonly receives different physical table IDs from the source table.
Therefore BR has to keep a source-to-target physical table ID mapping and rewrite table key prefixes during restore.

This makes restore more complicated and prevents direct reuse of source KV keys:

* BR has extra rewrite cost in the ingest path.
* Backup metadata has to preserve physical ID mappings carefully.
* Log restore and partial restore need additional object matching.
* Debugging cross-cluster restore results is harder because the same logical table has different physical table IDs after restore.

### TiDB-X tenant migration needs a portable storage identity

TiDB-X tenant migration needs to move data between tenants, keyspaces, or clusters.
If table data is keyed by a target-local `int64` physical table ID, migration has to either reserve non-conflicting table ID ranges or rewrite data keys during migration.

A globally restorable physical storage identity gives tenant migration a portable data identity.
The migration layer may still remap logical metadata IDs for the target tenant, but the physical data key prefix can remain stable for tables using the new key format.

## Detailed Design

### Definitions

This document separates three concepts:

* **Logical schema object ID**: existing `TableInfo.ID` and `PartitionDefinition.ID`.
  These remain `int64` IDs used by metadata, DDL jobs, diagnostics, and old-format tables.
* **Logical object UUID**: an optional future identity for logical lineage, audit, and cross-cluster object correlation.
  This document does not require it to be encoded in data keys.
* **Physical storage UUID**: a new identity for a physical table data space.
  This identity is encoded in the new row/index key prefix and is preserved by BR restore and TiDB-X tenant migration.

The core requirement is that the physical storage identity must be part of the physical data key.
Adding a logical UUID to table metadata alone cannot remove BR key rewrite while table keys still use `t{int64}` prefixes.

### Metadata

Add optional physical storage UUID fields to table and partition metadata:

```go
type TableInfo struct {
    // Existing fields.
    ID int64

    // PhysicalStorageUUID is set for tables using the UUID-based physical key format.
    // It identifies the physical data space of a non-partitioned table.
    PhysicalStorageUUID []byte

    // PhysicalStorageKeyFormat identifies how row/index keys are encoded.
    PhysicalStorageKeyFormat PhysicalStorageKeyFormat
}

type PartitionDefinition struct {
    // Existing fields.
    ID int64

    // PhysicalStorageUUID identifies this physical partition data space
    // when the partition uses the UUID-based physical key format.
    PhysicalStorageUUID []byte
}
```

The exact field names and protobuf/JSON representation can be adjusted during implementation.
The identity should be stored as raw 16 bytes in metadata and converted to the canonical UUID string only for display, logs, and API output.

For old-format tables:

* `PhysicalStorageKeyFormat` is `Int64TableID`.
* `PhysicalStorageUUID` is empty.
* Row/index keys continue using the current `t{int64}` prefix.

For new-format tables:

* `PhysicalStorageKeyFormat` is `UUID`.
* Non-partitioned tables have `TableInfo.PhysicalStorageUUID`.
* Partitioned tables have one `PartitionDefinition.PhysicalStorageUUID` per physical partition.
* Row/index keys use the UUID-based prefix.

### UUID Generation

New physical storage UUIDs should use RFC 9562 UUIDv7, while UUIDv4 is also acceptable.

UUIDv7 is preferred over UUIDv4 for this use case because it is roughly creation-time ordered.
That property is closer to TiDB's current increasing physical table ID behavior and helps metadata inspection, debugging, and operational correlation.
From a collision-probability perspective, both UUIDv4 and UUIDv7 are sufficient for TiDB DDL object creation rates.

Generation timing:

* `CREATE TABLE`: generate one physical storage UUID for a non-partitioned table.
* `CREATE TABLE` with partitions: generate one physical storage UUID per physical partition.
* `ADD PARTITION`: generate one physical storage UUID for each new partition.
* `REORGANIZE PARTITION`: generate UUIDs for newly created physical partitions.
* `TRUNCATE TABLE`: generate a new physical storage UUID for the new physical data space.
* `TRUNCATE PARTITION`: generate a new physical storage UUID for the truncated partition.
* BR restore and TiDB-X tenant migration: preserve source physical storage UUIDs by default.

TiDB should provide a shared helper instead of calling a UUID library directly from many DDL paths:

```go
func NewPhysicalStorageUUID() uuid.UUID {
    return uuid.Must(uuid.NewV7())
}
```

Even though random collision is practically negligible, metadata mutation must still enforce uniqueness within a keyspace.
If a newly generated UUID conflicts, DDL can retry generation.
If restore or migration sees an existing target object with the same preserved UUID, it must report a conflict unless the requested mode explicitly drops or replaces that object.
Compatibility modes may continue to use the current key rewrite implementation, but such modes do not provide no-rewrite restore semantics.

### Key Format

Introduce a versioned table key prefix for UUID-based physical storage identity.
The exact byte layout should be finalized with TiKV/BR compatibility review.
The design does not require the literal `t2` prefix, but it does require an unambiguous format marker.
The decoder must not infer the format only from the identity length.

Conceptually:

```text
old record key: t{int64_physical_table_id}_r{handle}
old index key : t{int64_physical_table_id}_i{index_id}...

new record key: t{format_marker}{16-byte_physical_storage_uuid}_r{handle}
new index key : t{format_marker}{16-byte_physical_storage_uuid}_i{index_id}...
```

The new layout may keep the `t` table-key family for locality and compatibility with table-key scanning, but the byte after `t` or an equivalent field must identify the key format before decoding the physical identity.
This is required because existing tablecodec logic assumes `t + 8-byte int64` in many places, including fixed offsets for record/index separators and table range boundaries.
A naked `t{16-byte_uuid}` layout would force raw key decoders to guess whether `_r` or `_i` belongs to the old offset or the new offset, and UUID bytes can contain arbitrary byte patterns.

The key format must preserve prefix locality for a table or partition:

```text
tablePrefix(physicalStorageUUID)
recordPrefix(physicalStorageUUID)
indexPrefix(physicalStorageUUID, indexID)
```

Code must not assume that a table range can always be expressed as:

```go
[EncodeTablePrefix(id), EncodeTablePrefix(id + 1))
```

Instead, callers should use prefix-aware helper APIs:

```go
type PhysicalTableIdentity struct {
    Format PhysicalStorageKeyFormat
    Int64ID int64
    UUID []byte
}

func EncodePhysicalTablePrefix(id PhysicalTableIdentity) []byte
func EncodePhysicalRecordPrefix(id PhysicalTableIdentity) []byte
func EncodePhysicalIndexPrefix(id PhysicalTableIdentity, indexID int64) []byte
func PrefixNext(prefix []byte) []byte
```

This gives both old and new key formats a common abstraction and avoids spreading conditional logic across planner, executor, BR, TiFlash, CDC, and diagnostic code.

### DDL Semantics

DDL must preserve the existing physical-data-space semantics:

* Rename table: preserve physical storage UUID.
* Truncate table: generate a new physical storage UUID.
* Add partition: generate UUIDs for added partitions.
* Drop partition: schedule delete-range for the dropped partition UUID prefix.
* Truncate partition: generate a new UUID for the truncated partition.
* Reorganize partition: generate UUIDs for newly created partitions.
* Exchange partition: keep the physical storage identity with the physical data being exchanged. The exact metadata swap rules need a dedicated sub-design because exchange partition is sensitive to logical-table and physical-data ownership.

Delete-range, GC, and DDL rollback paths must operate on `PhysicalTableIdentity` instead of only `int64` physical table IDs.

### BR Restore Semantics

Backup metadata should record both legacy IDs and physical storage UUIDs:

```text
database name
table name
table ID
partition IDs
physical storage key format
table physical storage UUID, if any
partition physical storage UUIDs, if any
```

Restore behavior:

* For old-format tables, BR continues using the current old-ID-to-new-ID rewrite path.
* For new-format tables, BR preserves source physical storage UUIDs and can ingest KV data without rewriting the physical table key prefix.
* If the target cluster already has the same physical storage UUID, restore reports a conflict unless the user requested a mode that drops/replaces the target object.
* Restore must advance or reconcile metadata ID allocators as needed, but this does not require rewriting UUID-based data key prefixes.
* Log restore should use physical storage UUID as the stable physical object identity for UUID-format tables.

This design does not remove all restore remapping.
Logical schema names, table IDs, auto IDs, placements, statistics, and privileges may still need restore-time decisions.
The goal is specifically to remove physical table ID key-prefix rewrite for tables using the UUID-based key format.

### TiDB-X Tenant Migration

Tenant migration can preserve physical storage UUIDs while moving data to another tenant, keyspace, or cluster.
The target tenant can allocate new local `int64` schema object IDs, while the physical data key prefix remains unchanged for UUID-format tables.

Migration conflict handling should follow restore semantics:

* If the target tenant has no object with the same physical storage UUID, preserve and attach the UUID.
* If the target tenant already has the same UUID, fail unless the migration mode explicitly replaces the target object.
* If the target only supports old-format tables, fall back to a rewrite path or reject no-rewrite migration.

### Component Changes

The following components need updates to understand `PhysicalTableIdentity` and the versioned key format:

* Tablecodec row/index encoding and decoding.
* Range building in planner, executor, and coprocessor requests.
* DDL delete-range and GC.
* BR backup/restore metadata, rewrite rules, ingest, and checksum.
* Lightning import and local backend if it creates raw TiKV keys.
* TiCDC table identity tracking and event encoding.
* TiFlash table sync metadata.
* PD region split and placement rule generation.
* Statistics, feedback, and persisted stats metadata.
* Diagnostics, logs, metrics, `ADMIN` statements, and internal virtual tables that display physical table identity.

### Rollout Plan

1. **Metadata preparation**
   * Add physical storage UUID metadata fields.
   * Generate UUIDs for new tables and partitions but keep old key format as default.
   * Add uniqueness checks and diagnostic display.

2. **Experimental key format**
   * Add UUID-based tablecodec helpers.
   * Enable the new format only behind an experimental switch for newly created tables.
   * Keep old-format table behavior unchanged.

3. **DDL and delete-range coverage**
   * Cover create, drop, truncate, partition DDLs, and delete-range behavior.
   * Add compatibility tests for mixed old/new-format tables.

4. **BR no-rewrite path**
   * Extend backup metadata.
   * Preserve physical storage UUIDs on restore.
   * Add conflict detection and user-facing errors.
   * Avoid table key-prefix rewrite for UUID-format tables.

5. **TiDB-X tenant migration POC**
   * Demonstrate tenant migration using preserved physical storage UUIDs.
   * Validate conflict handling and rollback behavior.

6. **Ecosystem support**
   * Complete support for TiFlash, TiCDC, Lightning, statistics, diagnostics, placement, and tooling.

7. **Optional migration for old tables**
   * If needed, design a separate conversion path from old `int64` prefixes to UUID prefixes.
   * This is not required for the initial no-rewrite restore path for newly created UUID-format tables.

## Test Design

### Functional Tests

* Create UUID-format non-partitioned and partitioned tables and verify metadata contains unique physical storage UUIDs.
* Verify row and index keys use the UUID-based prefix.
* Verify rename preserves UUIDs.
* Verify truncate table and truncate partition generate new UUIDs.
* Verify delete-range removes UUID-prefixed data after drop/truncate.
* Verify metadata uniqueness checks reject duplicate physical storage UUIDs.

### Scenario Tests

* Backup and restore a UUID-format table into an empty target cluster without table ID key-prefix rewrite.
* Restore a UUID-format partitioned table and verify each partition keeps its source physical storage UUID.
* Try restoring into a target cluster that already has the same physical storage UUID and verify the conflict error.
* Run a TiDB-X tenant migration POC that preserves physical storage UUIDs.
* Run mixed old-format and UUID-format tables in the same cluster.

### Compatibility Tests

* Upgrade a cluster and verify old-format tables keep using old key prefixes.
* Downgrade behavior must be explicitly guarded. A cluster with UUID-format tables should not be downgraded to a version that cannot decode them.
* Verify TiFlash, TiCDC, BR, Lightning, statistics, placement rules, and diagnostics with mixed table formats.
* Verify partition DDL compatibility, including add, drop, truncate, and reorganize partition.

### Benchmark Tests

* Measure storage overhead from increasing the physical table prefix from 8 bytes to 16 bytes.
* Measure point lookup, index lookup, range scan, write, and delete-range performance for old and UUID key formats.
* Measure BR restore throughput with and without key-prefix rewrite.
* Measure TiDB-X tenant migration throughput and metadata overhead.

## Impacts & Risks

Expected positive impacts:

* BR can avoid physical table ID key-prefix rewrite for UUID-format tables.
* TiDB-X tenant migration can preserve physical data identity across tenants and clusters.
* Cross-cluster debugging becomes easier because the physical storage identity is stable after restore or migration.

Expected costs and risks:

* Key size increases because the physical table prefix grows from 8 bytes to 16 bytes for UUID-format tables.
* Code paths that assume `int64` physical table IDs or `id + 1` range boundaries need careful refactoring.
* Cross-component compatibility is large and needs phased rollout.
* Downgrade rules become stricter once UUID-format tables are created.
* Conflict semantics during partial restore and migration must be explicit to avoid accidental overwrite.

## Investigation & Alternatives

### Globally Allocated int64 Physical IDs

Another alternative is to encode cluster, keyspace, or tenant information into an `int64` physical table ID.
This keeps the key prefix compact but introduces lifecycle problems:

* cluster ID allocation and reuse,
* tenant clone and restore semantics,
* ID-space ownership transfer,
* compatibility with existing allocators and reserved ranges.

UUID-based storage identity is more explicit and avoids turning local ID allocation into a global coordination problem.

## Unresolved Questions

* What is the exact binary key prefix layout for tablecodec v2?
* Should UUID-format tables be enabled by a session/global variable, a DDL option, or an internal cluster capability flag?
* How should `EXCHANGE PARTITION` handle physical storage UUID ownership?
* Which diagnostics should display both legacy table ID and physical storage UUID?
* What are the precise downgrade and backup-compatibility rules?
* Should old tables have an online/offline migration path to UUID-format keys?
* How should TiDB-X represent tenant-level ownership of a preserved physical storage UUID?
