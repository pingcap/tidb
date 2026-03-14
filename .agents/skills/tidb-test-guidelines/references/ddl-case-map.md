# TiDB ddl Test Case Map (pkg/ddl)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/ddl

### Tests
- `pkg/ddl/affinity_test.go` - ddl: Tests affinity build group definitions table.
- `pkg/ddl/attributes_sql_test.go` - ddl: Tests alter table partition attributes.
- `pkg/ddl/backfilling_dist_scheduler_test.go` - ddl: Tests backfilling scheduler local mode.
- `pkg/ddl/backfilling_test.go` - ddl: Tests done task keeper.
- `pkg/ddl/backfilling_txn_executor_test.go` - ddl: Tests expected ingest worker count.
- `pkg/ddl/bdr_test.go` - ddl: Tests denied by BDR when add column.
- `pkg/ddl/bench_test.go` - ddl: Tests extract datum by offsets.
- `pkg/ddl/cancel_test.go` - ddl: Tests cancel various jobs.
- `pkg/ddl/cluster_test.go` - ddl: Tests flashback close and reset PD schedule.
- `pkg/ddl/column_change_test.go` - ddl: Tests column add.
- `pkg/ddl/column_modify_test.go` - ddl: Tests add and drop column.
- `pkg/ddl/column_test.go` - ddl: Tests column basic.
- `pkg/ddl/column_type_change_test.go` - ddl: Tests column type change state between integer.
- `pkg/ddl/constraint_test.go` - ddl: Tests alter constraint add drop.
- `pkg/ddl/db_cache_test.go` - ddl: Tests alter table cache.
- `pkg/ddl/db_change_failpoints_test.go` - ddl: Tests modify column type args.
- `pkg/ddl/db_change_test.go` - ddl: Tests show create table.
- `pkg/ddl/db_integration_test.go` - ddl: Tests create table if not exists like.
- `pkg/ddl/db_rename_test.go` - ddl: Tests rename table with locked.
- `pkg/ddl/db_table_test.go` - ddl: Tests add not null column.
- `pkg/ddl/db_test.go` - ddl: Tests get time zone.
- `pkg/ddl/ddl_algorithm_test.go` - ddl: Tests find alter algorithm.
- `pkg/ddl/ddl_error_test.go` - ddl: Tests table error.
- `pkg/ddl/ddl_history_test.go` - ddl: Tests DDL history basic.
- `pkg/ddl/ddl_running_jobs_test.go` - ddl: Tests running jobs.
- `pkg/ddl/ddl_test.go` - ddl: Tests get interval from policy.
- `pkg/ddl/ddl_workerpool_test.go` - ddl: Tests DDL worker pool.
- `pkg/ddl/executor_nokit_test.go` - ddl: Tests build query string from jobs.
- `pkg/ddl/executor_test.go` - ddl: Tests get DDL jobs.
- `pkg/ddl/export_test.go` - ddl: Tests export.
- `pkg/ddl/fail_test.go` - ddl: Tests fail before decode args.
- `pkg/ddl/foreign_key_test.go` - ddl: Tests foreign key.
- `pkg/ddl/index_change_test.go` - ddl: Tests index change.
- `pkg/ddl/index_cop_test.go` - ddl: Tests add index fetch rows from coprocessor.
- `pkg/ddl/index_modify_test.go` - ddl: Tests add primary key1.
- `pkg/ddl/index_nokit_test.go` - ddl: Tests modify task param loop.
- `pkg/ddl/integration_test.go` - ddl: Tests DDL statements back fill.
- `pkg/ddl/job_scheduler_test.go` - ddl: Tests must reload schemas.
- `pkg/ddl/job_scheduler_testkit_test.go` - ddl: Tests DDL scheduling.
- `pkg/ddl/job_submitter_test.go` - ddl: Tests gen ID and insert jobs with retry.
- `pkg/ddl/job_worker_test.go` - ddl: Tests check owner.
- `pkg/ddl/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/ddl/metabuild_test.go` - ddl: Tests new meta build context with session ctx.
- `pkg/ddl/modify_column_test.go` - ddl: Tests modify column reorg info.
- `pkg/ddl/multi_schema_change_test.go` - ddl: Tests multi schema change add columns cancelled.
- `pkg/ddl/mv_index_test.go` - ddl: Tests multi valued index online DDL.
- `pkg/ddl/options_test.go` - ddl: Tests options.
- `pkg/ddl/owner_mgr_test.go` - ddl: Tests owner manager.
- `pkg/ddl/partition_test.go` - ddl: Tests drop and truncate partition.
- `pkg/ddl/placement_policy_ddl_test.go` - ddl: Tests placement policy in use.
- `pkg/ddl/placement_policy_test.go` - ddl: Tests placement policy.
- `pkg/ddl/placement_sql_test.go` - ddl: Tests create schema with placement.
- `pkg/ddl/primary_key_handle_test.go` - ddl: Tests multi-region get table end handle.
- `pkg/ddl/reorg_test.go` - ddl: Tests reorg ctx set max progress.
- `pkg/ddl/repair_table_test.go` - ddl: Tests repair table.
- `pkg/ddl/restart_test.go` - ddl: Tests schema resume.
- `pkg/ddl/rollingback_test.go` - ddl: Tests cancel add index job error.
- `pkg/ddl/schema_test.go` - ddl: Tests schema.
- `pkg/ddl/schema_version_test.go` - ddl: Tests should check assumed server.
- `pkg/ddl/sequence_test.go` - ddl: Tests create sequence.
- `pkg/ddl/stat_test.go` - ddl: Tests get DDL info.
- `pkg/ddl/table_mode_test.go` - ddl: Tests table mode basic.
- `pkg/ddl/table_modify_test.go` - ddl: Tests lock table read only.
- `pkg/ddl/table_split_test.go` - ddl: Tests table split.
- `pkg/ddl/table_test.go` - ddl: Tests table.
- `pkg/ddl/tiflash_replica_test.go` - ddl: Tests set table flash replica.
- `pkg/ddl/ttl_test.go` - ddl: Tests get TTL info in options.

## pkg/ddl/copr

### Tests
- `pkg/ddl/copr/copr_ctx_test.go` - ddl/copr: Tests new cop context single index.

## pkg/ddl/ingest

### Tests
- `pkg/ddl/ingest/checkpoint_test.go` - ddl/ingest: Tests checkpoint manager.
- `pkg/ddl/ingest/env_test.go` - ddl/ingest: Tests gen lightning data dir.
- `pkg/ddl/ingest/integration_test.go` - ddl/ingest: Tests add index ingest generated columns.
- `pkg/ddl/ingest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/ddl/ingest/mem_root_test.go` - ddl/ingest: Tests memory root.

## pkg/ddl/label

### Tests
- `pkg/ddl/label/attributes_test.go` - ddl/label: Tests new label.
- `pkg/ddl/label/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/ddl/label/rule_test.go` - ddl/label: Tests apply attributes spec.

## pkg/ddl/notifier

### Tests
- `pkg/ddl/notifier/events_test.go` - ddl/notifier: Tests event string.
- `pkg/ddl/notifier/store_test.go` - ddl/notifier: Tests leftover when unmarshal.
- `pkg/ddl/notifier/testkit_test.go` - ddl/notifier: Tests publish to table store.

## pkg/ddl/placement

### Tests
- `pkg/ddl/placement/bundle_test.go` - ddl/placement: Tests empty.
- `pkg/ddl/placement/common_test.go` - ddl/placement: Tests group.
- `pkg/ddl/placement/constraint_test.go` - ddl/placement: Tests new from yaml.
- `pkg/ddl/placement/constraints_test.go` - ddl/placement: Tests new constraints.
- `pkg/ddl/placement/meta_bundle_test.go` - ddl/placement: Tests new table bundle.
- `pkg/ddl/placement/rule_test.go` - ddl/placement: Tests clone.

## pkg/ddl/schematracker

### Tests
- `pkg/ddl/schematracker/dm_tracker_test.go` - ddl/schematracker: Tests no num limit.
- `pkg/ddl/schematracker/info_store_test.go` - ddl/schematracker: Tests info store lower case table names.

## pkg/ddl/schemaver

### Tests
- `pkg/ddl/schemaver/syncer_nokit_test.go` - ddl/schemaver: Tests node versions.
- `pkg/ddl/schemaver/syncer_test.go` - ddl/schemaver: Tests syncer simple.

## pkg/ddl/serverstate

### Tests
- `pkg/ddl/serverstate/syncer_test.go` - ddl/serverstate: Tests state syncer simple.

## pkg/ddl/session

### Tests
- `pkg/ddl/session/session_pool_test.go` - ddl/session: Tests session pool.

## pkg/ddl/systable

### Tests
- `pkg/ddl/systable/manager_test.go` - ddl/systable: Tests manager.
- `pkg/ddl/systable/min_job_id_test.go` - ddl/systable: Tests refresh min job ID.

## pkg/ddl/tests/adminpause

### Tests
- `pkg/ddl/tests/adminpause/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/ddl/tests/adminpause/pause_cancel_test.go` - ddl/tests/adminpause: Tests pause cancel and rerun schema stmt.
- `pkg/ddl/tests/adminpause/pause_negative_test.go` - ddl/tests/adminpause: Tests pause on write conflict.
- `pkg/ddl/tests/adminpause/pause_resume_test.go` - ddl/tests/adminpause: Tests pause and resume schema stmt.

## pkg/ddl/tests/fail

### Tests
- `pkg/ddl/tests/fail/fail_db_test.go` - ddl/tests/fail: Tests halfway cancel operations.
- `pkg/ddl/tests/fail/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/ddl/tests/fastcreatetable

### Tests
- `pkg/ddl/tests/fastcreatetable/fastcreatetable_test.go` - ddl/tests/fastcreatetable: Tests switch fast create table.
- `pkg/ddl/tests/fastcreatetable/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/ddl/tests/fk

### Tests
- `pkg/ddl/tests/fk/foreign_key_test.go` - ddl/tests/fk: Tests create table with foreign key meta info.
- `pkg/ddl/tests/fk/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/ddl/tests/indexmerge

### Tests
- `pkg/ddl/tests/indexmerge/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/ddl/tests/indexmerge/merge_test.go` - ddl/tests/indexmerge: Tests add index merge process.

## pkg/ddl/tests/metadatalock

### Tests
- `pkg/ddl/tests/metadatalock/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/ddl/tests/metadatalock/mdl_test.go` - ddl/tests/metadatalock: Tests MDL basic select.

## pkg/ddl/tests/multivaluedindex

### Tests
- `pkg/ddl/tests/multivaluedindex/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/ddl/tests/multivaluedindex/multi_valued_index_test.go` - ddl/tests/multivaluedindex: Tests create multi-valued index has binary collation.

## pkg/ddl/tests/partition

### Tests
- `pkg/ddl/tests/partition/db_partition_test.go` - ddl/tests/partition: Tests create table with partition.
- `pkg/ddl/tests/partition/error_injection_test.go` - ddl/tests/partition: Tests truncate partition list failures with global index.
- `pkg/ddl/tests/partition/exchange_partition_test.go` - ddl/tests/partition: Tests exchange range columns partition.
- `pkg/ddl/tests/partition/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/ddl/tests/partition/multi_domain_test.go` - ddl/tests/partition: Tests multi schema reorganize partition issue56819.
- `pkg/ddl/tests/partition/placement_test.go` - ddl/tests/partition: Tests partition by with placement.
- `pkg/ddl/tests/partition/reorg_partition_test.go` - ddl/tests/partition: Tests reorg partition failures.

## pkg/ddl/tests/serial

### Tests
- `pkg/ddl/tests/serial/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/ddl/tests/serial/serial_test.go` - ddl/tests/serial: Tests issue23872.

## pkg/ddl/tests/tiflash

### Tests
- `pkg/ddl/tests/tiflash/ddl_tiflash_test.go` - ddl/tests/tiflash: Tests TiFlash no redundant PD rules.
- `pkg/ddl/tests/tiflash/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/ddl/util

### Tests
- `pkg/ddl/util/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/ddl/util/util_test.go` - ddl/util: Tests folder not empty.
