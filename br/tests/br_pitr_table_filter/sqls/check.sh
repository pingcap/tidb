#!/bin/bash

set -eu

table_must_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$(echo $1 | cut -d. -f1)' AND TABLE_NAME = '$(echo $1 | cut -d. -f2)'"
    check_contains "COUNT(*): 1"
}

table_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$(echo $1 | cut -d. -f1)' AND TABLE_NAME = '$(echo $1 | cut -d. -f2)'"
    check_contains "COUNT(*): 0"
}

column_must_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '$(echo $1 | cut -d. -f1)' AND TABLE_NAME = '$(echo $1 | cut -d. -f2)' AND COLUMN_NAME = '$2'"
    check_contains "COUNT(*): 1"
}

column_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '$(echo $1 | cut -d. -f1)' AND TABLE_NAME = '$(echo $1 | cut -d. -f2)' AND COLUMN_NAME = '$2'"
    check_contains "COUNT(*): 0"
}

index_must_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '$(echo $1 | cut -d. -f1)' AND TABLE_NAME = '$(echo $1 | cut -d. -f2)' AND INDEX_NAME = '$2'"
    check_contains "COUNT(*): 1"
}

index_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '$(echo $1 | cut -d. -f1)' AND TABLE_NAME = '$(echo $1 | cut -d. -f2)' AND INDEX_NAME = '$2'"
    check_contains "COUNT(*): 0"
}

schema_must_exist() {
    run_sql "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$1'"
    check_contains "SCHEMA_NAME: $1"
}

schema_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$1'"
    check_contains "COUNT(*): 0"
}

table_not_duplicate() {
    run_sql "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2;"
    check_contains "COUNT(*): 1"
}

check_create_table_contains() {
    run_sql "SHOW CREATE TABLE $1"
    check_contains "$2"
}

view_must_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '$(echo $1 | cut -d. -f1)' AND TABLE_NAME = '$(echo $1 | cut -d. -f2)'"
    check_contains "COUNT(*): 1"
}

view_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '$(echo $1 | cut -d. -f1)' AND TABLE_NAME = '$(echo $1 | cut -d. -f2)'"
    check_contains "COUNT(*): 0"
}

sequence_must_exist() {
    run_sql "SELECT SEQUENCE_NAME FROM information_schema.SEQUENCES WHERE SEQUENCE_SCHEMA = '$1' AND SEQUENCE_NAME = '$2'"
    check_contains "SEQUENCE_NAME: $2"
}

sequence_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.SEQUENCES WHERE SEQUENCE_SCHEMA = '$1' AND SEQUENCE_NAME = '$2'"
    check_contains "COUNT(*): 0"
}

foreign_key_must_exist() {
    run_sql "SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2' AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME = '$3'"
    check_contains "CONSTRAINT_NAME: $3"
}

foreign_key_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2' AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME = '$3'"
    check_contains "COUNT(*): 0"
}

# Function to check if a check constraint exists
check_constraint_must_exist() {
    local schema_name="$1"
    local table_name="$2"
    local constraint_name="$3"
    
    run_sql "SELECT COUNT(*) FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '$schema_name' AND CONSTRAINT_NAME = '$constraint_name'"
    check_contains "COUNT(*): 1"
}

# Function to check if a check constraint does not exist
check_constraint_must_not_exist() {
    local schema_name="$1"
    local table_name="$2"
    local constraint_name="$3"
    
    run_sql "SELECT COUNT(*) FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '$schema_name' AND CONSTRAINT_NAME = '$constraint_name'"
    check_contains "COUNT(*): 0"
}

partition_must_exist() {
    run_sql "SELECT PARTITION_NAME FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2' AND PARTITION_NAME = '$3'"
    check_contains "PARTITION_NAME: $3"
}

partition_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2' AND PARTITION_NAME = '$3'"
    check_contains "COUNT(*): 0"
}

check_table_default_value() {
    run_sql "SELECT COLUMN_DEFAULT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2' AND COLUMN_NAME = '$3'"
    check_contains "COLUMN_DEFAULT: $4"
}

check_table_charset() {
    run_sql "SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2'"
    check_contains "$3"
}

check_schema_charset() {
    local schema_name="$1"
    local expected_collation="$2"
    
    run_sql "SELECT DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$schema_name'"
    check_contains "$expected_collation"
}

check_index_visibility() {
    local schema="$1"
    local table="$2"
    local index="$3"
    local visibility="$4"  # 'YES' for visible, 'NO' for invisible
    
    run_sql "SELECT IS_VISIBLE FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table' AND INDEX_NAME = '$index'"
    check_contains "IS_VISIBLE: $visibility"
}

check_tiflash_replica_count() {
    run_sql "SELECT REPLICA_COUNT FROM INFORMATION_SCHEMA.TIFLASH_REPLICA WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2'"
    check_contains "REPLICA_COUNT: $3"
}

row_must_exist() {
    run_sql "SELECT COUNT(*) FROM $1 WHERE $2"
    check_contains "COUNT(*): 1"
}

attributes_must_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.attributes WHERE ID LIKE '%$1%' AND ATTRIBUTES = '$2'"
    check_contains "COUNT(*): 1"
}

placement_policy_must_exist() {
    run_sql "SELECT POLICY_NAME FROM information_schema.PLACEMENT_POLICIES WHERE POLICY_NAME = '$1'"
    check_contains "POLICY_NAME: $1"
}

placement_policy_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.PLACEMENT_POLICIES WHERE POLICY_NAME = '$1'"
    check_contains "COUNT(*): 0"
}

check_table_stats_options() {
    run_sql "SHOW CREATE TABLE $1"
    check_contains "$2"
}

check_table_ttl() {
    run_sql "SHOW CREATE TABLE $1"
    check_contains "TTL="
}

check_table_no_ttl() {
    run_sql "SHOW CREATE TABLE $1"
    check_not_contains "TTL="
}

check_table_cache_status() {
    local table_name="$1"
    local expected_status="$2"
    
    run_sql "SHOW CREATE TABLE $table_name"
    
    if [ "$expected_status" = "ENABLE" ]; then
        check_contains "CACHED ON"
    else
        check_not_contains "CACHED ON"
    fi
}

check_table_recovered() {
    run_sql "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$(echo $1 | cut -d. -f1)' AND TABLE_NAME = '$(echo $1 | cut -d. -f2)'"
    check_contains "COUNT(*): 1"
}

resource_group_must_exist() {
    run_sql "SELECT RESOURCE_GROUP_NAME FROM information_schema.RESOURCE_GROUPS WHERE RESOURCE_GROUP_NAME = '$1'"
    check_contains "RESOURCE_GROUP_NAME: $1"
}

resource_group_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.RESOURCE_GROUPS WHERE RESOURCE_GROUP_NAME = '$1'"
    check_contains "COUNT(*): 0"
}

check_resource_group_ru_per_sec() {
    run_sql "SELECT RU_PER_SEC FROM information_schema.RESOURCE_GROUPS WHERE RESOURCE_GROUP_NAME = '$1'"
    check_contains "RU_PER_SEC: $2"
}

# ActionCreateSchema
schema_must_exist "test_log_db_create"

# ActionDropSchema
schema_must_not_exist "test_log_to_be_deleted"
table_must_not_exist "test_log_to_be_deleted.t1"
schema_must_not_exist "test_snapshot_db_to_be_deleted"
table_must_not_exist "test_snapshot_db_to_be_deleted.t1"

# ActionCreateTable
table_must_exist "test_log_db_create.t1";
table_must_exist "test_snapshot_db_create.t1";

# ActionCreateTables

# ActionDropTable
table_must_not_exist "test_snapshot_db_create.t_to_be_deleted";
table_must_not_exist "test_log_db_create.t_to_be_deleted";

# ActionAddColumn
column_must_exist "test_snapshot_db_create.t_add_column" "a"
column_must_exist "test_log_db_create.t_add_column" "a"

# ActionDropColumn
column_must_not_exist "test_snapshot_db_create.t_drop_column" "a"
column_must_not_exist "test_log_db_create.t_drop_column" "a"

# ActionAddIndex
index_must_exist "test_snapshot_db_create.t_add_index" "i1"
index_must_exist "test_snapshot_db_create.t_add_unique_key" "i1"
index_must_exist "test_log_db_create.t_add_index" "i1"
index_must_exist "test_log_db_create.t_add_unique_key" "i1"

# ActionDropIndex
index_must_not_exist "test_snapshot_db_create.t_drop_index" "i1"
index_must_not_exist "test_snapshot_db_create.t_drop_unique_key" "i1"
index_must_not_exist "test_log_db_create.t_drop_index" "i1"
index_must_not_exist "test_log_db_create.t_drop_unique_key" "i1"


# ActionTruncateTable
table_must_exist "test_snapshot_db_create.t_to_be_truncated"
table_must_exist "test_log_db_create.t_to_be_truncated"

# ActionModifyColumn
check_create_table_contains "test_snapshot_db_create.t_modify_column" "bigint"
check_create_table_contains "test_log_db_create.t_modify_column" "bigint"


# ActionRebaseAutoID

# ActionRenameTable
table_must_exist "test_snapshot_db_create.t_rename_b"
table_must_exist "test_log_db_create.t_rename_b"

# ActionRenameTables
table_must_exist "test_snapshot_db_create.t_renames_c"
table_must_exist "test_snapshot_db_create.t_renames_a"
table_must_not_exist "test_snapshot_db_create.t_renames_b"
table_must_exist "test_snapshot_db_create.t_renames_aaa"
table_must_exist "test_snapshot_db_create.t_renames_bbb"
table_must_not_exist "test_snapshot_db_create.t_renames_aa"
table_must_not_exist "test_snapshot_db_create.t_renames_bb"
table_must_exist "test_log_db_create.t_renames_c"
table_must_exist "test_log_db_create.t_renames_a"
table_must_not_exist "test_log_db_create.t_renames_b"
table_must_exist "test_log_db_create.t_renames_aaa"
table_must_exist "test_log_db_create.t_renames_bbb"
table_must_not_exist "test_log_db_create.t_renames_aa"
table_must_not_exist "test_log_db_create.t_renames_bb"

# ActionModifyTableComment
check_create_table_contains "test_snapshot_db_create.t_modify_comment" "after modify comment"
check_create_table_contains "test_log_db_create.t_modify_comment" "after modify comment"

# ActionRenameIndex
index_must_exist "test_snapshot_db_create.t_rename_index" "i2"
index_must_not_exist "test_snapshot_db_create.t_rename_index" "i1"
index_must_exist "test_log_db_create.t_rename_index" "i2"
index_must_not_exist "test_log_db_create.t_rename_index" "i1"

# ActionAddPrimaryKey
index_must_exist "test_snapshot_db_create.t_add_primary_key" "primary"
index_must_exist "test_log_db_create.t_add_primary_key" "primary"

# ActionDropPrimaryKey
index_must_not_exist "test_snapshot_db_create.t_drop_primary_key" "primary"
index_must_not_exist "test_log_db_create.t_drop_primary_key" "primary"

# ActionAddForeignKey
# foreign_key_must_exist "test_snapshot_db_create" "t_fk_child_add" "fk_added"
# foreign_key_must_exist "test_log_db_create" "t_fk_child_add" "fk_added"

# ActionDropForeignKey
# TODO: Known issue - DROP FOREIGN KEY conflicts with auto-created indexes during PITR restore
# foreign_key_must_not_exist "test_snapshot_db_create" "t_fk_child_drop" "fk_to_be_dropped"
# foreign_key_must_not_exist "test_log_db_create" "t_fk_child_drop" "fk_to_be_dropped"

# ActionCreateView
view_must_exist "test_log_db_create.v_view_created"

# ActionDropView
view_must_not_exist "test_snapshot_db_create.v_view_to_be_dropped"
view_must_not_exist "test_log_db_create.v_view_to_be_dropped"

# ActionCreateSequence
sequence_must_exist "test_log_db_create" "seq_created"

# ActionAlterSequence
sequence_must_exist "test_snapshot_db_create" "seq_to_be_altered"
sequence_must_exist "test_log_db_create" "seq_to_be_altered"

# ActionDropSequence
sequence_must_not_exist "test_snapshot_db_create" "seq_to_be_dropped"
sequence_must_not_exist "test_log_db_create" "seq_to_be_dropped"

# ActionAddTablePartition
partition_must_exist "test_snapshot_db_create" "t_add_partition" "p1"
partition_must_exist "test_log_db_create" "t_add_partition" "p1"

# ActionDropTablePartition
partition_must_not_exist "test_snapshot_db_create" "t_drop_partition" "p_to_be_dropped"
partition_must_not_exist "test_log_db_create" "t_drop_partition" "p_to_be_dropped"

# ActionAddCheckConstraint
check_constraint_must_exist test_snapshot_db_create t_add_check chk_age_added
check_constraint_must_exist test_log_db_create t_add_check chk_age_added

# ActionDropCheckConstraint
check_constraint_must_not_exist "test_snapshot_db_create" "t_drop_check" "chk_age_to_be_dropped"
check_constraint_must_not_exist "test_log_db_create" "t_drop_check" "chk_age_to_be_dropped"

# ActionAlterCheckConstraint
check_create_table_contains "test_snapshot_db_create.t_alter_check" "80016 NOT ENFORCED"
check_create_table_contains "test_log_db_create.t_alter_check" "80016 NOT ENFORCED"

# ActionSetDefaultValue
check_table_default_value "test_snapshot_db_create" "t_set_default" "status" "active"
check_table_default_value "test_log_db_create" "t_set_default" "status" "active"

# ActionModifyTableCharsetAndCollate
check_table_charset "test_snapshot_db_create" "t_modify_charset" "utf8mb4_unicode_ci"
check_table_charset "test_log_db_create" "t_modify_charset" "utf8mb4_unicode_ci"

# ActionModifySchemaCharsetAndCollate
check_schema_charset "test_snapshot_db_charset" "utf8mb4_unicode_ci"
check_schema_charset "test_log_db_charset" "utf8mb4_unicode_ci"

# ActionTruncateTablePartition
table_must_exist "test_snapshot_db_create.t_truncate_partition"
table_must_exist "test_log_db_create.t_truncate_partition"
partition_must_exist "test_snapshot_db_create" "t_truncate_partition" "p_to_be_truncated"
partition_must_exist "test_log_db_create" "t_truncate_partition" "p_to_be_truncated"

# ActionLockTable & ActionUnlockTable
table_must_exist "test_snapshot_db_create.t_to_be_locked"
table_must_exist "test_log_db_create.t_to_be_locked"

# ActionAlterIndexVisibility
index_must_exist "test_snapshot_db_create.t_index_visibility" "idx_name"
index_must_exist "test_log_db_create.t_index_visibility" "idx_name"
check_index_visibility "test_snapshot_db_create" "t_index_visibility" "idx_name" "NO"
check_index_visibility "test_log_db_create" "t_index_visibility" "idx_name" "NO"

# ActionRebaseAutoID
check_create_table_contains "test_snapshot_db_create.t_rebase_auto_id" "AUTO_INCREMENT=60000"
check_create_table_contains "test_log_db_create.t_rebase_auto_id" "AUTO_INCREMENT=60000"

# ActionModifyTableAutoIDCache
check_create_table_contains "test_snapshot_db_create.t_auto_id_cache" "AUTO_ID_CACHE=60000"
check_create_table_contains "test_log_db_create.t_auto_id_cache" "AUTO_ID_CACHE=60000"

# ActionShardRowID
check_create_table_contains "test_snapshot_db_create.t_shard_row" "SHARD_ROW_ID_BITS=4"
check_create_table_contains "test_log_db_create.t_shard_row" "SHARD_ROW_ID_BITS=4"

# ActionRebaseAutoRandomBase
check_create_table_contains "test_snapshot_db_create.t_auto_random" "AUTO_RANDOM_BASE=60000"
check_create_table_contains "test_log_db_create.t_auto_random" "AUTO_RANDOM_BASE=60000"

# ActionSetTiFlashReplica
# TiFlash replica checks commented out since TiFlash may not be available in test environment
# check_tiflash_replica_count "test_snapshot_db_create" "t_set_tiflash" 1
# check_tiflash_replica_count "test_log_db_create" "t_set_tiflash" 1

# ActionExchangeTablePartition
row_must_exist "test_snapshot_db_create.t_exchange_partition" "id = 115"
row_must_exist "test_snapshot_db_create.t_non_partitioned_table" "id = 105"
row_must_exist "test_log_db_create.t_exchange_partition" "id = 115"
row_must_exist "test_log_db_create.t_non_partitioned_table" "id = 105"

# ActionAlterTableAttributes
# attributes stored in PD
# attributes_must_exist "test_snapshot_db_create/t_alter_table_attributes" "merge_option=allow"
# attributes_must_exist "test_log_db_create/t_alter_table_attributes" "merge_option=allow"

# # ActionAlterTablePartitionAttributes
# attributes_must_exist "test_snapshot_db_create/t_alter_table_partition_attributes/p0" "merge_option=allow"
# attributes_must_exist "test_log_db_create/t_alter_table_partition_attributes/p0" "merge_option=allow"

# ActionReorganizePartition
check_create_table_contains "test_snapshot_db_create.t_reorganize_partition" "pnew"
check_create_table_contains "test_log_db_create.t_reorganize_partition" "pnew"
row_must_exist "test_snapshot_db_create.t_reorganize_partition" "id=50"
row_must_exist "test_snapshot_db_create.t_reorganize_partition" "id=150"
row_must_exist "test_snapshot_db_create.t_reorganize_partition" "id=250"
row_must_exist "test_log_db_create.t_reorganize_partition" "id=50"
row_must_exist "test_log_db_create.t_reorganize_partition" "id=150"
row_must_exist "test_log_db_create.t_reorganize_partition" "id=250"

# ActionAlterTablePartitioning
check_create_table_contains "test_snapshot_db_create.t_alter_table_partitioning" "p0"
check_create_table_contains "test_snapshot_db_create.t_alter_table_partitioning" "p1"
check_create_table_contains "test_log_db_create.t_alter_table_partitioning" "p0"
check_create_table_contains "test_log_db_create.t_alter_table_partitioning" "p1"

# ActionRemovePartitioning
partition_must_not_exist "test_snapshot_db_create" "t_remove_partitioning" "p0"
partition_must_not_exist "test_snapshot_db_create" "t_remove_partitioning" "p1"
partition_must_not_exist "test_log_db_create" "t_remove_partitioning" "p0"
partition_must_not_exist "test_log_db_create" "t_remove_partitioning" "p1"

# ActionAlterTTLInfo
check_table_ttl "test_snapshot_db_create.t_add_ttl"
check_table_ttl "test_log_db_create.t_add_ttl"

# ActionAlterTTLRemove
check_table_no_ttl "test_snapshot_db_create.t_remove_ttl"
check_table_no_ttl "test_log_db_create.t_remove_ttl"

# ActionAlterCacheTable
check_table_cache_status "test_snapshot_db_create.t_alter_cache" "ENABLE"
check_table_cache_status "test_log_db_create.t_alter_cache" "ENABLE"

# ActionAlterNoCacheTable
check_table_cache_status "test_snapshot_db_create.t_alter_no_cache" "DISABLE"
check_table_cache_status "test_log_db_create.t_alter_no_cache" "DISABLE"

# Foreign Key Cleanup Test
# Verify that both tables are properly dropped and no stale foreign key references remain
table_must_not_exist "test_snapshot_db_create.t_fk_parent_cleanup"
table_must_not_exist "test_snapshot_db_create.t_fk_child_cleanup"

# Test that we can create new tables with the same names without foreign key conflicts
# This would fail if stale foreign key references remained in the infoschema
run_sql "SET GLOBAL tidb_enable_foreign_key = ON"
run_sql "CREATE TABLE test_snapshot_db_create.t_fk_parent_cleanup (id int primary key, name varchar(50))"
run_sql "CREATE TABLE test_snapshot_db_create.t_fk_child_cleanup (id int primary key, parent_id int, constraint fk_test foreign key (parent_id) references test_snapshot_db_create.t_fk_parent_cleanup(id))"
run_sql "DROP TABLE test_snapshot_db_create.t_fk_child_cleanup"
run_sql "DROP TABLE test_snapshot_db_create.t_fk_parent_cleanup"
run_sql "SET GLOBAL tidb_enable_foreign_key = OFF"



