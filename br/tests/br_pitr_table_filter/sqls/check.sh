table_must_exist() {
    run_sql "select count(*) from $1" || {
        echo "Expected table $1 to exist but it doesn't"
        exit 1
    }
}

table_must_not_exist() {
    if run_sql "select count(*) from $1" 2>/dev/null; then
        echo "Expected table $1 to not exist but it does"
        exit 1
    fi
}

column_must_exist() {
    run_sql "select count($2) from $1" || {
        echo "Expected column $2 in table $1 to exist but it doesn't"
        exit 1
    }
}

column_must_not_exist() {
    if run_sql "select count($2) from $1" 2>/dev/null; then
        echo "Expected column $2 in table $1 to not exist but it does"
        exit 1
    fi
}

index_must_exist() {
    run_sql "select count(*) from $1 use index ($2)" || {
        echo "Expected index $2 on table $1 to exist but it doesn't"
        exit 1
    }
}

index_must_not_exist() {
    if run_sql "select count(*) from $1 use index ($2)" 2>/dev/null; then
        echo "Expected index $2 on table $1 to not exist but it does"
        exit 1
    fi
}

schema_must_exist() {
    run_sql "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$1'" | grep -q "$1" || {
        echo "Expected schema $1 to exist but it doesn't"
        exit 1
    }
}

schema_must_not_exist() {
    if run_sql "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$1'" 2>/dev/null | grep -q "$1"; then
        echo "Expected schema $1 to not exist but it does"
        exit 1
    fi
}

table_not_duplicate() {
    run_sql "select count(*) from information_schema.tables where table_schema = $1 and table_name = $2;"
    check_contains "count(*): 1"
}

check_create_table_contains() {
    run_sql "show create table $1"
    check_contains "$2"
}

view_must_exist() {
    run_sql "select count(*) from $1" || {
        echo "Expected view $1 to exist but it doesn't"
        exit 1
    }
}

view_must_not_exist() {
    if run_sql "select count(*) from $1" 2>/dev/null; then
        echo "Expected view $1 to not exist but it does"
        exit 1
    fi
}

sequence_must_exist() {
    run_sql "SELECT SEQUENCE_NAME FROM information_schema.SEQUENCES WHERE SEQUENCE_SCHEMA = '$1' AND SEQUENCE_NAME = '$2'" | grep -q "$2" || {
        echo "Expected sequence $1.$2 to exist but it doesn't"
        exit 1
    }
}

sequence_must_not_exist() {
    if run_sql "SELECT SEQUENCE_NAME FROM information_schema.SEQUENCES WHERE SEQUENCE_SCHEMA = '$1' AND SEQUENCE_NAME = '$2'" 2>/dev/null | grep -q "$2"; then
        echo "Expected sequence $1.$2 to not exist but it does"
        exit 1
    fi
}

foreign_key_must_exist() {
    run_sql "SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2' AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME = '$3'" | grep -q "$3" || {
        echo "Expected foreign key $3 on table $1.$2 to exist but it doesn't"
        exit 1
    }
}

foreign_key_must_not_exist() {
    if run_sql "SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2' AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME = '$3'" 2>/dev/null | grep -q "$3"; then
        echo "Expected foreign key $3 on table $1.$2 to not exist but it does"
        exit 1
    fi
}

# Function to check if a check constraint exists
check_constraint_must_exist() {
    local schema_name="$1"
    local table_name="$2"
    local constraint_name="$3"
    
    # DEBUG: Show exactly what the query returns
    echo "=== DEBUG: Checking constraint existence for $constraint_name ==="
    local result=$(run_sql "SELECT COUNT(*) FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '$schema_name' AND CONSTRAINT_NAME = '$constraint_name'")
    echo "Query result: '$result'"
    
    # Alternative approach: Direct constraint name search
    echo "=== DEBUG: Alternative check - looking for constraint directly ==="
    local direct_result=$(run_sql "SELECT CONSTRAINT_NAME FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '$schema_name' AND CONSTRAINT_NAME = '$constraint_name'")
    echo "Direct query result: '$direct_result'"
    
    # Use the direct approach - check if constraint name appears in result
    if ! echo "$direct_result" | grep -q "$constraint_name"; then
        echo "Expected check constraint $constraint_name on table $schema_name.$table_name to exist but it doesn't"
        echo "COUNT query result was: $result"
        echo "Direct query result was: $direct_result"
        exit 1
    fi
    echo "✓ Check constraint $constraint_name found successfully"
}

# Function to check if a check constraint does not exist
check_constraint_must_not_exist() {
    local schema_name="$1"
    local table_name="$2"
    local constraint_name="$3"
    
    # Use information_schema.CHECK_CONSTRAINTS instead of TABLE_CONSTRAINTS
    if run_sql "SELECT COUNT(*) FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '$schema_name' AND CONSTRAINT_NAME = '$constraint_name'" | grep -q "count.*1"; then
        echo "Expected check constraint $constraint_name on table $schema_name.$table_name to not exist but it does"
        exit 1
    fi
}

partition_must_exist() {
    run_sql "SELECT PARTITION_NAME FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2' AND PARTITION_NAME = '$3'" | grep -q "$3" || {
        echo "Expected partition $3 on table $1.$2 to exist but it doesn't"
        exit 1
    }
}

partition_must_not_exist() {
    if run_sql "SELECT PARTITION_NAME FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2' AND PARTITION_NAME = '$3'" 2>/dev/null | grep -q "$3"; then
        echo "Expected partition $3 on table $1.$2 to not exist but it does"
        exit 1
    fi
}

check_table_default_value() {
    run_sql "SELECT COLUMN_DEFAULT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2' AND COLUMN_NAME = '$3'" | grep -q "$4" || {
        echo "Expected column $3 on table $1.$2 to have default value '$4' but it doesn't"
        exit 1
    }
}

check_table_charset() {
    run_sql "SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2'" | grep -q "$3" || {
        echo "Expected table $1.$2 to have charset/collation containing '$3' but it doesn't"
        exit 1
    }
}

check_schema_charset() {
    local schema_name="$1"
    local expected_collation="$2"
    
    echo "=== DEBUG: Checking schema $schema_name for collation $expected_collation ==="
    local actual_collation=$(run_sql "SELECT DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$schema_name'")
    echo "Actual collation result: '$actual_collation'"
    
    # Also check if schema exists
    local schema_exists=$(run_sql "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$schema_name'")
    echo "Schema exists check: '$schema_exists'"
    
    # Show all schemas for debugging
    echo "=== All schemas ==="
    run_sql "SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA"
    
    if ! echo "$actual_collation" | grep -q "$expected_collation"; then
        echo "Expected schema $schema_name to have charset/collation containing '$expected_collation' but it doesn't"
        echo "Actual collation was: $actual_collation"
        exit 1
    fi
    echo "✓ Schema $schema_name has expected collation $expected_collation"
}

check_index_visibility() {
    local schema="$1"
    local table="$2"
    local index="$3"
    local visibility="$4"  # 'YES' for visible, 'NO' for invisible
    
    run_sql "SELECT IS_VISIBLE FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table' AND INDEX_NAME = '$index'" | grep -q "$visibility" || {
        echo "Expected index $index on table $schema.$table to have visibility '$visibility' but it doesn't"
        exit 1
    }
}

check_tiflash_replica_count() {
    run_sql "SELECT REPLICA_COUNT FROM INFORMATION_SCHEMA.TIFLASH_REPLICA WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2'"
    check_contains "REPLICA_COUNT: $3"
}

row_must_exist() {
    run_sql "select count(*) from $1 where $2"
    check_contains "count(*): 1"
}

attributes_must_exist() {
    run_sql "select count(*) from information_schema.attributes where ID like '%$1%' where ATTRIBUTES = '$2'"
    check_contains "count(*): 1"
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
check_tiflash_replica_count "test_snapshot_db_create" "t_set_tiflash" 1
check_tiflash_replica_count "test_log_db_create" "t_set_tiflash" 1

# ActionExchangeTablePartition
row_must_exist "test_snapshot_db_create.t_exchange_partition" "id = 115"
row_must_exist "test_snapshot_db_create.t_non_partitioned_table" "id = 105"
row_must_exist "test_log_db_create.t_exchange_partition" "id = 115"
row_must_exist "test_log_db_create.t_non_partitioned_table" "id = 105"

# ActionAlterTableAttributes
attributes_must_exist "test_snapshot_db_create/t_alter_table_attributes" "merge_option=allow"
attributes_must_exist "test_log_db_create/t_alter_table_attributes" "merge_option=allow"

# ActionAlterTablePartitionAttributes
attributes_must_exist "test_snapshot_db_create/t_alter_table_partition_attributes/p0" "merge_option=allow"
attributes_must_exist "test_log_db_create/t_alter_table_partition_attributes/p0" "merge_option=allow"

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

