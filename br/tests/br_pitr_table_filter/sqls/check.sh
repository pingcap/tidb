#!/bin/bash

set -eu

table_must_exist() {
    local table_name="$1"
    local schema=$(echo $table_name | cut -d. -f1)
    local table=$(echo $table_name | cut -d. -f2)
    
    # Method 1: information_schema.TABLES check
    run_sql "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table'"
    check_contains "COUNT(*): 1"
    
    # Method 2: Direct table access
    run_sql "SELECT COUNT(*) FROM $table_name LIMIT 0"
    
    # Method 3: SHOW TABLES verification
    run_sql "SHOW TABLES FROM $schema LIKE '$table'"
    check_contains "$table"
    
    # Method 4: SHOW CREATE TABLE
    run_sql "SHOW CREATE TABLE $table_name"
    check_contains "CREATE TABLE"
    
    # Method 5: Verify it's a BASE TABLE (not a view)
    run_sql "SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table'"
    check_contains "TABLE_TYPE: BASE TABLE"
    
    # Method 6: ADMIN CHECK TABLE for integrity
    run_sql "ADMIN CHECK TABLE $table_name"
    
    # Method 7: Table ID round-trip verification (name -> ID -> name)
    run_sql "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TIDB_TABLE_ID = (SELECT TIDB_TABLE_ID FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table')"
    check_contains "TABLE_SCHEMA: $schema"
    check_contains "TABLE_NAME: $table"
}

table_must_not_exist() {
    local table_name="$1"
    local schema=$(echo $table_name | cut -d. -f1)
    local table=$(echo $table_name | cut -d. -f2)
    
    # Method 1: information_schema.TABLES check
    run_sql "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table'"
    check_contains "COUNT(*): 0"
    
    # Method 2: Direct table access should fail
    if run_sql "SELECT COUNT(*) FROM $table_name LIMIT 0" 2>/dev/null; then
        echo "ERROR: Table $table_name should not exist but is accessible"
        exit 1
    fi
    
    # Method 3: SHOW TABLES should not contain the table
    if run_sql "SHOW TABLES FROM $schema LIKE '$table'" 2>/dev/null | grep -q "$table"; then
        echo "ERROR: Table $table should not exist but appears in SHOW TABLES"
        exit 1
    fi
    
    # Method 4: SHOW CREATE TABLE should fail
    if run_sql "SHOW CREATE TABLE $table_name" 2>/dev/null; then
        echo "ERROR: Table $table_name should not exist but SHOW CREATE TABLE succeeded"
        exit 1
    fi
}

column_must_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '$(echo $1 | cut -d. -f1)' AND TABLE_NAME = '$(echo $1 | cut -d. -f2)' AND COLUMN_NAME = '$2'"
    check_contains "COUNT(*): 1"
    run_sql "SELECT $2 FROM $1 LIMIT 0"
}

column_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '$(echo $1 | cut -d. -f1)' AND TABLE_NAME = '$(echo $1 | cut -d. -f2)' AND COLUMN_NAME = '$2'"
    check_contains "COUNT(*): 0"
    if run_sql "SELECT $2 FROM $1 LIMIT 0" 2>/dev/null; then
        echo "ERROR: Column $2 in table $1 should not exist but is accessible"
        exit 1
    fi
}

index_must_exist() {
    local table_name="$1"
    local index_name="$2"
    local schema=$(echo $table_name | cut -d. -f1)
    local table=$(echo $table_name | cut -d. -f2)
    
    # Method 1: information_schema.STATISTICS check
    run_sql "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table' AND INDEX_NAME = '$index_name'"
    check_contains "COUNT(*): 1"
    
    # Method 2: SHOW INDEX verification
    run_sql "SHOW INDEX FROM $table_name WHERE Key_name = '$index_name'"
    check_contains "$index_name"
    
    # Method 3: ADMIN CHECK INDEX (skip for PRIMARY key as it has special syntax)
    if [ "$index_name" != "PRIMARY" ]; then
        run_sql "ADMIN CHECK INDEX $table_name $index_name"
    else
        # For PRIMARY key, use a different verification approach
        run_sql "ADMIN CHECK TABLE $table_name"
    fi
    
    # Method 4: Check index properties
    run_sql "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table' AND INDEX_NAME = '$index_name' ORDER BY SEQ_IN_INDEX"
    check_contains "INDEX_NAME: $index_name"
    
    # Method 5: ADMIN CHECK TABLE
    run_sql "ADMIN CHECK TABLE $table_name"
}

index_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '$(echo $1 | cut -d. -f1)' AND TABLE_NAME = '$(echo $1 | cut -d. -f2)' AND INDEX_NAME = '$2'"
    check_contains "COUNT(*): 0"
    if run_sql "SHOW INDEX FROM $1 WHERE Key_name = '$2'" 2>/dev/null | grep -q "$2"; then
        echo "ERROR: Index $2 in table $1 should not exist but was found"
        exit 1
    fi
    run_sql "ADMIN CHECK TABLE $1"
}

schema_must_exist() {
    local schema_name="$1"
    
    # Method 1: information_schema.SCHEMATA check
    run_sql "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$schema_name'"
    check_contains "SCHEMA_NAME: $schema_name"
    
    # Method 2: USE schema test
    run_sql "USE $schema_name; SELECT 1 as success"
    check_contains "success: 1"
    
    # Method 3: SHOW DATABASES verification
    run_sql "SHOW DATABASES LIKE '$schema_name'"
    check_contains "$schema_name"
    
    # Method 4: SHOW CREATE DATABASE
    run_sql "SHOW CREATE DATABASE $schema_name"
    check_contains "CREATE DATABASE"
    check_contains "$schema_name"
}

schema_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$1'"
    check_contains "COUNT(*): 0"
    if run_sql "USE $1" 2>/dev/null; then
        echo "ERROR: Schema $1 should not exist but is accessible"
        exit 1
    fi
}

check_create_table_contains() {
    run_sql "SHOW CREATE TABLE $1"
    check_contains "$2"
}

view_must_exist() {
    local view_name="$1"
    local schema=$(echo $view_name | cut -d. -f1)
    local view=$(echo $view_name | cut -d. -f2)
    
    # Method 1: information_schema.VIEWS check
    run_sql "SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$view'"
    check_contains "COUNT(*): 1"
    
    # Method 2: Direct view access
    run_sql "SELECT COUNT(*) FROM $view_name LIMIT 0"
    
    # Method 3: SHOW CREATE VIEW
    run_sql "SHOW CREATE VIEW $view_name"
    check_contains "CREATE"
    check_contains "VIEW"
    
    # Method 4: Verify it's a VIEW type (not a table)
    run_sql "SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$view'"
    check_contains "TABLE_TYPE: VIEW"
    
    # Method 5: Table ID round-trip verification (views have table IDs in TiDB)
    run_sql "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TIDB_TABLE_ID = (SELECT TIDB_TABLE_ID FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$view')"
    check_contains "TABLE_SCHEMA: $schema"
    check_contains "TABLE_NAME: $view"
}

view_must_not_exist() {
    local view_name="$1"
    local schema=$(echo $view_name | cut -d. -f1)
    local view=$(echo $view_name | cut -d. -f2)
    
    # Method 1: information_schema.VIEWS check
    run_sql "SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$view'"
    check_contains "COUNT(*): 0"
    
    # Method 2: Direct view access should fail
    if run_sql "SELECT COUNT(*) FROM $view_name LIMIT 0" 2>/dev/null; then
        echo "ERROR: View $view_name should not exist but is accessible"
        exit 1
    fi
    
    # Method 3: SHOW CREATE VIEW should fail
    if run_sql "SHOW CREATE VIEW $view_name" 2>/dev/null; then
        echo "ERROR: View $view_name should not exist but SHOW CREATE VIEW succeeded"
        exit 1
    fi
    
    # Method 4: Should not appear in information_schema.TABLES as VIEW
    run_sql "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$view' AND TABLE_TYPE = 'VIEW'"
    check_contains "COUNT(*): 0"
}

sequence_must_exist() {
    local schema="$1"
    local sequence="$2"
    
    # Method 1: information_schema.SEQUENCES check
    run_sql "SELECT SEQUENCE_NAME FROM information_schema.SEQUENCES WHERE SEQUENCE_SCHEMA = '$schema' AND SEQUENCE_NAME = '$sequence'"
    check_contains "SEQUENCE_NAME: $sequence"
    
    # Method 2: SHOW CREATE SEQUENCE
    run_sql "SHOW CREATE SEQUENCE $schema.$sequence"
    check_contains "CREATE SEQUENCE"
    
    # Method 3: Test sequence functionality
    run_sql "SELECT nextval($schema.$sequence) as next_value"
}

sequence_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.SEQUENCES WHERE SEQUENCE_SCHEMA = '$1' AND SEQUENCE_NAME = '$2'"
    check_contains "COUNT(*): 0"
    if run_sql "SHOW CREATE SEQUENCE $1.$2" 2>/dev/null; then
        echo "ERROR: Sequence $1.$2 should not exist but is accessible"
        exit 1
    fi
}

foreign_key_must_exist() {
    local schema="$1"
    local table="$2"  
    local constraint_name="$3"
    
    # Method 1: information_schema.TABLE_CONSTRAINTS check
    run_sql "SELECT CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table' AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME = '$constraint_name'"
    check_contains "CONSTRAINT_NAME: $constraint_name"
    
    # Method 2: SHOW CREATE TABLE verification
    run_sql "SHOW CREATE TABLE $schema.$table"
    check_contains "CONSTRAINT \`$constraint_name\`"
    
    # Method 3: Check information_schema.KEY_COLUMN_USAGE
    run_sql "SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE CONSTRAINT_SCHEMA = '$schema' AND TABLE_NAME = '$table' AND CONSTRAINT_NAME = '$constraint_name'"
    check_contains "CONSTRAINT_NAME: $constraint_name"
    
    # Method 4: Check information_schema.REFERENTIAL_CONSTRAINTS
    run_sql "SELECT CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '$schema' AND CONSTRAINT_NAME = '$constraint_name'"
    check_contains "CONSTRAINT_NAME: $constraint_name"
}

foreign_key_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2' AND CONSTRAINT_TYPE = 'FOREIGN KEY' AND CONSTRAINT_NAME = '$3'"
    check_contains "COUNT(*): 0"
    run_sql "SHOW CREATE TABLE $1.$2"
    check_not_contains "CONSTRAINT \`$3\`"
}

check_constraint_must_exist() {
    local schema_name="$1"
    local table_name="$2"
    local constraint_name="$3"
    run_sql "SELECT COUNT(*) FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '$schema_name' AND CONSTRAINT_NAME = '$constraint_name'"
    check_contains "COUNT(*): 1"
    run_sql "SHOW CREATE TABLE $schema_name.$table_name"
    check_contains "CONSTRAINT \`$constraint_name\`"
}

check_constraint_must_not_exist() {
    local schema_name="$1"
    local table_name="$2"
    local constraint_name="$3"
    run_sql "SELECT COUNT(*) FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '$schema_name' AND CONSTRAINT_NAME = '$constraint_name'"
    check_contains "COUNT(*): 0"
    run_sql "SHOW CREATE TABLE $schema_name.$table_name"
    check_not_contains "CONSTRAINT \`$constraint_name\`"
}

partition_must_exist() {
    local schema="$1"
    local table="$2"
    local partition="$3"
    
    # Method 1: information_schema.PARTITIONS check
    run_sql "SELECT PARTITION_NAME FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table' AND PARTITION_NAME = '$partition'"
    check_contains "PARTITION_NAME: $partition"
    
    # Method 2: Direct partition access
    run_sql "SELECT COUNT(*) FROM $schema.$table PARTITION($partition) LIMIT 0"
    
    # Method 3: Partition properties verification
    run_sql "SELECT TABLE_SCHEMA, TABLE_NAME, PARTITION_NAME, PARTITION_ORDINAL_POSITION FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table' AND PARTITION_NAME = '$partition'"
    check_contains "TABLE_SCHEMA: $schema"
    check_contains "TABLE_NAME: $table"
    check_contains "PARTITION_NAME: $partition"
    
    # Method 4: SHOW CREATE TABLE should contain partition
    run_sql "SHOW CREATE TABLE $schema.$table"
    check_contains "PARTITION \`$partition\`"
}

partition_must_not_exist() {
    run_sql "SELECT COUNT(*) FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2' AND PARTITION_NAME = '$3'"
    check_contains "COUNT(*): 0"
    run_sql "SHOW CREATE TABLE $1.$2"
    check_not_contains "PARTITION \`$3\`"
}

check_table_default_value() {
    run_sql "SELECT COLUMN_DEFAULT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2' AND COLUMN_NAME = '$3'"
    check_contains "COLUMN_DEFAULT: $4"
    run_sql "SHOW CREATE TABLE $1.$2"
    check_contains "DEFAULT '$4'"
}

check_table_charset() {
    run_sql "SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2'"
    check_contains "$3"
    run_sql "SHOW CREATE TABLE $1.$2"
    check_contains "COLLATE=$3"
}

check_schema_charset() {
    local schema_name="$1"
    local expected_collation="$2"
    run_sql "SELECT DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '$schema_name'"
    check_contains "$expected_collation"
    run_sql "SHOW CREATE DATABASE $schema_name"
    check_contains "COLLATE $expected_collation"
}

check_index_visibility() {
    local schema="$1"
    local table="$2"
    local index="$3"
    local visibility="$4"
    run_sql "SELECT IS_VISIBLE FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '$schema' AND TABLE_NAME = '$table' AND INDEX_NAME = '$index'"
    check_contains "IS_VISIBLE: $visibility"
    run_sql "SHOW INDEX FROM $schema.$table WHERE Key_name = '$index'"
    if [ "$visibility" = "YES" ]; then
        check_contains "Visible: YES"
    else
        check_contains "Visible: NO"
    fi
}

check_tiflash_replica_count() {
    run_sql "SELECT REPLICA_COUNT FROM INFORMATION_SCHEMA.TIFLASH_REPLICA WHERE TABLE_SCHEMA = '$1' AND TABLE_NAME = '$2'"
    check_contains "REPLICA_COUNT: $3"
}

row_must_exist() {
    run_sql "SELECT COUNT(*) FROM $1 WHERE $2"
    check_contains "COUNT(*): 1"
    run_sql "SELECT * FROM $1 WHERE $2 LIMIT 1"
    check_contains "1. row"
}

row_must_not_exist() {
    run_sql "select count(*) from $1 where $2"
    check_contains "count(*): 0"
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
row_must_not_exist "test_snapshot_db_create.t_to_be_truncated" "id = 1"
row_must_not_exist "test_log_db_create.t_to_be_truncated" "id = 1"

# ActionTruncateTable with indexes
table_must_exist "test_snapshot_db_create.t_truncate_with_indexes"
table_must_exist "test_log_db_create.t_truncate_with_indexes"
# verify indexes still exist after truncation
index_must_exist "test_snapshot_db_create.t_truncate_with_indexes" "idx_name"
index_must_exist "test_snapshot_db_create.t_truncate_with_indexes" "idx_id"
index_must_exist "test_snapshot_db_create.t_truncate_with_indexes" "idx_data"
index_must_exist "test_log_db_create.t_truncate_with_indexes" "idx_name"
index_must_exist "test_log_db_create.t_truncate_with_indexes" "idx_id"
index_must_exist "test_log_db_create.t_truncate_with_indexes" "idx_data"
# verify all data was removed by truncation
row_must_not_exist "test_snapshot_db_create.t_truncate_with_indexes" "id = 1"
row_must_not_exist "test_snapshot_db_create.t_truncate_with_indexes" "id = 2"
row_must_not_exist "test_snapshot_db_create.t_truncate_with_indexes" "id = 3"
row_must_not_exist "test_log_db_create.t_truncate_with_indexes" "id = 1"
row_must_not_exist "test_log_db_create.t_truncate_with_indexes" "id = 2"
row_must_not_exist "test_log_db_create.t_truncate_with_indexes" "id = 3"

# ActionModifyColumn
check_create_table_contains "test_snapshot_db_create.t_modify_column" "bigint"
check_create_table_contains "test_log_db_create.t_modify_column" "bigint"

# ActionRenameTable
table_must_exist "test_snapshot_db_create.t_rename_b"
table_must_exist "test_log_db_create.t_rename_b"
row_must_exist "test_snapshot_db_create.t_rename_b" "id = 1"
row_must_exist "test_log_db_create.t_rename_b" "id = 1"

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
row_must_exist "test_snapshot_db_create.t_renames_c" "id = 1"
row_must_exist "test_snapshot_db_create.t_renames_a" "id = 2"
row_must_exist "test_snapshot_db_create.t_renames_aaa" "id = 11"
row_must_exist "test_snapshot_db_create.t_renames_bbb" "id = 22"
row_must_exist "test_log_db_create.t_renames_c" "id = 1"
row_must_exist "test_log_db_create.t_renames_a" "id = 2"
row_must_exist "test_log_db_create.t_renames_aaa" "id = 11"
row_must_exist "test_log_db_create.t_renames_bbb" "id = 22"

# ActionRenameTables over different databases
table_must_exist "test_snapshot_db_rename_2.t_renames_aa"
table_must_exist "test_snapshot_db_rename_1.t_renames_a"
table_must_not_exist "test_snapshot_db_rename_2.t_renames_b"
table_must_not_exist "test_snapshot_db_rename_1.t_renames_c"
table_must_exist "test_snapshot_db_rename_2.t_renames_c"
table_must_exist "test_log_db_rename_2.t_renames_aa"
table_must_exist "test_log_db_rename_1.t_renames_a"
table_must_not_exist "test_log_db_rename_2.t_renames_b"
table_must_not_exist "test_log_db_rename_1.t_renames_c"
table_must_exist "test_log_db_rename_2.t_renames_c"
row_must_exist "test_snapshot_db_rename_2.t_renames_aa" "id = 1"
row_must_exist "test_snapshot_db_rename_1.t_renames_a" "id = 2"
row_must_exist "test_snapshot_db_rename_2.t_renames_c" "id = 3"
row_must_exist "test_log_db_rename_2.t_renames_aa" "id = 1"
row_must_exist "test_log_db_rename_1.t_renames_a" "id = 2"
row_must_exist "test_log_db_rename_2.t_renames_c" "id = 3"

# ActionModifyTableComment
check_create_table_contains "test_snapshot_db_create.t_modify_comment" "after modify comment"
check_create_table_contains "test_log_db_create.t_modify_comment" "after modify comment"

# ActionRenameIndex
index_must_exist "test_snapshot_db_create.t_rename_index" "i2"
index_must_not_exist "test_snapshot_db_create.t_rename_index" "i1"
index_must_exist "test_log_db_create.t_rename_index" "i2"
index_must_not_exist "test_log_db_create.t_rename_index" "i1"

# ActionAddPrimaryKey
index_must_exist "test_snapshot_db_create.t_add_primary_key" "PRIMARY"
index_must_exist "test_log_db_create.t_add_primary_key" "PRIMARY"

# ActionDropPrimaryKey
index_must_not_exist "test_snapshot_db_create.t_drop_primary_key" "PRIMARY"
index_must_not_exist "test_log_db_create.t_drop_primary_key" "PRIMARY"

# ActionAddForeignKey
# foreign_key_must_exist "test_snapshot_db_create" "t_fk_child_add" "fk_added"
# foreign_key_must_exist "test_log_db_create" "t_fk_child_add" "fk_added"

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
row_must_not_exist "test_snapshot_db_create.t_truncate_partition" "id = 150"
row_must_not_exist "test_log_db_create.t_truncate_partition" "id = 150"

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
check_tiflash_replica_count "test_snapshot_db_create" "t_set_tiflash" 1
check_tiflash_replica_count "test_log_db_create" "t_set_tiflash" 1

# ActionExchangeTablePartition
row_must_exist "test_snapshot_db_create.t_exchange_partition" "id = 115"
row_must_exist "test_snapshot_db_create.t_non_partitioned_table" "id = 105"
row_must_exist "test_log_db_create.t_exchange_partition" "id = 115"
row_must_exist "test_log_db_create.t_non_partitioned_table" "id = 105"

# ActionExchangeTablePartition over different databases
row_must_exist "test_snapshot_db_exchange_partition_1.t_exchange_partition" "id = 115"
row_must_exist "test_snapshot_db_exchange_partition_2.t_non_partitioned_table" "id = 105"
row_must_exist "test_log_db_exchange_partition_1.t_exchange_partition" "id = 115"
row_must_exist "test_log_db_exchange_partition_2.t_non_partitioned_table" "id = 105"

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

# ActionDropForeignKey comprehensive test
# Test 1: FK constraints dropped but tables preserved
table_must_exist "test_snapshot_db_create.t_fk_parent_preserve"
table_must_exist "test_snapshot_db_create.t_fk_child_preserve"
table_must_exist "test_log_db_create.t_fk_parent_preserve"
table_must_exist "test_log_db_create.t_fk_child_preserve"
# verify foreign key constraints no longer exist
foreign_key_must_not_exist "test_snapshot_db_create" "t_fk_parent_preserve" "fk_preserve_parent"
foreign_key_must_not_exist "test_snapshot_db_create" "t_fk_child_preserve" "fk_preserve_child"
foreign_key_must_not_exist "test_log_db_create" "t_fk_parent_preserve" "fk_preserve_parent"
foreign_key_must_not_exist "test_log_db_create" "t_fk_child_preserve" "fk_preserve_child"
# verify data is still present
row_must_exist "test_snapshot_db_create.t_fk_parent_preserve" "id = 1 AND name = 'parent1'"
row_must_exist "test_snapshot_db_create.t_fk_child_preserve" "id = 1 AND parent_id = 1"
row_must_exist "test_log_db_create.t_fk_parent_preserve" "id = 1 AND name = 'parent1'"
row_must_exist "test_log_db_create.t_fk_child_preserve" "id = 1 AND parent_id = 1"
# test that we can insert data without foreign key constraints
run_sql "SET GLOBAL tidb_enable_foreign_key = ON"
run_sql "INSERT INTO test_snapshot_db_create.t_fk_child_preserve (id, parent_id) VALUES (2, 999)" # Should succeed since FK constraint is gone
run_sql "INSERT INTO test_log_db_create.t_fk_child_preserve (id, parent_id) VALUES (2, 999)" # Should succeed since FK constraint is gone

# Test 2: FK constraints dropped and tables also dropped
table_must_not_exist "test_snapshot_db_create.t_fk_parent_cleanup"
table_must_not_exist "test_snapshot_db_create.t_fk_child_cleanup"
table_must_not_exist "test_log_db_create.t_fk_parent_cleanup"
table_must_not_exist "test_log_db_create.t_fk_child_cleanup"
# test that we can create new tables with the same names without foreign key conflicts
run_sql "CREATE TABLE test_snapshot_db_create.t_fk_parent_cleanup (id int primary key, name varchar(50))"
run_sql "CREATE TABLE test_snapshot_db_create.t_fk_child_cleanup (id int primary key, parent_id int, constraint fk_test foreign key (parent_id) references test_snapshot_db_create.t_fk_parent_cleanup(id))"
run_sql "DROP TABLE test_snapshot_db_create.t_fk_child_cleanup"
run_sql "DROP TABLE test_snapshot_db_create.t_fk_parent_cleanup"
run_sql "SET GLOBAL tidb_enable_foreign_key = OFF"

# ActionAddVectorIndex
check_create_table_contains "test_snapshot_db_create.t_add_vector_index" "VEC_COSINE_DISTANCE"
check_create_table_contains "test_snapshot_db_create.t_add_vector_index" "VEC_COSINE_DISTANCE"
index_must_exist "test_snapshot_db_create.t_add_vector_index" "idx"
index_must_exist "test_log_db_create.t_add_vector_index" "idx"

# ActionDropSchema + ActionDropTable
table_must_not_exist "test_snapshot_multi_drop_schema.t_to_be_dropped"
table_must_not_exist "test_snapshot_multi_drop_schema.t_not_to_be_dropped"
schema_must_not_exist "test_snapshot_multi_drop_schema"
table_must_not_exist "test_log_multi_drop_schema.t_to_be_dropped"
table_must_not_exist "test_log_multi_drop_schema.t_not_to_be_dropped"
schema_must_not_exist "test_log_multi_drop_schema"

# ActionTruncateTable + ActionDropSchema
table_must_not_exist "test_snapshot_multi_drop_schema_with_truncate_table.t_to_be_truncated"
schema_must_not_exist "test_snapshot_multi_drop_schema_with_truncate_table"
table_must_not_exist "test_log_multi_drop_schema_with_truncate_table.t_to_be_truncated"
schema_must_not_exist "test_log_multi_drop_schema_with_truncate_table"

# ActionDropSchema + ActionDropSequence
sequence_must_not_exist "test_snapshot_multi_drop_schema_with_sequence" "seq_to_be_dropped"
table_must_not_exist "test_snapshot_multi_drop_schema_with_sequence.t_not_to_be_dropped"
schema_must_not_exist "test_snapshot_multi_drop_schema_with_sequence"
sequence_must_not_exist "test_log_multi_drop_schema_with_sequence" "seq_to_be_dropped"
table_must_not_exist "test_log_multi_drop_schema_with_sequence.t_not_to_be_dropped"
schema_must_not_exist "test_log_multi_drop_schema_with_sequence"
