table_must_exist() {
    run_sql "select count(*) from $1"
    check_contains "count"
}

table_must_not_exist() {
    run_sql "select count(*) from $1"
    check_not_contains "count"
}

column_must_exist() {
    run_sql "select count($2) from $1"
    check_contains "count"
}

column_must_not_exist() {
    run_sql "select count($2) from $1"
    check_not_contains "count"
}

index_must_exist() {
    run_sql "select count(*) from $1 use index ($2)"
    check_contains "count"
}

index_must_not_exist() {
    run_sql "select count(*) from $1 use index ($2)"
    check_not_contains "count"
}

table_not_duplicate() {
    run_sql "select count(*) from information_schema.tables where table_schema = $1 and table_name = $2;"
    check_contains "count(*): 1"
}

check_create_table_contains() {
    run_sql "show create table $1"
    check_contains "$2"
}

# ActionCreateSchema
table_must_exist "test_log_db_create"

# ActionDropSchema
table_must_not_exist "test_log_to_be_deleted_1"
table_must_not_exist "test_log_to_be_deleted_1.t1";
table_must_not_exist "test_snapshot_db_to_be_deleted_1";
table_must_not_exist "test_snapshot_db_to_be_deleted_1.t1";

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

# ActionAddForeignKey

# ActionDropForeignKey

# ActionTruncateTable
table_must_not_exist "test_snapshot_db_create.t_to_be_truncated"
table_must_not_exist "test_log_db_create.t_to_be_truncated"

# ActionModifyColumn
check_create_table_contains "test_snapshot_db_create.t_modify_column" "bigint"
check_create_table_contains "test_log_db_create.t_modify_column" "bigint"


-- ActionRebaseAutoID

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



# ActionSetDefaultValue
# ActionShardRowID
# ActionModifyTableComment
check_create_table_contains "test_snapshot_db_create.t_modify_comment" "after modify column"
check_create_table_contains "test_log_db_create.t_modify_comment" "after modify column"

# ActionRenameIndex
index_must_exist "test_snapshot_db_create.t_rename_index" "i2"
index_must_not_exist "test_snapshot_db_create.t_rename_index" "i1"
index_must_exist "test_log_db_create.t_rename_index" "i2"
index_must_not_exist "test_log_db_create.t_rename_index" "i1"


-- ActionAddTablePartition
-- ActionDropTablePartition
-- ActionCreateView
-- ActionModifyTableCharsetAndCollate
-- ActionTruncateTablePartition
-- ActionDropView
-- ActionRecoverTable
-- ActionModifySchemaCharsetAndCollate
-- ActionLockTable
-- ActionUnlockTable
-- ActionRepairTable
-- ActionSetTiFlashReplica
-- ActionUpdateTiFlashReplicaStatus
# ActionAddPrimaryKey
index_must_exist "test_snapshot_db_create.t_add_primary_key" "primary"
index_must_exist "test_log_db_create.t_add_primary_key" "primary"

# ActionDropPrimaryKey
index_must_not_exist "test_snapshot_db_create.t_drop_primary_key" "primary"
index_must_not_exist "test_log_db_create.t_drop_primary_key" "primary"
