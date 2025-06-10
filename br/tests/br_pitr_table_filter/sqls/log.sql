-- ActionCreateSchema
create database test_log_db_create;

-- ActionDropSchema
drop database test_snapshot_db_to_be_deleted_1;
create database test_log_to_be_deleted_1;
create table test_log_to_be_deleted_1.t1 (id int);
drop database test_log_to_be_deleted_1;

-- ActionCreateTable
create table test_snapshot_db_create.t1 (id int);
create table test_log_db_create.t1 (id int);

-- ActionCreateTables
-- ActionDropTable
drop table test_snapshot_db_create.t_to_be_deleted;
create table test_log_db_create.t_to_be_deleted (id int);
drop table test_log_db_create.t_to_be_deleted;

-- ActionAddColumn
alter table test_snapshot_db_create.t_add_column add column a int;
create table test_log_db_create.t_add_column (id int);
alter table test_log_db_create.t_add_column add column a int;

-- ActionDropColumn
alter table test_snapshot_db_create.t_drop_column drop column a;
create table test_log_db_create.t_drop_column (id int, a int);
alter table test_log_db_create.t_drop_column drop column a;

-- ActionAddIndex
alter table test_snapshot_db_create.t_add_index add index i1 (id);
alter table test_snapshot_db_create.t_add_unique_key add unique index i1 (id);
create table test_log_db_create.t_add_index (id int);
create table test_log_db_create.t_add_unique_key (id int);
alter table test_log_db_create.t_add_index add index i1 (id);
alter table test_log_db_create.t_add_unique_key add unique index i1 (id);

-- ActionDropIndex
alter table test_snapshot_db_create.t_drop_index drop index i1;
alter table test_snapshot_db_create.t_drop_unique_key drop index i1;
create table test_log_db_create.t_drop_index (id int, key i1(id));
create table test_log_db_create.t_drop_unique_key (id int, unique key i1(id));
alter table test_log_db_create.t_drop_index drop index i1;
alter table test_log_db_create.t_drop_unique_key drop index i1;

-- ActionAddForeignKey
-- ActionDropForeignKey
-- ActionTruncateTable
truncate table test_snapshot_db_create.t_to_be_truncated;
create table test_log_db_create.t_to_be_truncated (id int);
truncate table test_log_db_create.t_to_be_truncated;

-- ActionModifyColumn
alter table test_snapshot_db_create.t_modify_column modify id BIGINT;
create table test_log_db_create.t_modify_column (id int);
alter table test_log_db_create.t_modify_column modify id BIGINT;

-- ActionRebaseAutoID
-- ActionRenameTable
rename table test_snapshot_db_create.t_rename_a to test_snapshot_db_create.t_rename_b;
create table test_log_db_create.t_rename_a (id int);
rename table test_log_db_create.t_rename_a to test_log_db_create.t_rename_b;

-- ActionRenameTables
rename table test_snapshot_db_create.t_renames_a to test_snapshot_db_create.t_renames_c, test_snapshot_db_create.t_renames_b to test_snapshot_db_create.t_renames_a;
rename table test_snapshot_db_create.t_renames_aa to test_snapshot_db_create.t_renames_aaa, test_snapshot_db_create.t_renames_bb to test_snapshot_db_create.t_renames_bbb;
create table test_log_db_create.t_renames_a (id int);
create table test_log_db_create.t_renames_b (id int);
create table test_log_db_create.t_renames_aa (id int);
create table test_log_db_create.t_renames_bb (id int);
rename table test_log_db_create.t_renames_a to test_log_db_create.t_renames_c, test_log_db_create.t_renames_b to test_log_db_create.t_renames_a;
rename table test_log_db_create.t_renames_aa to test_log_db_create.t_renames_aaa, test_log_db_create.t_renames_bb to test_log_db_create.t_renames_bbb;

-- ActionSetDefaultValue
-- ActionShardRowID
-- ActionModifyTableComment
alter table test_snapshot_db_create.t_modify_comment comment = 'after modify column';
create table test_log_db_create.t_modify_comment (id int) comment='before modify column';
alter table test_log_db_create.t_modify_comment comment = 'after modify column';

-- ActionRenameIndex
alter table test_snapshot_db_create.t_rename_index rename index i1 to i2;
create table test_log_db_create.t_rename_index (id int, index i1 (id));
alter table test_log_db_create.t_rename_index rename index i1 to i2;

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
-- ActionAddPrimaryKey
alter table test_snapshot_db_create.t_add_primary_key add primary key (id);
create table test_log_db_create.t_add_primary_key (id int);
alter table test_log_db_create.t_add_primary_key add primary key (id);

-- ActionDropPrimaryKey
alter table test_snapshot_db_create.t_drop_primary_key drop primary key;
create table test_log_db_create.t_drop_primary_key (id int, primary key (id) NONCLUSTERED);
alter table test_log_db_create.t_drop_primary_key drop primary key;
