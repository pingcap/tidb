-- ActionCreateSchema
create database test_log_db_create;

-- ActionDropSchema
drop database test_snapshot_db_to_be_deleted;
create database test_log_to_be_deleted;
create table test_log_to_be_deleted.t1 (id int);
drop database test_log_to_be_deleted;

-- ActionCreateTable
create table test_snapshot_db_create.t1 (id int);
create table test_log_db_create.t1 (id int);

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
-- alter table test_snapshot_db_create.t_fk_child_add add constraint fk_added foreign key (parent_id) references test_snapshot_db_create.t_fk_parent(id);
-- create table test_log_db_create.t_fk_parent (id int primary key, name varchar(50));
-- create table test_log_db_create.t_fk_child_add (id int primary key, parent_id int);
-- alter table test_log_db_create.t_fk_child_add add constraint fk_added foreign key (parent_id) references test_log_db_create.t_fk_parent(id);

-- ActionDropForeignKey
-- TODO: Known issue - DROP FOREIGN KEY conflicts with auto-created indexes during PITR restore
-- alter table test_snapshot_db_create.t_fk_child_drop drop foreign key fk_to_be_dropped;
-- create table test_log_db_create.t_fk_child_drop (id int primary key, parent_id int, constraint fk_to_be_dropped foreign key (parent_id) references test_log_db_create.t_fk_parent(id));
-- alter table test_log_db_create.t_fk_child_drop drop foreign key fk_to_be_dropped;

-- ActionTruncateTable
truncate table test_snapshot_db_create.t_to_be_truncated;
create table test_log_db_create.t_to_be_truncated (id int);
truncate table test_log_db_create.t_to_be_truncated;

-- ActionModifyColumn
alter table test_snapshot_db_create.t_modify_column modify id BIGINT;
create table test_log_db_create.t_modify_column (id int);
alter table test_log_db_create.t_modify_column modify id BIGINT;

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
alter table test_snapshot_db_create.t_set_default alter column status set default 'active';
create table test_log_db_create.t_set_default (id int, status varchar(20));
alter table test_log_db_create.t_set_default alter column status set default 'active';

-- ActionModifyTableComment
alter table test_snapshot_db_create.t_modify_comment comment = 'after modify comment';
create table test_log_db_create.t_modify_comment (id int) comment='before modify comment';
alter table test_log_db_create.t_modify_comment comment = 'after modify comment';

-- ActionRenameIndex
alter table test_snapshot_db_create.t_rename_index rename index i1 to i2;
create table test_log_db_create.t_rename_index (id int, index i1 (id));
alter table test_log_db_create.t_rename_index rename index i1 to i2;

-- ActionAddTablePartition
alter table test_snapshot_db_create.t_add_partition add partition (partition p1 values less than (200));
create table test_log_db_create.t_add_partition (id int, name varchar(50)) partition by range (id) (partition p0 values less than (100));
alter table test_log_db_create.t_add_partition add partition (partition p1 values less than (200));

-- ActionDropTablePartition
alter table test_snapshot_db_create.t_drop_partition drop partition p_to_be_dropped;
create table test_log_db_create.t_drop_partition (id int, name varchar(50)) partition by range (id) (
    partition p0 values less than (100),
    partition p1 values less than (200),
    partition p_to_be_dropped values less than (300)
);
alter table test_log_db_create.t_drop_partition drop partition p_to_be_dropped;

-- ActionCreateView
create table test_log_db_create.t_view_base (id int, name varchar(50));
create view test_log_db_create.v_view_created as select id, name from test_log_db_create.t_view_base;

-- ActionDropView
drop view test_snapshot_db_create.v_view_to_be_dropped;
create view test_log_db_create.v_view_to_be_dropped as select id, name from test_log_db_create.t_view_base;
drop view test_log_db_create.v_view_to_be_dropped;

-- ActionModifyTableCharsetAndCollate
alter table test_snapshot_db_create.t_modify_charset convert to charset utf8mb4 collate utf8mb4_unicode_ci;
create table test_log_db_create.t_modify_charset (id int, name varchar(50)) charset=utf8mb4 collate=utf8mb4_bin;
alter table test_log_db_create.t_modify_charset convert to charset utf8mb4 collate utf8mb4_unicode_ci;

-- ActionAddPrimaryKey
alter table test_snapshot_db_create.t_add_primary_key add primary key (id);
create table test_log_db_create.t_add_primary_key (id int);
alter table test_log_db_create.t_add_primary_key add primary key (id);

-- ActionDropPrimaryKey
alter table test_snapshot_db_create.t_drop_primary_key drop primary key;
create table test_log_db_create.t_drop_primary_key (id int, primary key (id) NONCLUSTERED);
alter table test_log_db_create.t_drop_primary_key drop primary key;

-- ActionCreateSequence
create sequence test_log_db_create.seq_created start with 1 increment by 1;

-- ActionAlterSequence
alter sequence test_snapshot_db_create.seq_to_be_altered restart with 100 increment by 5;
create sequence test_log_db_create.seq_to_be_altered start with 1 increment by 1;
alter sequence test_log_db_create.seq_to_be_altered restart with 100 increment by 5;

-- ActionDropSequence
drop sequence test_snapshot_db_create.seq_to_be_dropped;
create sequence test_log_db_create.seq_to_be_dropped start with 1 increment by 1;
drop sequence test_log_db_create.seq_to_be_dropped;

-- ActionAddCheckConstraint
alter table test_snapshot_db_create.t_add_check add constraint chk_age_added check (age >= 0);
create table test_log_db_create.t_add_check (id int, age int);
alter table test_log_db_create.t_add_check add constraint chk_age_added check (age >= 0);

-- ActionDropCheckConstraint
alter table test_snapshot_db_create.t_drop_check drop constraint chk_age_to_be_dropped;
create table test_log_db_create.t_drop_check (id int, age int, constraint chk_age_to_be_dropped check (age >= 0 and age <= 120));
alter table test_log_db_create.t_drop_check drop constraint chk_age_to_be_dropped;

-- ActionModifySchemaCharsetAndCollate
alter database test_snapshot_db_charset default character set = utf8mb4 collate = utf8mb4_unicode_ci;
create database test_log_db_charset default character set = utf8mb4 collate = utf8mb4_bin;
alter database test_log_db_charset default character set = utf8mb4 collate = utf8mb4_unicode_ci;

-- ActionTruncateTablePartition
alter table test_snapshot_db_create.t_truncate_partition truncate partition p_to_be_truncated;
create table test_log_db_create.t_truncate_partition (id int, name varchar(50)) partition by range (id) (
    partition p0 values less than (100),
    partition p_to_be_truncated values less than (200)
);
alter table test_log_db_create.t_truncate_partition truncate partition p_to_be_truncated;

-- ActionLockTable
lock tables test_snapshot_db_create.t_to_be_locked write;
unlock tables;
create table test_log_db_create.t_to_be_locked (id int, data varchar(100));
lock tables test_log_db_create.t_to_be_locked write;

-- ActionUnlockTable
unlock tables;

-- ActionAlterIndexVisibility
alter table test_snapshot_db_create.t_index_visibility alter index idx_name invisible;
create table test_log_db_create.t_index_visibility (id int, name varchar(50), index idx_name (name));
alter table test_log_db_create.t_index_visibility alter index idx_name invisible;

-- === UNIMPLEMENTED DDL OPERATIONS ===
-- The following DDL operations are not yet implemented in this test:

-- ActionCreateTables
-- ActionRebaseAutoID  
-- ActionShardRowID
-- ActionRecoverTable
-- ActionRepairTable
-- ActionSetTiFlashReplica
-- ActionUpdateTiFlashReplicaStatus
-- ActionModifyTableAutoIDCache
-- ActionRebaseAutoRandomBase
-- ActionExchangeTablePartition
-- ActionAlterCheckConstraint
-- ActionAlterTableAttributes
-- ActionAlterTablePartitionPlacement
-- ActionAlterTablePartitionAttributes
-- ActionCreatePlacementPolicy
-- ActionAlterPlacementPolicy
-- ActionDropPlacementPolicy
-- ActionModifySchemaDefaultPlacement
-- ActionAlterTablePlacement
-- ActionAlterCacheTable
-- ActionAlterNoCacheTable
-- ActionAlterTableStatsOptions
-- ActionMultiSchemaChange
-- ActionFlashbackCluster
-- ActionRecoverSchema
-- ActionReorganizePartition
-- ActionAlterTTLInfo
-- ActionAlterTTLRemove
-- ActionCreateResourceGroup
-- ActionAlterResourceGroup
-- ActionDropResourceGroup
-- ActionAlterTablePartitioning
-- ActionRemovePartitioning
-- ActionAddVectorIndex
-- ActionAlterTableMode
-- ActionRefreshMeta
