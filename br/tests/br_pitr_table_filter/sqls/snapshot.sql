-- ActionDropSchema
create database test_snapshot_db_to_be_deleted;
create table test_snapshot_db_to_be_deleted.t1 (id int);

-- ActionCreateTable
create database test_snapshot_db_create;

-- ActionDropTable
create table test_snapshot_db_create.t_to_be_deleted (id int);

-- ActionAddColumn
create table test_snapshot_db_create.t_add_column (id int);

-- ActionDropColumn
create table test_snapshot_db_create.t_drop_column (id int, a int);

-- ActionAddIndex
create table test_snapshot_db_create.t_add_index (id int);
create table test_snapshot_db_create.t_add_unique_key (id int);

-- ActionDropIndex
create table test_snapshot_db_create.t_drop_index (id int, key i1(id));
create table test_snapshot_db_create.t_drop_unique_key (id int, unique key i1(id));

-- ActionAddForeignKey
-- TODO: Known issue - DROP FOREIGN KEY conflicts with auto-created indexes during PITR restore
-- create table test_snapshot_db_create.t_fk_parent (id int primary key, name varchar(50));
-- create table test_snapshot_db_create.t_fk_child_add (id int primary key, parent_id int);

-- ActionDropForeignKey
-- TODO: Known issue - DROP FOREIGN KEY conflicts with auto-created indexes during PITR restore
-- create table test_snapshot_db_create.t_fk_child_drop (id int primary key, parent_id int, constraint fk_to_be_dropped foreign key (parent_id) references test_snapshot_db_create.t_fk_parent(id));

-- ActionTruncateTable
create table test_snapshot_db_create.t_to_be_truncated (id int);
insert into test_snapshot_db_create.t_to_be_truncated values (1);

-- ActionModifyColumn
create table test_snapshot_db_create.t_modify_column (id int);

-- ActionRenameTable
create table test_snapshot_db_create.t_rename_a (id int);

-- ActionRenameTables
create table test_snapshot_db_create.t_renames_a (id int);
create table test_snapshot_db_create.t_renames_b (id int);
create table test_snapshot_db_create.t_renames_aa (id int);
create table test_snapshot_db_create.t_renames_bb (id int);

-- ActionSetDefaultValue
create table test_snapshot_db_create.t_set_default (id int, status varchar(20));

-- ActionModifyTableComment
create table test_snapshot_db_create.t_modify_comment (id int) comment='before modify comment';

-- ActionRenameIndex
create table test_snapshot_db_create.t_rename_index (id int, index i1 (id));

-- ActionAddTablePartition
create table test_snapshot_db_create.t_add_partition (id int, name varchar(50)) partition by range (id) (partition p0 values less than (100));

-- ActionDropTablePartition
create table test_snapshot_db_create.t_drop_partition (id int, name varchar(50)) partition by range (id) (
    partition p0 values less than (100),
    partition p1 values less than (200),
    partition p_to_be_dropped values less than (300)
);

-- ActionCreateView
create table test_snapshot_db_create.t_view_base (id int, name varchar(50));
create view test_snapshot_db_create.v_view_to_be_dropped as select id, name from test_snapshot_db_create.t_view_base;

-- ActionDropView
-- (v_view_to_be_dropped will be dropped in log.sql)

-- ActionModifyTableCharsetAndCollate
create table test_snapshot_db_create.t_modify_charset (id int, name varchar(50)) charset=utf8mb4 collate=utf8mb4_bin;

-- ActionAddPrimaryKey
create table test_snapshot_db_create.t_add_primary_key (id int);

-- ActionDropPrimaryKey
create table test_snapshot_db_create.t_drop_primary_key (id int, primary key (id) NONCLUSTERED);

-- ActionCreateSequence
create sequence test_snapshot_db_create.seq_to_be_dropped start with 1 increment by 1;
create sequence test_snapshot_db_create.seq_to_be_altered start with 1 increment by 1;

-- ActionAlterSequence
-- (seq_to_be_altered will be altered in log.sql)

-- ActionDropSequence
-- (seq_to_be_dropped will be dropped in log.sql)

-- ActionAddCheckConstraint
create table test_snapshot_db_create.t_add_check (id int, age int);

-- ActionDropCheckConstraint
create table test_snapshot_db_create.t_drop_check (id int, age int, constraint chk_age_to_be_dropped check (age >= 0 and age <= 120));

-- ActionAlterCheckConstraint
create table test_snapshot_db_create.t_alter_check (id int, age int, constraint t_alter_check_chk_age check (age >= 0 and age <= 120));

-- ActionModifySchemaCharsetAndCollate
create database test_snapshot_db_charset default character set = utf8mb4 collate = utf8mb4_bin;

-- ActionTruncateTablePartition
create table test_snapshot_db_create.t_truncate_partition (id int, name varchar(50)) partition by range (id) (
    partition p0 values less than (100),
    partition p_to_be_truncated values less than (200)
);
insert into test_snapshot_db_create.t_truncate_partition (id, name) values (150, "150");

-- ActionLockTable
create table test_snapshot_db_create.t_to_be_locked (id int, data varchar(100));

-- ActionUnlockTable
-- (tables will be unlocked in log.sql)

-- ActionAlterIndexVisibility
create table test_snapshot_db_create.t_index_visibility (id int, name varchar(50), index idx_name (name));

-- ActionRebaseAutoID
create table test_snapshot_db_create.t_rebase_auto_id (id int primary key auto_increment, c int);

-- ActionModifyTableAutoIDCache
create table test_snapshot_db_create.t_auto_id_cache (id int primary key auto_increment, c int);

-- ActionShardRowID
create table test_snapshot_db_create.t_shard_row (id int primary key nonclustered);

-- ActionRebaseAutoRandomBase
create table test_snapshot_db_create.t_auto_random (id bigint auto_random primary key);

-- ActionSetTiFlashReplica
create table test_snapshot_db_create.t_set_tiflash (id int);

-- ActionExchangeTablePartition
create table test_snapshot_db_create.t_exchange_partition (id int) partition by range (id) (
    partition p0 values less than (100),
    partition p_to_be_exchanged values less than (200)
);
insert into test_snapshot_db_create.t_exchange_partition (id) values (105);
create table test_snapshot_db_create.t_non_partitioned_table (id int);
insert into test_snapshot_db_create.t_non_partitioned_table (id) values (115);

-- ActionAlterTableAttributes
create table test_snapshot_db_create.t_alter_table_attributes (id int);

-- ActionAlterTablePartitionAttributes
create table test_snapshot_db_create.t_alter_table_partition_attributes (id int, name varchar(50)) partition by range (id) (partition p0 values less than (100));

-- ActionReorganizePartition
create table test_snapshot_db_create.t_reorganize_partition (id int) partition by range (id) (
    partition p0 values less than (100),
    partition p1 values less than (200),
    partition p_to_be_dropped values less than (300)
);
insert into test_snapshot_db_create.t_reorganize_partition (id) values (50), (150), (250);

-- ActionAlterTablePartitioning
create table test_snapshot_db_create.t_alter_table_partitioning (id int);

-- ActionRemovePartitioning
create table test_snapshot_db_create.t_remove_partitioning (id int) partition by range columns (id) (partition p0 values less than (5), partition p1 values less than (10));

-- ActionAlterTTLInfo
create table test_snapshot_db_create.t_add_ttl (id int, created_at timestamp default current_timestamp);

-- ActionAlterTTLRemove
create table test_snapshot_db_create.t_remove_ttl (id int, created_at timestamp default current_timestamp) TTL = `created_at` + INTERVAL 30 DAY;

-- ActionAlterCacheTable
create table test_snapshot_db_create.t_alter_cache (id int, data varchar(100));

-- ActionAlterNoCacheTable
create table test_snapshot_db_create.t_alter_no_cache (id int, data varchar(100));

-- Foreign Key Cleanup Test
-- Create two tables with foreign keys to each other at snapshot time
-- These will be dropped during log backup to test foreign key cleanup
SET GLOBAL tidb_enable_foreign_key = ON;
create table test_snapshot_db_create.t_fk_parent_cleanup (id int primary key, name varchar(50));
create table test_snapshot_db_create.t_fk_child_cleanup (id int primary key, parent_id int, constraint fk_cleanup_child foreign key (parent_id) references test_snapshot_db_create.t_fk_parent_cleanup(id));
-- Add a reverse foreign key to create circular dependency
alter table test_snapshot_db_create.t_fk_parent_cleanup add column child_id int;
alter table test_snapshot_db_create.t_fk_parent_cleanup add constraint fk_cleanup_parent foreign key (child_id) references test_snapshot_db_create.t_fk_child_cleanup(id);
SET GLOBAL tidb_enable_foreign_key = OFF;

-- ActionAddVectorIndex
create table test_snapshot_db_create.t_add_vector_index (id int, v vector(3));

-- ActionDropSchema + ActionDropTable
create database test_snapshot_multi_drop_schema;
create table test_snapshot_multi_drop_schema.t_to_be_dropped (id int);
create table test_snapshot_multi_drop_schema.t_not_to_be_dropped (id int);
