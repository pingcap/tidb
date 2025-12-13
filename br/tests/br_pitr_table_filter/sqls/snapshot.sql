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

-- ActionTruncateTable
create table test_snapshot_db_create.t_to_be_truncated (id int);
insert into test_snapshot_db_create.t_to_be_truncated values (1);

-- ActionTruncateTable with indexes
create table test_snapshot_db_create.t_truncate_with_indexes (id int, name varchar(50), data varchar(100), index idx_name (name), unique index idx_id (id), index idx_data (data));
insert into test_snapshot_db_create.t_truncate_with_indexes values (1, 'test1', 'data1'), (2, 'test2', 'data2'), (3, 'test3', 'data3');

-- ActionModifyColumn
create table test_snapshot_db_create.t_modify_column (id int);

-- ActionRenameTable
create table test_snapshot_db_create.t_rename_a (id int);
insert into test_snapshot_db_create.t_rename_a values (1);

-- ActionRenameTables
create table test_snapshot_db_create.t_renames_a (id int);
create table test_snapshot_db_create.t_renames_b (id int);
create table test_snapshot_db_create.t_renames_aa (id int);
create table test_snapshot_db_create.t_renames_bb (id int);
insert into test_snapshot_db_create.t_renames_a values (1);
insert into test_snapshot_db_create.t_renames_b values (2);
insert into test_snapshot_db_create.t_renames_aa values (11);
insert into test_snapshot_db_create.t_renames_bb values (22);

-- ActionRenameTables over different databases
create database test_snapshot_db_rename_1;
create database test_snapshot_db_rename_2;
create table test_snapshot_db_rename_1.t_renames_a (id int);
create table test_snapshot_db_rename_2.t_renames_b (id int);
create table test_snapshot_db_rename_1.t_renames_c (id int);
insert into test_snapshot_db_rename_1.t_renames_a values (1);
insert into test_snapshot_db_rename_2.t_renames_b values (2);
insert into test_snapshot_db_rename_1.t_renames_c values (3);

-- ActionRenameTable back
SET GLOBAL tidb_enable_foreign_key = ON;
create database test_snapshot_db_rename_3;
create database test_snapshot_db_rename_4;
create database filtered_out_test_snapshot_db_rename;
create table test_snapshot_db_rename_3.t_parent (id int primary key, name varchar(50));
create table test_snapshot_db_rename_3.t_child_filtered_in (id int primary key, parent_id int, constraint fk_preserve_child foreign key (parent_id) references test_snapshot_db_rename_3.t_parent(id));
create table test_snapshot_db_rename_3.t_child_filtered_out (id int primary key, parent_id int, constraint fk_preserve_child foreign key (parent_id) references test_snapshot_db_rename_3.t_parent(id));
create table test_snapshot_db_rename_3.t_parts_filtered_in (id int primary key, name_id int) partition by range (id) (partition p0 values less than (100), partition p1 values less than (1000));
create table test_snapshot_db_rename_3.t_parts_filtered_out (id int primary key, name_id int) partition by range (id) (partition p0 values less than (100), partition p1 values less than (1000));
insert into test_snapshot_db_rename_3.t_parent values (10, 'aa'), (20, 'bb');
insert into test_snapshot_db_rename_3.t_child_filtered_in values (10, 10), (20, 20);
insert into test_snapshot_db_rename_3.t_child_filtered_out values (10, 10), (20, 20);
insert into test_snapshot_db_rename_3.t_parts_filtered_in values (10, 10), (210, 210);
insert into test_snapshot_db_rename_3.t_parts_filtered_out values (10, 10), (210, 210);
SET GLOBAL tidb_enable_foreign_key = OFF;

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

-- ActionExchangeTablePartition over different databases
create database test_snapshot_db_exchange_partition_1;
create database test_snapshot_db_exchange_partition_2;
create table test_snapshot_db_exchange_partition_1.t_exchange_partition (id int) partition by range (id) (
    partition p0 values less than (100),
    partition p_to_be_exchanged values less than (200)
);
insert into test_snapshot_db_exchange_partition_1.t_exchange_partition (id) values (105);
create table test_snapshot_db_exchange_partition_2.t_non_partitioned_table (id int);
insert into test_snapshot_db_exchange_partition_2.t_non_partitioned_table (id) values (115);

-- ActionExchangeTablePartition back
create database test_snapshot_db_exchange_partition_3;
create table test_snapshot_db_exchange_partition_3.t_exchange_partition (id int) partition by range (id) (
    partition p0 values less than (100),
    partition p_to_be_exchanged values less than (200)
);
insert into test_snapshot_db_exchange_partition_3.t_exchange_partition (id) values (105);
create table test_snapshot_db_exchange_partition_3.t_non_partitioned_table (id int);
insert into test_snapshot_db_exchange_partition_3.t_non_partitioned_table (id) values (115);

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

-- ActionDropForeignKey comprehensive test
-- Create tables with circular foreign keys to test different FK cleanup scenarios
SET GLOBAL tidb_enable_foreign_key = ON;
-- Tables that will have FKs dropped but tables preserved
create table test_snapshot_db_create.t_fk_parent_preserve (id int primary key, name varchar(50));
create table test_snapshot_db_create.t_fk_child_preserve (id int primary key, parent_id int, constraint fk_preserve_child foreign key (parent_id) references test_snapshot_db_create.t_fk_parent_preserve(id));
alter table test_snapshot_db_create.t_fk_parent_preserve add column child_id int;
alter table test_snapshot_db_create.t_fk_parent_preserve add constraint fk_preserve_parent foreign key (child_id) references test_snapshot_db_create.t_fk_child_preserve(id);
insert into test_snapshot_db_create.t_fk_parent_preserve (id, name) values (1, 'parent1');
insert into test_snapshot_db_create.t_fk_child_preserve (id, parent_id) values (1, 1);

-- Tables that will have FKs dropped and then tables dropped
create table test_snapshot_db_create.t_fk_parent_cleanup (id int primary key, name varchar(50));
create table test_snapshot_db_create.t_fk_child_cleanup (id int primary key, parent_id int, constraint fk_cleanup_child foreign key (parent_id) references test_snapshot_db_create.t_fk_parent_cleanup(id));
alter table test_snapshot_db_create.t_fk_parent_cleanup add column child_id int;
alter table test_snapshot_db_create.t_fk_parent_cleanup add constraint fk_cleanup_parent foreign key (child_id) references test_snapshot_db_create.t_fk_child_cleanup(id);
SET GLOBAL tidb_enable_foreign_key = OFF;

-- ActionAddVectorIndex
create table test_snapshot_db_create.t_add_vector_index (id int, v vector(3));

-- ActionDropSchema + ActionDropTable
create database test_snapshot_multi_drop_schema;
create table test_snapshot_multi_drop_schema.t_to_be_dropped (id int);
create table test_snapshot_multi_drop_schema.t_not_to_be_dropped (id int);

-- ActionTruncateTable + ActionDropSchema
create database test_snapshot_multi_drop_schema_with_truncate_table;
create table test_snapshot_multi_drop_schema_with_truncate_table.t_to_be_truncated (id int);

-- ActionDropSchema + ActionDropSequence
create database test_snapshot_multi_drop_schema_with_sequence;
create sequence test_snapshot_multi_drop_schema_with_sequence.seq_to_be_dropped start with 1 increment by 1;
create table test_snapshot_multi_drop_schema_with_sequence.t_not_to_be_dropped (id int);
