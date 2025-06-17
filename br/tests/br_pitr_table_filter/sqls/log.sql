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

-- ActionTruncateTable
truncate table test_snapshot_db_create.t_to_be_truncated;
create table test_log_db_create.t_to_be_truncated (id int);
insert into test_log_db_create.t_to_be_truncated values (1);
truncate table test_log_db_create.t_to_be_truncated;

-- ActionTruncateTable with indexes
truncate table test_snapshot_db_create.t_truncate_with_indexes;
create table test_log_db_create.t_truncate_with_indexes (id int, name varchar(50), data varchar(100), index idx_name (name), unique index idx_id (id), index idx_data (data));
insert into test_log_db_create.t_truncate_with_indexes values (1, 'test1', 'data1'), (2, 'test2', 'data2'), (3, 'test3', 'data3');
truncate table test_log_db_create.t_truncate_with_indexes;

-- ActionModifyColumn
alter table test_snapshot_db_create.t_modify_column modify id BIGINT;
create table test_log_db_create.t_modify_column (id int);
alter table test_log_db_create.t_modify_column modify id BIGINT;

-- ActionRenameTable
rename table test_snapshot_db_create.t_rename_a to test_snapshot_db_create.t_rename_b;
create table test_log_db_create.t_rename_a (id int);
insert into test_log_db_create.t_rename_a values (1);
rename table test_log_db_create.t_rename_a to test_log_db_create.t_rename_b;

-- ActionRenameTables
rename table test_snapshot_db_create.t_renames_a to test_snapshot_db_create.t_renames_c, test_snapshot_db_create.t_renames_b to test_snapshot_db_create.t_renames_a;
rename table test_snapshot_db_create.t_renames_aa to test_snapshot_db_create.t_renames_aaa, test_snapshot_db_create.t_renames_bb to test_snapshot_db_create.t_renames_bbb;
create table test_log_db_create.t_renames_a (id int);
create table test_log_db_create.t_renames_b (id int);
create table test_log_db_create.t_renames_aa (id int);
create table test_log_db_create.t_renames_bb (id int);
insert into test_log_db_create.t_renames_a values (1);
insert into test_log_db_create.t_renames_b values (2);
insert into test_log_db_create.t_renames_aa values (11);
insert into test_log_db_create.t_renames_bb values (22);
rename table test_log_db_create.t_renames_a to test_log_db_create.t_renames_c, test_log_db_create.t_renames_b to test_log_db_create.t_renames_a;
rename table test_log_db_create.t_renames_aa to test_log_db_create.t_renames_aaa, test_log_db_create.t_renames_bb to test_log_db_create.t_renames_bbb;

-- ActionRenameTables over different databases
rename table test_snapshot_db_rename_1.t_renames_a to test_snapshot_db_rename_2.t_renames_aa, test_snapshot_db_rename_2.t_renames_b to test_snapshot_db_rename_1.t_renames_a, test_snapshot_db_rename_1.t_renames_c to test_snapshot_db_rename_2.t_renames_c;
create database test_log_db_rename_1;
create database test_log_db_rename_2;
create table test_log_db_rename_1.t_renames_a (id int);
create table test_log_db_rename_2.t_renames_b (id int);
create table test_log_db_rename_1.t_renames_c (id int);
insert into test_log_db_rename_1.t_renames_a values (1);
insert into test_log_db_rename_2.t_renames_b values (2);
insert into test_log_db_rename_1.t_renames_c values (3);
rename table test_log_db_rename_1.t_renames_a to test_log_db_rename_2.t_renames_aa, test_log_db_rename_2.t_renames_b to test_log_db_rename_1.t_renames_a, test_log_db_rename_1.t_renames_c to test_log_db_rename_2.t_renames_c;

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

-- ActionAlterCheckConstraint
alter table test_snapshot_db_create.t_alter_check alter constraint t_alter_check_chk_age not enforced;
create table test_log_db_create.t_alter_check (id int, age int, constraint t_alter_check_chk_age check (age >= 0 and age <= 120));
alter table test_log_db_create.t_alter_check alter constraint t_alter_check_chk_age not enforced;

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
insert into test_log_db_create.t_truncate_partition (id, name) values (150, "150");
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

-- ActionRebaseAutoID
alter table test_snapshot_db_create.t_rebase_auto_id auto_increment = 60000;
create table test_log_db_create.t_rebase_auto_id (id int primary key auto_increment, c int);
alter table test_log_db_create.t_rebase_auto_id auto_increment = 60000;

-- ActionModifyTableAutoIDCache
alter table test_snapshot_db_create.t_auto_id_cache auto_id_cache = 60000;
create table test_log_db_create.t_auto_id_cache (id int primary key auto_increment, c int);
alter table test_log_db_create.t_auto_id_cache auto_id_cache = 60000;

-- ActionShardRowID
alter table test_snapshot_db_create.t_shard_row SHARD_ROW_ID_BITS=4;
create table test_log_db_create.t_shard_row (id int primary key nonclustered);
alter table test_log_db_create.t_shard_row SHARD_ROW_ID_BITS=4;

-- ActionRebaseAutoRandomBase
alter table test_snapshot_db_create.t_auto_random force auto_random_base=60000;
create table test_log_db_create.t_auto_random (id bigint auto_random primary key);
alter table test_log_db_create.t_auto_random force auto_random_base=60000;

-- ActionSetTiFlashReplica
alter table test_snapshot_db_create.t_set_tiflash set tiflash replica 1;
create table test_log_db_create.t_set_tiflash (id int);
alter table test_log_db_create.t_set_tiflash set tiflash replica 1;

-- ActionExchangeTablePartition
alter table test_snapshot_db_create.t_exchange_partition exchange partition p_to_be_exchanged with table test_snapshot_db_create.t_non_partitioned_table;
create table test_log_db_create.t_exchange_partition (id int) partition by range (id) (
    partition p0 values less than (100),
    partition p_to_be_exchanged values less than (200)
);
insert into test_log_db_create.t_exchange_partition (id) values (105);
create table test_log_db_create.t_non_partitioned_table (id int);
insert into test_log_db_create.t_non_partitioned_table (id) values (115);
alter table test_log_db_create.t_exchange_partition exchange partition p_to_be_exchanged with table test_log_db_create.t_non_partitioned_table;

-- ActionExchangeTablePartition over different databases
alter table test_snapshot_db_exchange_partition_1.t_exchange_partition exchange partition p_to_be_exchanged with table test_snapshot_db_exchange_partition_2.t_non_partitioned_table;
create database test_log_db_exchange_partition_1;
create database test_log_db_exchange_partition_2;
create table test_log_db_exchange_partition_1.t_exchange_partition (id int) partition by range (id) (
    partition p0 values less than (100),
    partition p_to_be_exchanged values less than (200)
);
insert into test_log_db_exchange_partition_1.t_exchange_partition (id) values (105);
create table test_log_db_exchange_partition_2.t_non_partitioned_table (id int);
insert into test_log_db_exchange_partition_2.t_non_partitioned_table (id) values (115);
alter table test_log_db_exchange_partition_1.t_exchange_partition exchange partition p_to_be_exchanged with table test_log_db_exchange_partition_2.t_non_partitioned_table;

-- ActionAlterTableAttributes
alter table test_snapshot_db_create.t_alter_table_attributes attributes "merge_option=allow";
create table test_log_db_create.t_alter_table_attributes (id int);
alter table test_log_db_create.t_alter_table_attributes attributes "merge_option=allow";

-- ActionAlterTablePartitionAttributes
alter table test_snapshot_db_create.t_alter_table_partition_attributes partition p0 attributes "merge_option=allow";
create table test_log_db_create.t_alter_table_partition_attributes (id int, name varchar(50)) partition by range (id) (partition p0 values less than (100));
alter table test_log_db_create.t_alter_table_partition_attributes partition p0 attributes "merge_option=allow";

-- ActionReorganizePartition
alter table test_snapshot_db_create.t_reorganize_partition reorganize partition p0,p1 INTO (partition pnew values less than (200));
create table test_log_db_create.t_reorganize_partition (id int) partition by range (id) (
    partition p0 values less than (100),
    partition p1 values less than (200),
    partition p_to_be_dropped values less than (300)
);
insert into test_log_db_create.t_reorganize_partition (id) values (50), (150), (250);
alter table test_log_db_create.t_reorganize_partition reorganize partition p0,p1 INTO (partition pnew values less than (200));

-- ActionAlterTablePartitioning
alter table test_snapshot_db_create.t_alter_table_partitioning partition by range columns (id) (partition p0 values less than (5), partition p1 values less than (10));
create table test_log_db_create.t_alter_table_partitioning (id int);
alter table test_log_db_create.t_alter_table_partitioning partition by range columns (id) (partition p0 values less than (5), partition p1 values less than (10));

-- ActionRemovePartitioning
alter table test_snapshot_db_create.t_remove_partitioning remove partitioning;
create table test_log_db_create.t_remove_partitioning (id int) partition by range columns (id) (partition p0 values less than (5), partition p1 values less than (10));
alter table test_log_db_create.t_remove_partitioning remove partitioning;

-- ActionAlterTTLInfo
alter table test_snapshot_db_create.t_add_ttl TTL = `created_at` + INTERVAL 30 DAY;
create table test_log_db_create.t_add_ttl (id int, created_at timestamp default current_timestamp);
alter table test_log_db_create.t_add_ttl TTL = `created_at` + INTERVAL 30 DAY;

-- ActionAlterTTLRemove
alter table test_snapshot_db_create.t_remove_ttl remove TTL;
create table test_log_db_create.t_remove_ttl (id int, created_at timestamp default current_timestamp) TTL = `created_at` + INTERVAL 30 DAY;
alter table test_log_db_create.t_remove_ttl remove TTL;

-- ActionAlterCacheTable
alter table test_snapshot_db_create.t_alter_cache cache;
create table test_log_db_create.t_alter_cache (id int, data varchar(100));
alter table test_log_db_create.t_alter_cache cache;

-- ActionAlterNoCacheTable
alter table test_snapshot_db_create.t_alter_no_cache nocache;
create table test_log_db_create.t_alter_no_cache (id int, data varchar(100));
alter table test_log_db_create.t_alter_no_cache cache;
alter table test_log_db_create.t_alter_no_cache nocache;

-- ActionDropForeignKey comprehensive test
SET GLOBAL tidb_enable_foreign_key = ON;
SET foreign_key_checks = OFF;

-- Test 1: Drop FKs but preserve tables
alter table test_snapshot_db_create.t_fk_parent_preserve drop foreign key fk_preserve_parent;
alter table test_snapshot_db_create.t_fk_child_preserve drop foreign key fk_preserve_child;

-- Test 2: Drop FKs and then drop tables 
alter table test_snapshot_db_create.t_fk_parent_cleanup drop foreign key fk_cleanup_parent;
alter table test_snapshot_db_create.t_fk_child_cleanup drop foreign key fk_cleanup_child;
drop table test_snapshot_db_create.t_fk_child_cleanup;
drop table test_snapshot_db_create.t_fk_parent_cleanup;

-- Create log database equivalent tables for both scenarios
create table test_log_db_create.t_fk_parent_preserve (id int primary key, name varchar(50));
create table test_log_db_create.t_fk_child_preserve (id int primary key, parent_id int, constraint fk_preserve_child foreign key (parent_id) references test_log_db_create.t_fk_parent_preserve(id));
alter table test_log_db_create.t_fk_parent_preserve add column child_id int;
alter table test_log_db_create.t_fk_parent_preserve add constraint fk_preserve_parent foreign key (child_id) references test_log_db_create.t_fk_child_preserve(id);
insert into test_log_db_create.t_fk_parent_preserve (id, name) values (1, 'parent1');
insert into test_log_db_create.t_fk_child_preserve (id, parent_id) values (1, 1);
-- Drop only the foreign keys but preserve tables
alter table test_log_db_create.t_fk_parent_preserve drop foreign key fk_preserve_parent;
alter table test_log_db_create.t_fk_child_preserve drop foreign key fk_preserve_child;

create table test_log_db_create.t_fk_parent_cleanup (id int primary key, name varchar(50));
create table test_log_db_create.t_fk_child_cleanup (id int primary key, parent_id int, constraint fk_cleanup_child foreign key (parent_id) references test_log_db_create.t_fk_parent_cleanup(id));
alter table test_log_db_create.t_fk_parent_cleanup add column child_id int;
alter table test_log_db_create.t_fk_parent_cleanup add constraint fk_cleanup_parent foreign key (child_id) references test_log_db_create.t_fk_child_cleanup(id);
-- Drop FKs and then drop tables
alter table test_log_db_create.t_fk_parent_cleanup drop foreign key fk_cleanup_parent;
alter table test_log_db_create.t_fk_child_cleanup drop foreign key fk_cleanup_child;
drop table test_log_db_create.t_fk_child_cleanup;
drop table test_log_db_create.t_fk_parent_cleanup;

SET foreign_key_checks = ON;
SET GLOBAL tidb_enable_foreign_key = OFF;

-- ActionAddVectorIndex
alter table test_snapshot_db_create.t_add_vector_index set tiflash replica 1;
insert into test_snapshot_db_create.t_add_vector_index values (1, '[1,2.1,3.3]');
alter table test_snapshot_db_create.t_add_vector_index add vector index idx((VEC_COSINE_DISTANCE(v))) USING HNSW;
create table test_log_db_create.t_add_vector_index (id int, v vector(3));
alter table test_log_db_create.t_add_vector_index set tiflash replica 1;
insert into test_log_db_create.t_add_vector_index values (1, '[1,2.1,3.3]');
alter table test_log_db_create.t_add_vector_index add vector index idx((VEC_COSINE_DISTANCE(v))) USING HNSW;

-- ActionDropSchema + ActionDropTable
drop table test_snapshot_multi_drop_schema.t_to_be_dropped;
drop database test_snapshot_multi_drop_schema;
create database test_log_multi_drop_schema;
create table test_log_multi_drop_schema.t_to_be_dropped (id int);
create table test_log_multi_drop_schema.t_not_to_be_dropped (id int);
drop table test_log_multi_drop_schema.t_to_be_dropped;
drop database test_log_multi_drop_schema;

-- ActionTruncateTable + ActionDropSchema
truncate table test_snapshot_multi_drop_schema_with_truncate_table.t_to_be_truncated;
drop database test_snapshot_multi_drop_schema_with_truncate_table;
create database test_log_multi_drop_schema_with_truncate_table;
create table test_log_multi_drop_schema_with_truncate_table.t_to_be_truncated (id int);
truncate table test_log_multi_drop_schema_with_truncate_table.t_to_be_truncated;
drop database test_log_multi_drop_schema_with_truncate_table;

-- ActionDropSchema + ActionDropSequence
drop sequence test_snapshot_multi_drop_schema_with_sequence.seq_to_be_dropped;
drop database test_snapshot_multi_drop_schema_with_sequence;
create database test_log_multi_drop_schema_with_sequence;
create sequence test_log_multi_drop_schema_with_sequence.seq_to_be_dropped start with 1 increment by 1;
create table test_log_multi_drop_schema_with_sequence.t_not_to_be_dropped (id int);
drop sequence test_log_multi_drop_schema_with_sequence.seq_to_be_dropped;
drop database test_log_multi_drop_schema_with_sequence;
