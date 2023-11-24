-- 1. Drop Schema
drop database db_to_be_dropped;
-- 2. Drop/Truncate Table
drop table table_to_be_dropped_or_truncated.t0_dropped;
drop table table_to_be_dropped_or_truncated.t1_dropped;
truncate table table_to_be_dropped_or_truncated.t0_truncated;
truncate table table_to_be_dropped_or_truncated.t1_truncated;
-- 3. Drop/Truncate Table Partition
alter table partition_to_be_dropped_or_truncated.t1_dropped drop partition p0; 
alter table partition_to_be_dropped_or_truncated.t1_truncated truncate partition p0;
-- 4. Drop Table Index/PrimaryKey
alter table index_or_primarykey_to_be_dropped.t0 drop index k1;
alter table index_or_primarykey_to_be_dropped.t1 drop index k1;
alter table index_or_primarykey_to_be_dropped.t0 drop primary key;
alter table index_or_primarykey_to_be_dropped.t1 drop primary key;
create index k1 on index_or_primarykey_to_be_dropped.t0 (name);
create index k1 on index_or_primarykey_to_be_dropped.t1 (name);
alter table index_or_primarykey_to_be_dropped.t0 add primary key (id);
alter table index_or_primarykey_to_be_dropped.t1 add primary key (id);
-- 5. Drop Table Indexes
alter table indexes_to_be_dropped.t0 drop index k1, drop index k2;
alter table indexes_to_be_dropped.t1 drop index k1, drop index k2;
-- 6. Drop Table Column/Columns
alter table column_s_to_be_dropped.t0_column drop column name;
alter table column_s_to_be_dropped.t1_column drop column name;
alter table column_s_to_be_dropped.t0_columns drop column name, drop column c;
alter table column_s_to_be_dropped.t1_columns drop column name, drop column c;
-- 7. Modify Table Column
alter table column_to_be_modified.t0 modify column name varchar(25);
