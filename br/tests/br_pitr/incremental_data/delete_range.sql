-- 1. Drop Schema
drop database db_to_be_dropped;
-- 2. Drop/Truncate Table
drop table table_to_be_dropped_or_truncated.t0_dropped;
drop table table_to_be_dropped_or_truncated.t1_dropped;
truncate table table_to_be_dropped_or_truncated.t0_truncated;
truncate table table_to_be_dropped_or_truncated.t1_truncated;
-- 3.1. Drop/Truncate Table Partition
alter table partition_to_be_dropped_or_truncated.t1_dropped drop partition p0; 
alter table partition_to_be_dropped_or_truncated.t1_truncated truncate partition p0;
alter table partition_to_be_dropped_or_truncated.t1_truncated reorganize partition p2 INTO (PARTITION p2 VALUES LESS THAN (20), PARTITION p3 VALUES LESS THAN MAXVALUE);
-- 3.2. Remove/Alter Table Partitioning
alter table partition_to_be_removed_or_altered.t_removed remove partitioning;
alter table partition_to_be_removed_or_altered.t_altered partition by range(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (100), PARTITION p2 VALUES LESS THAN MAXVALUE );
alter table partition_to_be_removed_or_altered.t_altered partition by key(id) partitions 3;
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


-- CREATE TABLE IN THE LOR RESTORE STAGE
-- 1. Drop Schema
create database db_to_be_dropped_2;
create table db_to_be_dropped_2.t0(id int primary key, c int, name char(20));
create table db_to_be_dropped_2.t1(id int primary key, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );
 
create index k1 on db_to_be_dropped_2.t0 (name);
create index k2 on db_to_be_dropped_2.t0(c);
create index k1 on db_to_be_dropped_2.t1(name);
create index k2 on db_to_be_dropped_2.t1(c);
create index k3 on db_to_be_dropped_2.t1 (id, c);
 
insert into db_to_be_dropped_2.t0 values (1, 2, "123"), (2, 3, "123");
insert into db_to_be_dropped_2.t1 values (1, 2, "123"), (2, 3, "123");
-- 2. Drop/Truncate Table
create database table_to_be_dropped_or_truncated_2;
create table table_to_be_dropped_or_truncated_2.t0_dropped(id int primary key, c int, name char(20));
create table table_to_be_dropped_or_truncated_2.t1_dropped(id int primary key, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );
create table table_to_be_dropped_or_truncated_2.t0_truncated(id int primary key, c int, name char(20));
create table table_to_be_dropped_or_truncated_2.t1_truncated(id int primary key, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );

create index k1 on table_to_be_dropped_or_truncated_2.t0_dropped (name);
create index k2 on table_to_be_dropped_or_truncated_2.t0_dropped (c);
create index k1 on table_to_be_dropped_or_truncated_2.t1_dropped (name);
create index k2 on table_to_be_dropped_or_truncated_2.t1_dropped (c);
create index k3 on table_to_be_dropped_or_truncated_2.t1_dropped (id, c);

create index k1 on table_to_be_dropped_or_truncated_2.t0_truncated (name);
create index k2 on table_to_be_dropped_or_truncated_2.t0_truncated (c);
create index k1 on table_to_be_dropped_or_truncated_2.t1_truncated (name);
create index k2 on table_to_be_dropped_or_truncated_2.t1_truncated (c);
create index k3 on table_to_be_dropped_or_truncated_2.t1_truncated (id, c);
 
insert into table_to_be_dropped_or_truncated_2.t0_dropped values (1, 2, "123"), (2, 3, "123");
insert into table_to_be_dropped_or_truncated_2.t1_dropped values (1, 2, "123"), (2, 3, "123");

insert into table_to_be_dropped_or_truncated_2.t0_truncated values (1, 2, "123"), (2, 3, "123");
insert into table_to_be_dropped_or_truncated_2.t1_truncated values (1, 2, "123"), (2, 3, "123");

-- 3.1. Drop/Truncate Table Partition
create database partition_to_be_dropped_or_truncated_2;
create table partition_to_be_dropped_or_truncated_2.t0_dropped(id int primary key, c int, name char(20));
create table partition_to_be_dropped_or_truncated_2.t1_dropped(id int primary key, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );
create table partition_to_be_dropped_or_truncated_2.t0_truncated(id int primary key, c int, name char(20));
create table partition_to_be_dropped_or_truncated_2.t1_truncated(id int primary key, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );

create index k1 on partition_to_be_dropped_or_truncated_2.t0_dropped (name);
create index k2 on partition_to_be_dropped_or_truncated_2.t0_dropped (c);
create index k1 on partition_to_be_dropped_or_truncated_2.t1_dropped (name);
create index k2 on partition_to_be_dropped_or_truncated_2.t1_dropped (c);
create index k3 on partition_to_be_dropped_or_truncated_2.t1_dropped (id, c);

create index k1 on partition_to_be_dropped_or_truncated_2.t0_truncated (name);
create index k2 on partition_to_be_dropped_or_truncated_2.t0_truncated (c);
create index k1 on partition_to_be_dropped_or_truncated_2.t1_truncated (name);
create index k2 on partition_to_be_dropped_or_truncated_2.t1_truncated (c);
create index k3 on partition_to_be_dropped_or_truncated_2.t1_truncated (id, c);
 
insert into partition_to_be_dropped_or_truncated_2.t0_dropped values (1, 2, "123"), (2, 3, "123");
insert into partition_to_be_dropped_or_truncated_2.t1_dropped values (1, 2, "123"), (2, 3, "123");

insert into partition_to_be_dropped_or_truncated_2.t0_truncated values (1, 2, "123"), (2, 3, "123");
insert into partition_to_be_dropped_or_truncated_2.t1_truncated values (1, 2, "123"), (2, 3, "123");

-- 3.2. Remove/Alter Table Partitioning
create database partition_to_be_removed_or_altered_2;
create table partition_to_be_removed_or_altered_2.t_removed(id int primary key, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );
create table partition_to_be_removed_or_altered_2.t_altered(id int primary key, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );

create index k1 on partition_to_be_removed_or_altered_2.t_removed (name);
create index k2 on partition_to_be_removed_or_altered_2.t_removed (c);
create index k3 on partition_to_be_removed_or_altered_2.t_removed (id, c);

create index k1 on partition_to_be_removed_or_altered_2.t_altered (name);
create index k2 on partition_to_be_removed_or_altered_2.t_altered (c);
create index k3 on partition_to_be_removed_or_altered_2.t_altered (id, c);

insert into partition_to_be_removed_or_altered_2.t_removed values (1, 2, "123"), (2, 3, "123");

insert into partition_to_be_removed_or_altered_2.t_altered values (1, 2, "123"), (2, 3, "123");

-- 4. Drop Table Index/PrimaryKey
create database index_or_primarykey_to_be_dropped_2;
create table index_or_primarykey_to_be_dropped_2.t0(id int primary key nonclustered, c int, name char(20));
create table index_or_primarykey_to_be_dropped_2.t1(id int primary key nonclustered, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );

create index k1 on index_or_primarykey_to_be_dropped_2.t0 (name);
create index k2 on index_or_primarykey_to_be_dropped_2.t0 (c);
create index k1 on index_or_primarykey_to_be_dropped_2.t1 (name);
create index k2 on index_or_primarykey_to_be_dropped_2.t1 (c);
create index k3 on index_or_primarykey_to_be_dropped_2.t1 (id, c);
 
insert into index_or_primarykey_to_be_dropped_2.t0 values (1, 2, "123"), (2, 3, "123");
insert into index_or_primarykey_to_be_dropped_2.t1 values (1, 2, "123"), (2, 3, "123");
-- 5. Drop Table INDEXES
create database indexes_to_be_dropped_2;
create table indexes_to_be_dropped_2.t0(id int primary key nonclustered, c int, name char(20));
create table indexes_to_be_dropped_2.t1(id int primary key nonclustered, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );

create index k1 on indexes_to_be_dropped_2.t0 (name);
create index k2 on indexes_to_be_dropped_2.t0 (c);
create index k1 on indexes_to_be_dropped_2.t1 (name);
create index k2 on indexes_to_be_dropped_2.t1 (c);
create index k3 on indexes_to_be_dropped_2.t1 (id, c);
 
insert into indexes_to_be_dropped_2.t0 values (1, 2, "123"), (2, 3, "123");
insert into indexes_to_be_dropped_2.t1 values (1, 2, "123"), (2, 3, "123");
-- 6. Drop Table Column/Columns
create database column_s_to_be_dropped_2;
create table column_s_to_be_dropped_2.t0_column(id int primary key nonclustered, c int, name char(20));
create table column_s_to_be_dropped_2.t1_column(id int primary key nonclustered, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );
create table column_s_to_be_dropped_2.t0_columns(id int primary key nonclustered, c int, name char(20));
create table column_s_to_be_dropped_2.t1_columns(id int primary key nonclustered, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );

create index k1 on column_s_to_be_dropped_2.t0_column (name);
create index k2 on column_s_to_be_dropped_2.t0_column (c);
create index k1 on column_s_to_be_dropped_2.t1_column (name);
create index k2 on column_s_to_be_dropped_2.t1_column (c);
create index k3 on column_s_to_be_dropped_2.t1_column (id, c);

create index k1 on column_s_to_be_dropped_2.t0_columns (name);
create index k2 on column_s_to_be_dropped_2.t0_columns (c);
create index k1 on column_s_to_be_dropped_2.t1_columns (name);
create index k2 on column_s_to_be_dropped_2.t1_columns (c);
-- create index k3 on column_s_to_be_dropped_2.t1_columns (id, c);
 
insert into column_s_to_be_dropped_2.t0_column values (1, 2, "123"), (2, 3, "123");
insert into column_s_to_be_dropped_2.t1_column values (1, 2, "123"), (2, 3, "123");
insert into column_s_to_be_dropped_2.t0_columns values (1, 2, "123"), (2, 3, "123");
insert into column_s_to_be_dropped_2.t1_columns values (1, 2, "123"), (2, 3, "123");
-- 7. Modify Table Column
create database column_to_be_modified_2;
create table column_to_be_modified_2.t0(id int primary key nonclustered, c int, name char(20));
create table column_to_be_modified_2.t1(id int primary key nonclustered, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );

create index k1 on column_to_be_modified_2.t0 (name);
create index k2 on column_to_be_modified_2.t0 (c);
create index k1 on column_to_be_modified_2.t1 (name);
create index k2 on column_to_be_modified_2.t1 (c);
create index k3 on column_to_be_modified_2.t1 (id, c);

insert into column_to_be_modified_2.t0 values (1, 2, "123"), (2, 3, "123");
insert into column_to_be_modified_2.t1 values (1, 2, "123"), (2, 3, "123");

-- 1. Drop Schema
drop database db_to_be_dropped_2;
-- 2. Drop/Truncate Table
drop table table_to_be_dropped_or_truncated_2.t0_dropped;
drop table table_to_be_dropped_or_truncated_2.t1_dropped;
truncate table table_to_be_dropped_or_truncated_2.t0_truncated;
truncate table table_to_be_dropped_or_truncated_2.t1_truncated;
-- 3.1. Drop/Truncate Table Partition
alter table partition_to_be_dropped_or_truncated_2.t1_dropped drop partition p0; 
alter table partition_to_be_dropped_or_truncated_2.t1_truncated truncate partition p0;
alter table partition_to_be_dropped_or_truncated_2.t1_truncated reorganize partition p2 INTO (PARTITION p2 VALUES LESS THAN (20), PARTITION p3 VALUES LESS THAN MAXVALUE);
-- 3.2. Remove/Alter Table Partitioning
alter table partition_to_be_removed_or_altered_2.t_removed remove partitioning;
alter table partition_to_be_removed_or_altered_2.t_altered partition by range(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (100), PARTITION p2 VALUES LESS THAN MAXVALUE );
alter table partition_to_be_removed_or_altered_2.t_altered partition by key(id) partitions 3;
-- 4. Drop Table Index/PrimaryKey
alter table index_or_primarykey_to_be_dropped_2.t0 drop index k1;
alter table index_or_primarykey_to_be_dropped_2.t1 drop index k1;
alter table index_or_primarykey_to_be_dropped_2.t0 drop primary key;
alter table index_or_primarykey_to_be_dropped_2.t1 drop primary key;
create index k1 on index_or_primarykey_to_be_dropped_2.t0 (name);
create index k1 on index_or_primarykey_to_be_dropped_2.t1 (name);
alter table index_or_primarykey_to_be_dropped_2.t0 add primary key (id);
alter table index_or_primarykey_to_be_dropped_2.t1 add primary key (id);
-- 5. Drop Table Indexes
alter table indexes_to_be_dropped_2.t0 drop index k1, drop index k2;
alter table indexes_to_be_dropped_2.t1 drop index k1, drop index k2;
-- 6. Drop Table Column/Columns
alter table column_s_to_be_dropped_2.t0_column drop column name;
alter table column_s_to_be_dropped_2.t1_column drop column name;
alter table column_s_to_be_dropped_2.t0_columns drop column name, drop column c;
alter table column_s_to_be_dropped_2.t1_columns drop column name, drop column c;
-- 7. Modify Table Column
alter table column_to_be_modified_2.t0 modify column name varchar(25);

