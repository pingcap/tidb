-- 1. Drop Schema
create database db_to_be_dropped;
create table db_to_be_dropped.t0(id int primary key, c int, name char(20));
create table db_to_be_dropped.t1(id int primary key, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );
 
create index k1 on db_to_be_dropped.t0 (name);
create index k2 on db_to_be_dropped.t0(c);
create index k1 on db_to_be_dropped.t1(name);
create index k2 on db_to_be_dropped.t1(c);
create index k3 on db_to_be_dropped.t1 (id, c);
 
insert into db_to_be_dropped.t0 values (1, 2, "123"), (2, 3, "123");
insert into db_to_be_dropped.t1 values (1, 2, "123"), (2, 3, "123");
-- 2. Drop/Truncate Table
create database table_to_be_dropped_or_truncated;
create table table_to_be_dropped_or_truncated.t0_dropped(id int primary key, c int, name char(20));
create table table_to_be_dropped_or_truncated.t1_dropped(id int primary key, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );
create table table_to_be_dropped_or_truncated.t0_truncated(id int primary key, c int, name char(20));
create table table_to_be_dropped_or_truncated.t1_truncated(id int primary key, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );

create index k1 on table_to_be_dropped_or_truncated.t0_dropped (name);
create index k2 on table_to_be_dropped_or_truncated.t0_dropped (c);
create index k1 on table_to_be_dropped_or_truncated.t1_dropped (name);
create index k2 on table_to_be_dropped_or_truncated.t1_dropped (c);
create index k3 on table_to_be_dropped_or_truncated.t1_dropped (id, c);

create index k1 on table_to_be_dropped_or_truncated.t0_truncated (name);
create index k2 on table_to_be_dropped_or_truncated.t0_truncated (c);
create index k1 on table_to_be_dropped_or_truncated.t1_truncated (name);
create index k2 on table_to_be_dropped_or_truncated.t1_truncated (c);
create index k3 on table_to_be_dropped_or_truncated.t1_truncated (id, c);
 
insert into table_to_be_dropped_or_truncated.t0_dropped values (1, 2, "123"), (2, 3, "123");
insert into table_to_be_dropped_or_truncated.t1_dropped values (1, 2, "123"), (2, 3, "123");

insert into table_to_be_dropped_or_truncated.t0_truncated values (1, 2, "123"), (2, 3, "123");
insert into table_to_be_dropped_or_truncated.t1_truncated values (1, 2, "123"), (2, 3, "123");

-- 3.1. Drop/Truncate Table Partition
create database partition_to_be_dropped_or_truncated;
create table partition_to_be_dropped_or_truncated.t0_dropped(id int primary key, c int, name char(20));
create table partition_to_be_dropped_or_truncated.t1_dropped(id int primary key, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );
create table partition_to_be_dropped_or_truncated.t0_truncated(id int primary key, c int, name char(20));
create table partition_to_be_dropped_or_truncated.t1_truncated(id int primary key, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );

create index k1 on partition_to_be_dropped_or_truncated.t0_dropped (name);
create index k2 on partition_to_be_dropped_or_truncated.t0_dropped (c);
create index k1 on partition_to_be_dropped_or_truncated.t1_dropped (name);
create index k2 on partition_to_be_dropped_or_truncated.t1_dropped (c);
create index k3 on partition_to_be_dropped_or_truncated.t1_dropped (id, c);

create index k1 on partition_to_be_dropped_or_truncated.t0_truncated (name);
create index k2 on partition_to_be_dropped_or_truncated.t0_truncated (c);
create index k1 on partition_to_be_dropped_or_truncated.t1_truncated (name);
create index k2 on partition_to_be_dropped_or_truncated.t1_truncated (c);
create index k3 on partition_to_be_dropped_or_truncated.t1_truncated (id, c);
 
insert into partition_to_be_dropped_or_truncated.t0_dropped values (1, 2, "123"), (2, 3, "123");
insert into partition_to_be_dropped_or_truncated.t1_dropped values (1, 2, "123"), (2, 3, "123");

insert into partition_to_be_dropped_or_truncated.t0_truncated values (1, 2, "123"), (2, 3, "123");
insert into partition_to_be_dropped_or_truncated.t1_truncated values (1, 2, "123"), (2, 3, "123");

-- 3.2. Remove/Alter Table Partitioning
create database partition_to_be_removed_or_altered;
create table partition_to_be_removed_or_altered.t_removed(id int primary key, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );
create table partition_to_be_removed_or_altered.t_altered(id int primary key, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );

create index k1 on partition_to_be_removed_or_altered.t_removed (name);
create index k2 on partition_to_be_removed_or_altered.t_removed (c);
create index k3 on partition_to_be_removed_or_altered.t_removed (id, c);

create index k1 on partition_to_be_removed_or_altered.t_altered (name);
create index k2 on partition_to_be_removed_or_altered.t_altered (c);
create index k3 on partition_to_be_removed_or_altered.t_altered (id, c);

insert into partition_to_be_removed_or_altered.t_removed values (1, 2, "123"), (2, 3, "123");

insert into partition_to_be_removed_or_altered.t_altered values (1, 2, "123"), (2, 3, "123");

-- 4. Drop Table Index/PrimaryKey
create database index_or_primarykey_to_be_dropped;
create table index_or_primarykey_to_be_dropped.t0(id int primary key nonclustered, c int, name char(20));
create table index_or_primarykey_to_be_dropped.t1(id int primary key nonclustered, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );

create index k1 on index_or_primarykey_to_be_dropped.t0 (name);
create index k2 on index_or_primarykey_to_be_dropped.t0 (c);
create index k1 on index_or_primarykey_to_be_dropped.t1 (name);
create index k2 on index_or_primarykey_to_be_dropped.t1 (c);
create index k3 on index_or_primarykey_to_be_dropped.t1 (id, c);
 
insert into index_or_primarykey_to_be_dropped.t0 values (1, 2, "123"), (2, 3, "123");
insert into index_or_primarykey_to_be_dropped.t1 values (1, 2, "123"), (2, 3, "123");
-- 5. Drop Table INDEXES
create database indexes_to_be_dropped;
create table indexes_to_be_dropped.t0(id int primary key nonclustered, c int, name char(20));
create table indexes_to_be_dropped.t1(id int primary key nonclustered, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );

create index k1 on indexes_to_be_dropped.t0 (name);
create index k2 on indexes_to_be_dropped.t0 (c);
create index k1 on indexes_to_be_dropped.t1 (name);
create index k2 on indexes_to_be_dropped.t1 (c);
create index k3 on indexes_to_be_dropped.t1 (id, c);
 
insert into indexes_to_be_dropped.t0 values (1, 2, "123"), (2, 3, "123");
insert into indexes_to_be_dropped.t1 values (1, 2, "123"), (2, 3, "123");
-- 6. Drop Table Column/Columns
create database column_s_to_be_dropped;
create table column_s_to_be_dropped.t0_column(id int primary key nonclustered, c int, name char(20));
create table column_s_to_be_dropped.t1_column(id int primary key nonclustered, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );
create table column_s_to_be_dropped.t0_columns(id int primary key nonclustered, c int, name char(20));
create table column_s_to_be_dropped.t1_columns(id int primary key nonclustered, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );

create index k1 on column_s_to_be_dropped.t0_column (name);
create index k2 on column_s_to_be_dropped.t0_column (c);
create index k1 on column_s_to_be_dropped.t1_column (name);
create index k2 on column_s_to_be_dropped.t1_column (c);
create index k3 on column_s_to_be_dropped.t1_column (id, c);

create index k1 on column_s_to_be_dropped.t0_columns (name);
create index k2 on column_s_to_be_dropped.t0_columns (c);
create index k1 on column_s_to_be_dropped.t1_columns (name);
create index k2 on column_s_to_be_dropped.t1_columns (c);
-- create index k3 on column_s_to_be_dropped.t1_columns (id, c);
 
insert into column_s_to_be_dropped.t0_column values (1, 2, "123"), (2, 3, "123");
insert into column_s_to_be_dropped.t1_column values (1, 2, "123"), (2, 3, "123");
insert into column_s_to_be_dropped.t0_columns values (1, 2, "123"), (2, 3, "123");
insert into column_s_to_be_dropped.t1_columns values (1, 2, "123"), (2, 3, "123");
-- 7. Modify Table Column
create database column_to_be_modified;
create table column_to_be_modified.t0(id int primary key nonclustered, c int, name char(20));
create table column_to_be_modified.t1(id int primary key nonclustered, c int, name char(20)) PARTITION BY RANGE(id) ( PARTITION p0 VALUES LESS THAN (0), PARTITION p1 VALUES LESS THAN (10), PARTITION p2 VALUES LESS THAN MAXVALUE );

create index k1 on column_to_be_modified.t0 (name);
create index k2 on column_to_be_modified.t0 (c);
create index k1 on column_to_be_modified.t1 (name);
create index k2 on column_to_be_modified.t1 (c);
create index k3 on column_to_be_modified.t1 (id, c);

insert into column_to_be_modified.t0 values (1, 2, "123"), (2, 3, "123");
insert into column_to_be_modified.t1 values (1, 2, "123"), (2, 3, "123");
