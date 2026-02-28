create database if not exists shiro_fuzz_r1;
use shiro_fuzz_r1;
drop table if exists t0, t1, t2, t3, t4;
create table t0 (id bigint, k0 varchar(64), k1 varchar(64), k2 int, k3 int, p0 float, p1 tinyint(1), primary key (id));
create table t1 (id bigint, k0 varchar(64), d0 bigint, d1 double, primary key (id));
create table t2 (id bigint, k1 varchar(64), k0 varchar(64), d0 decimal(12,2), d1 double, primary key (id));
create table t3 (id bigint, k2 int, k0 varchar(64), d0 float, d1 date, primary key (id), key idx_id_4 (id));
create table t4 (id bigint, k3 int, k0 varchar(64), d0 date, d1 bigint, primary key (id));
