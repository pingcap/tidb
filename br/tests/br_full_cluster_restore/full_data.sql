create database db1;
create table db1.t1(id int, val varchar(100));
insert into db1.t1 values(1, 'a'), (2, 'b');
create table db1.t2(id int, val varchar(100));
insert into db1.t2 values(1, 'a'), (2, 'b');
create database db2;
create table db2.t1(id int, val varchar(100));
insert into db2.t1 values(1, 'a'), (2, 'b');
create table db2.t2(id int, val varchar(100));
insert into db2.t2 values(1, 'a'), (2, 'b');

-- user1 can select on db1 and select on db2.t1 and db2.t2
create user user1 identified by '123456';
grant select on db1.* to user1; -- mysql.db
grant select on db2.t1 to user1; -- mysql.tables_priv
grant select, update(val) on db2.t2 to user1; -- mysql.tables_priv mysql.columns_priv

-- user2 default role is role1 which can do select on db1
create role role1;
grant select on db1.* to role1;
create user user2 identified by '123456';
grant role1 to user2; -- mysql.role_edges
set default role all to user2; -- mysql.default_roles
grant ROLE_ADMIN on *.* to user2; -- mysql.global_grants

-- user3 can only login with ssl and select db1.t1
create user user3 identified by '123456' require ssl;
grant select on db1.t1 to user3;

-- cloud_admin
create user cloud_admin@'127.0.0.1' identified by '000000';

create user cloud_admin identified by '123456' require ssl; -- require ssl stores in global_priv
grant role1 to cloud_admin; -- mysql.role_edges
set default role all to cloud_admin; -- mysql.default_roles
grant ROLE_ADMIN on *.* to cloud_admin; -- mysql.global_grants
grant select on db1.* to cloud_admin; -- mysql.db
grant select on db2.t1 to cloud_admin; -- mysql.tables_priv
grant select, update(val) on db2.t2 to cloud_admin; -- mysql.tables_priv mysql.columns_priv
