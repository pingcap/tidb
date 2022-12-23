create database e2e_foreign_key DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
use e2e_foreign_key;
create table parent (id int key);
create table child  (id int key, pid int, constraint fk_1 foreign key (pid) references parent(id));
insert into parent values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);
insert into child values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10);
set foreign_key_checks=0;
insert into child values (100,100);
