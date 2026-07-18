create table child (id int key, pid int, constraint fk_1 foreign key (pid) references parent(id));
