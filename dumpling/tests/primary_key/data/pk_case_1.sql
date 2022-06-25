# test reversely sorted by primary key
create table `pk_case_1` (a int primary key, b int);
insert into `pk_case_1` values
(10, 0),
(9, 1),
(8, 2),
(7, 3),
(6, 4),
(5, 5),
(4, 6),
(3, 7),
(2, 8),
(1, 9),
(0, 10);
