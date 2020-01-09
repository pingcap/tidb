# test random order and no primary key
create table `pk_case_3` (a int, b int);
insert into `pk_case_3` values
(6, 4),
(4, 6),
(8, 2),
(3, 7),
(1, 9),
(2, 8),
(5, 5),
(10, 0),
(0, 10),
(9, 1),
(7, 3);
