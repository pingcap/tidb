create table `pt_case_0` (a int, b int, unique index idx(a) global, index idx1(a) global) partition by hash(b) partitions 5;
insert into `pt_case_0` values
(0, 10),
(1, 9),
(2, 8),
(3, 7),
(4, 6),
(5, 5),
(6, 4),
(7, 3),
(8, 2),
(9, 1),
(10, 0);
