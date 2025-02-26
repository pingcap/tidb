create table `pt_case_2` (a int, b int, unique index idx(a) global, index idx1(a) global) partition by range(b)
(partition p0 values less than (4),
 partition p1 values less than (7),
 partition p2 values less than (11));
insert into `pt_case_2` values
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
