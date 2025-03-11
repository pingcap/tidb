create table `pt_case_1` (a int, b int, unique index idx(a) global, index idx1(a) global) partition by list(b)
(partition p0 values in (0, 1, 2, 3),
 partition p1 values in (4, 5, 6),
 partition p2 values in (7, 8, 9, 10));
insert into `pt_case_1` values
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
