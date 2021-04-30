# test random order and no primary key
create table `pk_case_3` (a int, b int, g geometry);
insert into `pk_case_3` values
(6, 4, ST_GeomFromText('POINT(1 1)')),
(4, 6, ST_GeomFromText('LINESTRING(2 1, 6 6)')),
(8, 2, NULL),
(3, 7, NULL),
(1, 9, NULL),
(2, 8, NULL),
(5, 5, NULL),
(10, 0, NULL),
(0, 10, NULL),
(9, 1, NULL),
(7, 3, NULL);
