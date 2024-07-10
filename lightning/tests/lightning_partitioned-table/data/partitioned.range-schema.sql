create table `range` (a int, b varchar(16), KEY key_b (`b`)) partition by range(a) (partition pNeg values less than (0), partition pMax values less than (maxvalue));
