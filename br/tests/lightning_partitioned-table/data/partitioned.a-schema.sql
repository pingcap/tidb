create table a (a int, b varchar(16), KEY key_b (`b`)) partition by hash(a) partitions 5;
