create table a (a int, b varchar(16), c int, KEY key_b (`b`), unique index key_c(c) global) partition by hash(a) partitions 5;
