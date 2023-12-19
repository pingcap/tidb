create table `defaultlist` (a int, b varchar(16), KEY key_b (`b`)) partition by list(a) (partition p1 values in (1,4,8),partition p2 values in (32,default), partition p3 values in (262144,65536));
