/*
 1778961125641936898 is 0001100010110000001000111011011111101011000111011110000000000010
 bigger than the max increment part of sharded auto row id.
 */
CREATE TABLE nonclustered_cache1_shard_autorowid (
   id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
   v int,
   PRIMARY KEY (id) NONCLUSTERED
) AUTO_ID_CACHE=1 SHARD_ROW_ID_BITS=4;
