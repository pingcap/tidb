-- basic test: [INDEX/UNIQUE], [COMMENT], [INDEXTYPE], [INVISIBLE], [EXPRESSION]
ALTER TABLE test.pairs ADD INDEX i1(y, z) USING HASH COMMENT "edelw;fe?fewfe\nefwe" INVISIBLE;
ALTER TABLE test.pairs ADD UNIQUE KEY u1(x, y) USING RTREE VISIBLE;
ALTER TABLE test.pairs ADD INDEX i2(y, (z + 1)) USING BTREE COMMENT "123";
ALTER TABLE test.pairs ADD UNIQUE KEY u2(x, (y+1)) USING HASH COMMENT "243";

-- test: [COLUMN LENGTH], [EXPRESSION], [PRIMARY]
ALTER TABLE test.pairs2 ADD INDEX i1(y, z(10));
ALTER TABLE test.pairs2 ADD UNIQUE KEY u1(y, z(10), (y * 2)) USING RTREE VISIBLE;
ALTER TABLE test.pairs2 ADD PRIMARY KEY (x) USING HASH;

-- test: [MULTIVALUED]
ALTER TABLE test.pairs3 ADD INDEX zips2((CAST(custinfo->'$.zipcode' AS UNSIGNED ARRAY)));

-- test: DROP operation
ALTER TABLE test.pairs4 ADD INDEX i1(y, z) USING HASH COMMENT "edelw;fe?fewfe\nefwe" INVISIBLE;
ALTER TABLE test.pairs4 ADD UNIQUE KEY u1(x, y) USING RTREE VISIBLE;
ALTER TABLE test.pairs4 ADD INDEX i2(y, (z + 1)) USING BTREE COMMENT "123";
ALTER TABLE test.pairs4 ADD UNIQUE KEY u2(x, (y+1)) USING HASH COMMENT "243";
ALTER TABLE test.pairs4 DROP INDEX i1;
ALTER TABLE test.pairs4 DROP INDEX u1;
ALTER TABLE test.pairs4 DROP INDEX i2;
ALTER TABLE test.pairs4 DROP INDEX u2;

-- test: DROP operation
ALTER TABLE test.pairs5 ADD INDEX i1(y, z(10));
ALTER TABLE test.pairs5 ADD UNIQUE KEY u1(y, z(10), (y * 2)) USING RTREE VISIBLE;
ALTER TABLE test.pairs5 ADD PRIMARY KEY (x) USING HASH;
ALTER TABLE test.pairs5 DROP INDEX i1;
ALTER TABLE test.pairs5 DROP INDEX u1;
ALTER TABLE test.pairs5 DROP INDEX `PRIMARY`;

-- test: [strange string in EXPRESSION], [rename operation]
ALTER TABLE test.pairs6 ADD INDEX zips2((CAST(`cust``;info`->'$.zipcode' AS UNSIGNED ARRAY)));
ALTER TABLE test.pairs6 ADD INDEX i1(`nam``;e`, (`nam``;e` * 2));
RENAME TABLE test.pairs6 TO test.pairs7;
ALTER TABLE test.pairs7 RENAME INDEX i1 to i2;

-- future test: [MODIFY COLUMN operation]
ALTER TABLE test.pairs8 ADD INDEX i1(y);
ALTER TABLE test.pairs8 MODIFY y varchar(20);

-- future test: [CHANGE COLUMN operation]
ALTER TABLE test.pairs9 ADD INDEX i1(y);
ALTER TABLE test.pairs9 CHANGE y y2 varchar(20);

-- test partition
ALTER TABLE test.pairs10 ADD INDEX i1(y);
