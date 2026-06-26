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


CREATE INDEX huge ON test.huge_idx(blob1, blob2);

-- test foreign key constraint 1
ALTER TABLE test.pairs12_parent ADD INDEX i1 (id);
ALTER TABLE test.pairs12_child ADD INDEX i1 (pid);
ALTER TABLE test.pairs12_child ADD CONSTRAINT fk_0 FOREIGN KEY (pid) REFERENCES test.pairs12_parent (id) ON DELETE CASCADE ON UPDATE CASCADE;

-- test foreign key constraint 2.1
ALTER TABLE test.pairs13_child ADD INDEX i1 (pid);
ALTER TABLE test.pairs13_child ADD CONSTRAINT fk_0 FOREIGN KEY (pid) REFERENCES test.pairs13_parent (id) ON DELETE CASCADE ON UPDATE CASCADE;

-- test foreign key constraint 2.2
ALTER TABLE test.pairs14_child ADD CONSTRAINT fk_0 FOREIGN KEY (pid) REFERENCES test.pairs14_parent (id) ON DELETE CASCADE ON UPDATE CASCADE;

-- test foreign key constraint 3
ALTER TABLE test.pairs15_parent ADD INDEX i1 (id);
ALTER TABLE test.pairs15_child ADD CONSTRAINT fk_0 FOREIGN KEY (pid) REFERENCES test.pairs15_parent (id) ON DELETE CASCADE ON UPDATE CASCADE;

-- test foreign key constraint 4
ALTER TABLE test.pairs16_parent ADD INDEX i2 (id);
ALTER TABLE test.pairs16_child ADD INDEX i2 (pid);
ALTER TABLE test.pairs16_child ADD CONSTRAINT fk_0 FOREIGN KEY (pid) REFERENCES test.pairs16_parent (id) ON DELETE CASCADE ON UPDATE CASCADE;

-- test foreign key constraint 5
ALTER TABLE test.pairs17_parent ADD INDEX i2(id, pid);

-- test foreign key constraint 6
ALTER TABLE test.pairs18_parent ADD INDEX i1 (id);
ALTER TABLE test2.pairs18_child ADD INDEX i1 (pid);
ALTER TABLE test2.pairs18_child ADD CONSTRAINT fk_0 FOREIGN KEY (pid) REFERENCES test.pairs18_parent (id) ON DELETE SET NULL ON UPDATE CASCADE;

-- test global index
alter table test.pairs19 add unique index i1(pid) global;
