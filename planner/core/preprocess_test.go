// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core_test

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testValidatorSuite{})

type testValidatorSuite struct {
}

func (s *testValidatorSuite) TestValidator(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql       string
		inPrepare bool
		err       error
	}{
		{"select ?", false, parser.ErrSyntax},
		{"select ?", true, nil},
		{"create table t(id int not null auto_increment default 2, key (id))", true,
			errors.New("Invalid default value for 'id'")},
		{"create table t(id int not null default 2 auto_increment, key (id))", true,
			errors.New("Invalid default value for 'id'")},
		// Default value can be null when the column is primary key in MySQL 5.6.
		// But it can't be null in MySQL 5.7.
		{"create table t(id int auto_increment default null, primary key (id))", true, nil},
		{"create table t(id int default null auto_increment, primary key (id))", true, nil},
		{"create table t(id int not null auto_increment)", true,
			errors.New("[autoid:1075]Incorrect table definition; there can be only one auto column and it must be defined as a key")},
		{"create table t(id int not null auto_increment, c int auto_increment, key (id, c))", true,
			errors.New("[autoid:1075]Incorrect table definition; there can be only one auto column and it must be defined as a key")},
		{"create table t(id int not null auto_increment, c int, key (c, id))", true,
			errors.New("[autoid:1075]Incorrect table definition; there can be only one auto column and it must be defined as a key")},
		{"create table t(id decimal auto_increment, key (id))", true,
			errors.New("Incorrect column specifier for column 'id'")},
		{"create table t(id float auto_increment, key (id))", true, nil},
		{"create table t(id int auto_increment) ENGINE=MYISAM", true, nil},
		{"create table t(a int primary key, b int, c varchar(10), d char(256));", true,
			errors.New("[types:1074]Column length too big for column 'd' (max = 255); use BLOB or TEXT instead")},
		{"create index ib on t(b,a,b);", true, errors.New("[schema:1060]Duplicate column name 'b'")},
		{"alter table t add index idx(a, b, A)", true, errors.New("[schema:1060]Duplicate column name 'A'")},
		{"create table t (a int, b int, index(a, b, A))", true, errors.New("[schema:1060]Duplicate column name 'A'")},
		{"create table t (a int, b int, key(a, b, A))", true, errors.New("[schema:1060]Duplicate column name 'A'")},
		{"create table t (a int, b int, unique(a, b, A))", true, errors.New("[schema:1060]Duplicate column name 'A'")},
		{"create table t (a int, b int, unique key(a, b, A))", true, errors.New("[schema:1060]Duplicate column name 'A'")},
		{"create table t (a int, b int, unique index(a, b, A))", true, errors.New("[schema:1060]Duplicate column name 'A'")},
		{"create table t(c1 int not null primary key, c2 int not null primary key)", true,
			errors.New("[schema:1068]Multiple primary key defined")},
		{"create table t(c1 int not null primary key, c2 int not null, primary key(c1))", true,
			errors.New("[schema:1068]Multiple primary key defined")},
		{"create table t(c1 int not null, c2 int not null, primary key(c1), primary key(c2))", true,
			errors.New("[schema:1068]Multiple primary key defined")},
		{"alter table t auto_increment=1", true, nil},
		{"alter table t add column c int auto_increment key, auto_increment=10", true, nil},
		{"alter table t add column c int auto_increment key", true, nil},
		{"alter table t add column char4294967295 char(255)", true, nil},
		{"create table t (c float(53))", true, nil},
		{"alter table t add column c float(53)", true, nil},
		{"create table t (c set ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64'))", true, nil},
		{"alter table t add column c set ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64')", true, nil},
		{"create table t (c varchar(21845) CHARACTER SET utf8)", true, nil},
		{"create table t (c varchar(16383) CHARACTER SET utf8mb4)", true, nil},
		{"create table t (c varchar(65535) CHARACTER SET ascii)", true, nil},
		{"alter table t add column c varchar(21845) CHARACTER SET utf8", true, nil},
		{"alter table t add column c varchar(16383) CHARACTER SET utf8mb4", true, nil},
		{"alter table t add column c varchar(65535) CHARACTER SET ascii", true, nil},
		{"alter table t add column char4294967295 char(4294967295)", true,
			errors.New("[types:1074]Column length too big for column 'char4294967295' (max = 255); use BLOB or TEXT instead")},
		{"alter table t add column char4294967296 char(4294967296)", true,
			errors.New("[types:1439]Display width out of range for column 'char4294967296' (max = 4294967295)")},
		{"create table t (c float(4294967296))", true,
			errors.New("[types:1439]Display width out of range for column 'c' (max = 4294967295)")},
		{"alter table t add column c float(4294967296)", true,
			errors.New("[types:1439]Display width out of range for column 'c' (max = 4294967295)")},
		{"create table t (set65 set ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64','65'))", true,
			errors.New("[types:1097]Too many strings for column set65 and SET")},
		{"alter table t add column set65 set ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60','61','62','63','64','65')", true,
			errors.New("[types:1097]Too many strings for column set65 and SET")},
		{"create table t (c varchar(4294967295) CHARACTER SET utf8)", true,
			errors.New("[types:1074]Column length too big for column 'c' (max = 21845); use BLOB or TEXT instead")},
		{"create table t (c varchar(4294967295) CHARACTER SET utf8mb4)", true,
			errors.New("[types:1074]Column length too big for column 'c' (max = 16383); use BLOB or TEXT instead")},
		{"create table t (c varchar(4294967295) CHARACTER SET ascii)", true,
			errors.New("[types:1074]Column length too big for column 'c' (max = 65535); use BLOB or TEXT instead")},
		{"alter table t add column c varchar(4294967295) CHARACTER SET utf8", true,
			errors.New("[types:1074]Column length too big for column 'c' (max = 21845); use BLOB or TEXT instead")},
		{"alter table t add column c varchar(4294967295) CHARACTER SET utf8mb4;", true,
			errors.New("[types:1074]Column length too big for column 'c' (max = 16383); use BLOB or TEXT instead")},
		{"alter table t add column c varchar(4294967295) CHARACTER SET ascii", true,
			errors.New("[types:1074]Column length too big for column 'c' (max = 65535); use BLOB or TEXT instead")},
		{"create table t", false, ddl.ErrTableMustHaveColumns},
		{"create table t (unique(c))", false, ddl.ErrTableMustHaveColumns},

		{"create table `t ` (a int)", true, errors.New("[ddl:1103]Incorrect table name 't '")},
		{"create table `` (a int)", true, errors.New("[ddl:1103]Incorrect table name ''")},
		{"create table t (`` int)", true, errors.New("[ddl:1166]Incorrect column name ''")},
		{"create table t (`a ` int)", true, errors.New("[ddl:1166]Incorrect column name 'a '")},
		{"drop table if exists ``", true, errors.New("[ddl:1103]Incorrect table name ''")},
		{"drop table `t `", true, errors.New("[ddl:1103]Incorrect table name 't '")},
		{"create database ``", true, errors.New("[ddl:1102]Incorrect database name ''")},
		{"create database `test `", true, errors.New("[ddl:1102]Incorrect database name 'test '")},
		{"alter database collate = 'utf8mb4_bin'", true, nil},
		{"alter database `` collate = 'utf8mb4_bin'", true, errors.New("[ddl:1102]Incorrect database name ''")},
		{"alter database `test ` collate = 'utf8mb4_bin'", true, errors.New("[ddl:1102]Incorrect database name 'test '")},
		{"drop database ``", true, errors.New("[ddl:1102]Incorrect database name ''")},
		{"drop database `test `", true, errors.New("[ddl:1102]Incorrect database name 'test '")},
		{"alter table `t ` add column c int", true, errors.New("[ddl:1103]Incorrect table name 't '")},
		{"alter table `` add column c int", true, errors.New("[ddl:1103]Incorrect table name ''")},
		{"alter table t rename `t ` ", true, errors.New("[ddl:1103]Incorrect table name 't '")},
		{"alter table t rename `` ", true, errors.New("[ddl:1103]Incorrect table name ''")},
		{"alter table t add column `c ` int", true, errors.New("[ddl:1166]Incorrect column name 'c '")},
		{"alter table t add column `` int", true, errors.New("[ddl:1166]Incorrect column name ''")},
		{"alter table t change column a `` int", true, errors.New("[ddl:1166]Incorrect column name ''")},
		{"alter table t change column a `a ` int", true, errors.New("[ddl:1166]Incorrect column name 'a '")},
		{"create index idx on `t ` (a)", true, errors.New("[ddl:1103]Incorrect table name 't '")},
		{"create index idx on  `` (a)", true, errors.New("[ddl:1103]Incorrect table name ''")},
		{"rename table t to ``", false, errors.New("[ddl:1103]Incorrect table name ''")},
		{"rename table `` to t", false, errors.New("[ddl:1103]Incorrect table name ''")},

		// issue 3844
		{`create table t (a set("a, b", "c, d"))`, true, errors.New("[types:1367]Illegal set 'a, b' value found during parsing")},
		{`alter table t add column a set("a, b", "c, d")`, true, errors.New("[types:1367]Illegal set 'a, b' value found during parsing")},
		// issue 3843
		{"create index `primary` on t (i)", true, errors.New("[ddl:1280]Incorrect index name 'primary'")},
		{"alter table t add index `primary` (i)", true, errors.New("[ddl:1280]Incorrect index name 'primary'")},

		// issue 2273
		{"create table t(a char, b char, c char, d char, e char, f char, g char, h char ,i char, j char, k int, l char ,m char , n char, o char , p char, q char, index(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q))", true, errors.New("[schema:1070]Too many key parts specified; max 16 parts allowed")},

		// issue #4429
		{"CREATE TABLE `t` (`a` date DEFAULT now());", false, types.ErrInvalidDefault},
		{"CREATE TABLE `t` (`a` timestamp DEFAULT now());", false, nil},
		{"CREATE TABLE `t` (`a` datetime DEFAULT now());", false, nil},
		{"CREATE TABLE `t` (`a` int DEFAULT now());", false, types.ErrInvalidDefault},
		{"CREATE TABLE `t` (`a` float DEFAULT now());", false, types.ErrInvalidDefault},
		{"CREATE TABLE `t` (`a` varchar(10) DEFAULT now());", false, types.ErrInvalidDefault},
		{"CREATE TABLE `t` (`a` double DEFAULT 1.0 DEFAULT now() DEFAULT 2.0 );", false, nil},

		{`explain format = "xx" select 100;`, false, core.ErrUnknownExplainFormat.GenWithStackByArgs("xx")},

		// issue 4472
		{`select sum(distinct(if('a', (select adddate(elt(999, count(*)), interval 1 day)), .1))) as foo;`, true, nil},
		{`select sum(1 in (select count(1)))`, true, nil},

		// issue 5529
		{"CREATE TABLE `t` (`id` int(11) NOT NULL AUTO_INCREMENT, `a` decimal(100,4) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin", false, types.ErrTooBigPrecision},
		{"CREATE TABLE `t` (`id` int(11) NOT NULL AUTO_INCREMENT, `a` decimal(65,4) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin", true, nil},
		{"CREATE TABLE `t` (`id` int(11) NOT NULL AUTO_INCREMENT, `a` decimal(65,31) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin", false, types.ErrTooBigScale},
		{"CREATE TABLE `t` (`id` int(11) NOT NULL AUTO_INCREMENT, `a` decimal(66,31) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin", false, types.ErrTooBigScale},
		{"alter table t modify column a DECIMAL(66,30);", false, types.ErrTooBigPrecision},
		{"alter table t modify column a DECIMAL(65,31);", false, types.ErrTooBigScale},
		{"alter table t modify column a DECIMAL(65,30);", true, nil},

		{"CREATE TABLE t (a float(255, 30))", true, nil},
		{"CREATE TABLE t (a double(255, 30))", true, nil},
		{"CREATE TABLE t (a float(256, 30))", false, types.ErrTooBigPrecision},
		{"CREATE TABLE t (a float(255, 31))", false, types.ErrTooBigScale},
		{"CREATE TABLE t (a double(256, 30))", false, types.ErrTooBigPrecision},
		{"CREATE TABLE t (a double(255, 31))", false, types.ErrTooBigScale},

		// FIXME: temporary 'not implemented yet' test for 'CREATE TABLE ... SELECT' (issue 4754)
		{"CREATE TABLE t SELECT * FROM u", false, errors.New("'CREATE TABLE ... SELECT' is not implemented yet")},
		{"CREATE TABLE t (m int) SELECT * FROM u", false, errors.New("'CREATE TABLE ... SELECT' is not implemented yet")},
		{"CREATE TABLE t IGNORE SELECT * FROM u UNION SELECT * from v", false, errors.New("'CREATE TABLE ... SELECT' is not implemented yet")},
		{"CREATE TABLE t (m int) REPLACE AS (SELECT * FROM u) UNION (SELECT * FROM v)", false, errors.New("'CREATE TABLE ... SELECT' is not implemented yet")},

		{"select * from ( select 1 ) a, (select 2) a;", false, core.ErrNonUniqTable},
		{"select * from ( select 1 ) a, (select 2) b, (select 3) a;", false, core.ErrNonUniqTable},
		{"select * from ( select 1 ) a, (select 2) b, (select 3) A;", false, core.ErrNonUniqTable},
		{"select * from ( select 1 ) a join (select 2) b join (select 3) a;", false, core.ErrNonUniqTable},
		{"select person.id from person inner join person on person.id = person.id;", false, core.ErrNonUniqTable},
		{"select * from ( select 1 ) a, (select 2) b;", true, nil},
		{"select * from (select * from ( select 1 ) a join (select 2) b) b join (select 3) a;", false, nil},
		{"select * from (select 1 ) a , (select 2) b, (select * from (select 3) a join (select 4) b) c;", false, nil},

		{"CREATE VIEW V (a,b,c) AS SELECT 1,1,3;", false, nil},
		{"CREATE VIEW V AS SELECT 5 INTO OUTFILE 'ttt'", true, ddl.ErrViewSelectClause.GenWithStackByArgs("INFO")},
		{"CREATE VIEW V AS SELECT 5 FOR UPDATE", false, nil},
		{"CREATE VIEW V AS SELECT 5 LOCK IN SHARE MODE", false, nil},

		// issue 9464
		{"CREATE TABLE t1 (id INT NOT NULL, c1 VARCHAR(20) AS ('foo') VIRTUAL KEY NULL, PRIMARY KEY (id));", false, core.ErrUnsupportedOnGeneratedColumn},
		{"CREATE TABLE t1 (id INT NOT NULL, c1 VARCHAR(20) AS ('foo') VIRTUAL KEY NOT NULL, PRIMARY KEY (id));", false, core.ErrUnsupportedOnGeneratedColumn},
		{"create table t (a DOUBLE NULL, b_sto DOUBLE GENERATED ALWAYS AS (a + 2) STORED UNIQUE KEY NOT NULL PRIMARY KEY);", false, nil},

		// issue 13032
		{"CREATE TABLE origin (a int primary key, b varchar(10), c int auto_increment);", false, autoid.ErrWrongAutoKey},
		{"CREATE TABLE origin (a int auto_increment, b int key);", false, autoid.ErrWrongAutoKey},
		{"CREATE TABLE origin (a int auto_increment, b int unique);", false, autoid.ErrWrongAutoKey},
		{"CREATE TABLE origin (a int primary key auto_increment, b int);", false, nil},
		{"CREATE TABLE origin (a int unique auto_increment, b int);", false, nil},
		{"CREATE TABLE origin (a int key auto_increment, b int);", false, nil},
	}

	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	ctx := se.(sessionctx.Context)
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable()})
	for _, tt := range tests {
		stmts, err1 := session.Parse(ctx, tt.sql)
		c.Assert(err1, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		var opts []core.PreprocessOpt
		if tt.inPrepare {
			opts = append(opts, core.InPrepare)
		}
		err = core.Preprocess(ctx, stmt, is, opts...)
		c.Assert(terror.ErrorEqual(err, tt.err), IsTrue, Commentf("sql: %s, err:%v", tt.sql, err))
	}
}
