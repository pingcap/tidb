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

package plan_test

import (
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/terror"
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
			errors.New("Incorrect table definition; there can be only one auto column and it must be defined as a key")},
		{"create table t(id int not null auto_increment, c int auto_increment, key (id, c))", true,
			errors.New("Incorrect table definition; there can be only one auto column and it must be defined as a key")},
		{"create table t(id int not null auto_increment, c int, key (c, id))", true,
			errors.New("Incorrect table definition; there can be only one auto column and it must be defined as a key")},
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
		{"alter table t auto_increment=1", true, errors.New("[autoid:3]No support for setting auto_increment using alter_table")},
		{"alter table t add column c int auto_increment key, auto_increment=10", true,
			errors.New("[autoid:3]No support for setting auto_increment using alter_table")},
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
		{"create table t (c float(54))", true,
			errors.New("[types:1063]Incorrect column specifier for column 'c'")},
		{"alter table t add column c float(54)", true,
			errors.New("[types:1063]Incorrect column specifier for column 'c'")},
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

		{"create table `t ` (a int)", true, errors.New("[ddl:1103]Incorrect table name 't '")},
		{"create table `` (a int)", true, errors.New("[ddl:1103]Incorrect table name ''")},
		{"create table t (`` int)", true, errors.New("[ddl:1166]Incorrect column name ''")},
		{"create table t (`a ` int)", true, errors.New("[ddl:1166]Incorrect column name 'a '")},
		{"drop table if exists ``", true, errors.New("[ddl:1103]Incorrect table name ''")},
		{"drop table `t `", true, errors.New("[ddl:1103]Incorrect table name 't '")},
		{"create database ``", true, errors.New("[ddl:1102]Incorrect database name ''")},
		{"create database `test `", true, errors.New("[ddl:1102]Incorrect database name 'test '")},
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

		// issue 3844
		{`create table t (a set("a, b", "c, d"))`, true, errors.New("[types:1367]Illegal set 'a, b' value found during parsing")},
		{`alter table t add column a set("a, b", "c, d")`, true, errors.New("[types:1367]Illegal set 'a, b' value found during parsing")},
		// issue 3843
		{"create index `primary` on t (i)", true, errors.New("[ddl:1280]Incorrect index name 'primary'")},
		{"alter table t add index `primary` (i)", true, errors.New("[ddl:1280]Incorrect index name 'primary'")},
	}

	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	defer store.Close()
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)
	for _, tt := range tests {
		stmts, err1 := tidb.Parse(se.(context.Context), tt.sql)
		c.Assert(err1, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		err = plan.Validate(stmt, tt.inPrepare)
		c.Assert(terror.ErrorEqual(err, tt.err), IsTrue)
	}
}
