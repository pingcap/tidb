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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core_test

import (
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/stretchr/testify/require"
)

func runSQL(t *testing.T, ctx sessionctx.Context, is infoschema.InfoSchema, sql string, inPrepare bool, terr error) {
	stmts, err := session.Parse(ctx, sql)
	require.NoErrorf(t, err, "sql: %s", sql)
	require.Len(t, stmts, 1)
	stmt := stmts[0]
	var opts []core.PreprocessOpt
	if inPrepare {
		opts = append(opts, core.InPrepare)
	}
	err = core.Preprocess(ctx, stmt, append(opts, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: is}))...)
	require.Truef(t, terror.ErrorEqual(err, terr), "sql: %s, err:%v", sql, err)
}

func TestValidator(t *testing.T) {
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
		{"create table t", false, dbterror.ErrTableMustHaveColumns},
		{"create table t (unique(c))", false, dbterror.ErrTableMustHaveColumns},

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
		{"CREATE TABLE t (a float(256, 30))", false, types.ErrTooBigDisplayWidth},
		{"CREATE TABLE t (a float(255, 31))", false, types.ErrTooBigScale},
		{"CREATE TABLE t (a double(256, 30))", false, types.ErrTooBigDisplayWidth},
		{"CREATE TABLE t (a double(255, 31))", false, types.ErrTooBigScale},

		// issue 20447
		{"CREATE TABLE t (a float(53))", true, nil},
		{"CREATE TABLE t (a float(54))", false, types.ErrWrongFieldSpec},
		{"CREATE TABLE t (a double)", true, nil},

		// FIXME: temporary 'not implemented yet' test for 'CREATE TABLE ... SELECT' (issue 4754)
		{"CREATE TABLE t SELECT * FROM u", false, errors.New("'CREATE TABLE ... SELECT' is not implemented yet")},
		{"CREATE TABLE t (m int) SELECT * FROM u", false, errors.New("'CREATE TABLE ... SELECT' is not implemented yet")},
		{"CREATE TABLE t IGNORE SELECT * FROM u UNION SELECT * from v", false, errors.New("'CREATE TABLE ... SELECT' is not implemented yet")},
		{"CREATE TABLE t (m int) REPLACE AS (SELECT * FROM u) UNION (SELECT * FROM v)", false, errors.New("'CREATE TABLE ... SELECT' is not implemented yet")},

		// issue 24309
		{"SELECT * FROM t INTO OUTFILE 'ttt' UNION SELECT * FROM u", false, core.ErrWrongUsage.GenWithStackByArgs("UNION", "INTO")},

		// Error caused by "Table 'test.u' doesn't exist".
		// {"(SELECT * FROM t INTO OUTFILE 'ttt') UNION SELECT * FROM u", false, core.ErrWrongUsage.GenWithStackByArgs("UNION", "INTO")},

		{"select * from ( select 1 ) a, (select 2) a;", false, core.ErrNonUniqTable},
		{"select * from ( select 1 ) a, (select 2) b, (select 3) a;", false, core.ErrNonUniqTable},
		{"select * from ( select 1 ) a, (select 2) b, (select 3) A;", false, core.ErrNonUniqTable},
		{"select * from ( select 1 ) a join (select 2) b join (select 3) a;", false, core.ErrNonUniqTable},
		{"select person.id from person inner join person on person.id = person.id;", false, core.ErrNonUniqTable},
		{"select * from ( select 1 ) a, (select 2) b;", true, nil},
		{"select * from (select * from ( select 1 ) a join (select 2) b) b join (select 3) a;", false, nil},
		{"select * from (select 1 ) a , (select 2) b, (select * from (select 3) a join (select 4) b) c;", false, nil},

		{"CREATE VIEW V (a,b,c) AS SELECT 1,1,3;", false, nil},
		{"CREATE VIEW V AS SELECT 5 INTO OUTFILE 'ttt'", true, dbterror.ErrViewSelectClause.GenWithStackByArgs("INFO")},
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

		// issue 18149
		{"CREATE TABLE t (a int, index ``(a));", true, errors.New("[ddl:1280]Incorrect index name ''")},
		{"CREATE TABLE t (a int, b int, index ``((a+1), (b+1)));", true, errors.New("[ddl:1280]Incorrect index name ''")},
		{"CREATE TABLE t (a int, key ``(a));", true, errors.New("[ddl:1280]Incorrect index name ''")},
		{"CREATE TABLE t (a int, b int, key ``((a+1), (b+1)));", true, errors.New("[ddl:1280]Incorrect index name ''")},
		{"CREATE TABLE t (a int, index(a));", false, nil},
		{"CREATE INDEX `` on t (a);", true, errors.New("[ddl:1280]Incorrect index name ''")},
		{"CREATE INDEX `` on t ((lower(a)));", true, errors.New("[ddl:1280]Incorrect index name ''")},

		// issue 21082
		{"CREATE TABLE t (a int) ENGINE=Unknown;", false, dbterror.ErrUnknownEngine},
		{"CREATE TABLE t (a int) ENGINE=InnoDB;", false, nil},
		{"CREATE TABLE t (a int);", false, nil},
		{"ALTER TABLE t ENGINE=InnoDB;", false, nil},
		{"ALTER TABLE t ENGINE=Unknown;", false, dbterror.ErrUnknownEngine},

		// issue 20295
		// issue 11193
		{"select cast(1.23 as decimal(65,65))", true, types.ErrTooBigScale.GenWithStackByArgs(65, "1.23", mysql.MaxDecimalScale)},
		{"select CONVERT( 2, DECIMAL(62,60) )", true, types.ErrTooBigScale.GenWithStackByArgs(60, "2", mysql.MaxDecimalScale)},
		{"select CONVERT( 2, DECIMAL(66,29) )", true, types.ErrTooBigPrecision.GenWithStackByArgs(66, "2", mysql.MaxDecimalWidth)},
		{"select CONVERT( 2, DECIMAL(28,29) )", true, types.ErrMBiggerThanD.GenWithStackByArgs("2")},
		{"select CONVERT( 2, DECIMAL(30,65) )", true, types.ErrMBiggerThanD.GenWithStackByArgs("2")},
		{"select CONVERT( 2, DECIMAL(66,99) )", true, types.ErrMBiggerThanD.GenWithStackByArgs("2")},

		// TABLESAMPLE
		{"select * from t tablesample bernoulli();", false, expression.ErrInvalidTableSample},
		{"select * from t tablesample bernoulli(10 rows);", false, expression.ErrInvalidTableSample},
		{"select * from t tablesample bernoulli(23 percent) repeatable (23);", false, expression.ErrInvalidTableSample},
		{"select * from t tablesample system() repeatable (10);", false, expression.ErrInvalidTableSample},
	}

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable()})
	for _, tt := range tests {
		runSQL(t, tk.Session(), is, tt.sql, tt.inPrepare, tt.err)
	}
}

func TestForeignKey(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t1(a int, b int, c int)")
	tk.MustExec("create table test.t2(d int)")
	tk.MustExec("create database test2")
	tk.MustExec("create table test2.t(e int)")

	is := dom.InfoSchema()
	runSQL(t, tk.Session(), is, "ALTER TABLE test.t1 ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES t2 (d)", false, nil)

	tk.MustExec("use test")
	runSQL(t, tk.Session(), is, "ALTER TABLE test.t1 ADD CONSTRAINT fk FOREIGN KEY (b) REFERENCES t2 (d)", false, nil)
	runSQL(t, tk.Session(), is, "ALTER TABLE test.t1 ADD CONSTRAINT fk FOREIGN KEY (c) REFERENCES test2.t (e)", false, nil)
}

func TestDropGlobalTempTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table tb(id int);")
	tk.MustExec("create global temporary table temp(id int) on commit delete rows;")
	tk.MustExec("create global temporary table temp1(id int) on commit delete rows;")
	tk.MustExec("create temporary table ltemp1(id int);")
	tk.MustExec("create database test2")
	tk.MustExec("create global temporary table test2.temp2(id int) on commit delete rows;")

	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	runSQL(t, tk.Session(), is, "drop global temporary table tb;", false, core.ErrDropTableOnTemporaryTable)
	runSQL(t, tk.Session(), is, "drop global temporary table temp", false, nil)
	runSQL(t, tk.Session(), is, "drop global temporary table test.tb;", false, core.ErrDropTableOnTemporaryTable)
	runSQL(t, tk.Session(), is, "drop global temporary table test.temp1", false, nil)
	runSQL(t, tk.Session(), is, "drop global temporary table ltemp1", false, core.ErrDropTableOnTemporaryTable)
	runSQL(t, tk.Session(), is, "drop global temporary table test.ltemp1", false, core.ErrDropTableOnTemporaryTable)
	runSQL(t, tk.Session(), is, "drop global temporary table temp, temp1", false, nil)
	runSQL(t, tk.Session(), is, "drop global temporary table temp, tb", false, core.ErrDropTableOnTemporaryTable)
	runSQL(t, tk.Session(), is, "drop global temporary table temp, ltemp1", false, core.ErrDropTableOnTemporaryTable)
	runSQL(t, tk.Session(), is, "drop global temporary table test2.temp2, temp1", false, nil)
}

func TestErrKeyPart0(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database TestErrKeyPart")
	tk.MustExec("use TestErrKeyPart")
	err := tk.ExecToErr("CREATE TABLE `tbl11`(`a` INT(11) NOT NULL, `b` INT(11), PRIMARY KEY (`a`(0))) CHARSET UTF8MB4 COLLATE UTF8MB4_BIN")
	require.EqualError(t, err, "[planner:1391]Key part 'a' length cannot be 0")
	err = tk.ExecToErr("create table t (a int, b varchar(255), key (b(0)))")
	require.EqualError(t, err, "[planner:1391]Key part 'b' length cannot be 0")
	err = tk.ExecToErr("create table t (a int, b varchar(255))")
	require.NoError(t, err)
	err = tk.ExecToErr("alter table t add index (b(0))")
	require.EqualError(t, err, "[planner:1391]Key part 'b' length cannot be 0")
}

// For issue #30328
func TestLargeVarcharAutoConv(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable()})
	runSQL(t, tk.Session(), is, "CREATE TABLE t1(a varbinary(70000), b varchar(70000000))", false,
		errors.New("[types:1074]Column length too big for column 'a' (max = 65535); use BLOB or TEXT instead"))

	tk.MustExec("SET sql_mode = 'NO_ENGINE_SUBSTITUTION'")
	runSQL(t, tk.Session(), is, "CREATE TABLE t1(a varbinary(70000), b varchar(70000000));", false, nil)
	runSQL(t, tk.Session(), is, "CREATE TABLE t1(a varbinary(70000), b varchar(70000000) charset utf8mb4);", false, nil)
	warnCnt := tk.Session().GetSessionVars().StmtCtx.WarningCount()
	// It is only 3. For the first stmt, charset of column b is not resolved, so ddl will append a warning for it
	require.Equal(t, uint16(3), warnCnt)
	warns := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
	for i := range warns {
		require.True(t, terror.ErrorEqual(warns[i].Err, dbterror.ErrAutoConvert))
	}
}

func TestPreprocessCTE(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t, t1, t2;")
	tk.MustExec("create table t  (c int);insert into t values (1), (2), (3), (4), (5);")
	tk.MustExec("create table t1 (a int);insert into t1 values (0), (1), (2), (3), (4);")
	tk.MustExec("create table t2 (b int);insert into t2 values (1), (2), (3), (4), (5);")
	tk.MustExec("create table t11111 (d int);insert into t11111 values (1), (2), (3), (4), (5);")
	tk.MustExec("drop table if exists tbl_1;\nCREATE TABLE `tbl_1` (\n  `col_2` char(65) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n  `col_3` int(11) NOT NULL\n);")
	testCases := []struct {
		before string
		after  string
	}{
		{
			"create view v1 as WITH t1 as (select a from t2 where t2.a=3 union select t2.a+1 from t1,t2 where t1.a=t2.a) select * from t1;",
			"CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `test`.`v1` AS WITH `t1` AS (SELECT `a` FROM `test`.`t2` WHERE `t2`.`a`=3 UNION SELECT `t2`.`a`+1 FROM (`test`.`t1`) JOIN `test`.`t2` WHERE `t1`.`a`=`t2`.`a`) SELECT * FROM `t1`",
		},
		{
			"WITH t1 AS ( SELECT(WITH t1 AS ( WITH qn AS ( SELECT 10 * a AS a FROM t1 ) SELECT 10 * a AS a FROM qn ) SELECT *  FROM t1  LIMIT 1  )  FROM t2  WHERE t2.b = 3 UNION SELECT t2.b + 1  FROM t1, t2 WHERE t1.a = t2.b) SELECT * FROM t1",
			"WITH `t1` AS (SELECT (WITH `t1` AS (WITH `qn` AS (SELECT 10*`a` AS `a` FROM `test`.`t1`) SELECT 10*`a` AS `a` FROM `qn`) SELECT * FROM `t1` LIMIT 1) FROM `test`.`t2` WHERE `t2`.`b`=3 UNION SELECT `t2`.`b`+1 FROM (`test`.`t1`) JOIN `test`.`t2` WHERE `t1`.`a`=`t2`.`b`) SELECT * FROM `t1`",
		},
		{
			"with recursive cte_8932 (col_34891,col_34892) AS ( with recursive cte_8932 (col_34893,col_34894,col_34895) AS ( with tbl_1 (col_34896,col_34897,col_34898,col_34899) AS ( select 1, \"2\",3,col_3 from tbl_1 ) select cte_as_8958.col_34896,cte_as_8958.col_34898,cte_as_8958.col_34899 from tbl_1 as cte_as_8958 UNION DISTINCT select col_34893 + 1,concat(col_34894, 1),col_34895 + 1 from cte_8932 where col_34893 < 5 ) select cte_as_8959.col_34893,cte_as_8959.col_34895 from cte_8932 as cte_as_8959 ) select * from cte_8932 as cte_as_8960 order by cte_as_8960.col_34891,cte_as_8960.col_34892;",
			"WITH RECURSIVE `cte_8932` (`col_34891`, `col_34892`) AS (WITH RECURSIVE `cte_8932` (`col_34893`, `col_34894`, `col_34895`) AS (WITH `tbl_1` (`col_34896`, `col_34897`, `col_34898`, `col_34899`) AS (SELECT 1,_UTF8MB4'2',3,`col_3` FROM `test`.`tbl_1`) SELECT `cte_as_8958`.`col_34896`,`cte_as_8958`.`col_34898`,`cte_as_8958`.`col_34899` FROM `tbl_1` AS `cte_as_8958` UNION SELECT `col_34893`+1,CONCAT(`col_34894`, 1),`col_34895`+1 FROM `cte_8932` WHERE `col_34893`<5) SELECT `cte_as_8959`.`col_34893`,`cte_as_8959`.`col_34895` FROM `cte_8932` AS `cte_as_8959`) SELECT * FROM `cte_8932` AS `cte_as_8960` ORDER BY `cte_as_8960`.`col_34891`,`cte_as_8960`.`col_34892`",
		},
		{
			"with t1 as (with t11 as (select * from t) select * from t1, t2) select * from t1;",
			"WITH `t1` AS (WITH `t11` AS (SELECT * FROM `test`.`t`) SELECT * FROM (`test`.`t1`) JOIN `test`.`t2`) SELECT * FROM `t1`",
		},
		{
			"with t1 as (with t1 as (select * from t) select * from t1, t2) select * from t1;",
			"WITH `t1` AS (WITH `t1` AS (SELECT * FROM `test`.`t`) SELECT * FROM (`t1`) JOIN `test`.`t2`) SELECT * FROM `t1`",
		},
		{
			"WITH t1 AS ( WITH t1 AS ( SELECT * FROM t ) SELECT ( WITH t2 AS ( SELECT * FROM t ) SELECT * FROM t limit 1 ) FROM t1, t2 ) \n\nSELECT\n* \nFROM\n\tt1;",
			"WITH `t1` AS (WITH `t1` AS (SELECT * FROM `test`.`t`) SELECT (WITH `t2` AS (SELECT * FROM `test`.`t`) SELECT * FROM `test`.`t` LIMIT 1) FROM (`t1`) JOIN `test`.`t2`) SELECT * FROM `t1`",
		},
		{
			"WITH t123 AS (WITH t11111 AS ( SELECT * FROM test.t1 ) SELECT ( WITH t2 AS ( SELECT ( WITH t23 AS ( SELECT * FROM t11111 ) SELECT * FROM t23 LIMIT 1 ) FROM t11111 ) SELECT *  FROM t2  LIMIT 1  )  FROM t11111, test.t2 ) SELECT * FROM t11111;",
			"WITH `t123` AS (WITH `t11111` AS (SELECT * FROM `test`.`t1`) SELECT (WITH `t2` AS (SELECT (WITH `t23` AS (SELECT * FROM `t11111`) SELECT * FROM `t23` LIMIT 1) FROM `t11111`) SELECT * FROM `t2` LIMIT 1) FROM (`t11111`) JOIN `test`.`t2`) SELECT * FROM `test`.`t11111`",
		},
	}
	for _, tc := range testCases {
		stmts, warnings, err := parser.New().ParseSQL(tc.before)
		require.Len(t, warnings, 0)
		require.NoError(t, err)
		require.Len(t, stmts, 1)

		err = core.Preprocess(tk.Session(), stmts[0])
		require.NoError(t, err)

		var rs strings.Builder
		err = stmts[0].Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &rs))
		require.NoError(t, err)
		require.Equal(t, tc.after, rs.String())
	}
}
