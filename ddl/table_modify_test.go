// Copyright 2022 PingCAP, Inc.
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

package ddl_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

const tableModifyLease = 600 * time.Millisecond

func TestCreateTable(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, tableModifyLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `t` (`a` double DEFAULT 1.0 DEFAULT now() DEFAULT 2.0 );")
	tk.MustExec("CREATE TABLE IF NOT EXISTS `t` (`a` double DEFAULT 1.0 DEFAULT now() DEFAULT 2.0 );")
	is := domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	cols := tbl.Cols()
	require.Len(t, cols, 1)
	col := cols[0]
	require.Equal(t, "a", col.Name.L)
	d, ok := col.DefaultValue.(string)
	require.True(t, ok)
	require.Equal(t, "2.0", d)

	tk.MustExec("drop table t")
	tk.MustGetErrCode("CREATE TABLE `t` (`a` int) DEFAULT CHARSET=abcdefg", errno.ErrUnknownCharacterSet)

	tk.MustExec("CREATE TABLE `collateTest` (`a` int, `b` varchar(10)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci")
	expects := "collateTest CREATE TABLE `collateTest` (\n  `a` int(11) DEFAULT NULL,\n  `b` varchar(10) COLLATE utf8_general_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci"
	tk.MustQuery("show create table collateTest").Check(testkit.Rows(expects))

	tk.MustGetErrCode("CREATE TABLE `collateTest2` (`a` int) CHARSET utf8 COLLATE utf8mb4_unicode_ci", errno.ErrCollationCharsetMismatch)
	tk.MustGetErrCode("CREATE TABLE `collateTest3` (`a` int) COLLATE utf8mb4_unicode_ci CHARSET utf8", errno.ErrConflictingDeclarations)

	tk.MustExec("CREATE TABLE `collateTest4` (`a` int) COLLATE utf8_uniCOde_ci")
	expects = "collateTest4 CREATE TABLE `collateTest4` (\n  `a` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"
	tk.MustQuery("show create table collateTest4").Check(testkit.Rows(expects))

	tk.MustExec("create database test2 default charset utf8 collate utf8_general_ci")
	tk.MustExec("use test2")
	tk.MustExec("create table dbCollateTest (a varchar(10))")
	expects = "dbCollateTest CREATE TABLE `dbCollateTest` (\n  `a` varchar(10) COLLATE utf8_general_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci"
	tk.MustQuery("show create table dbCollateTest").Check(testkit.Rows(expects))

	// test for enum column
	tk.MustExec("use test")
	tk.MustGetErrCode("create table t_enum (a enum('e','e'));", errno.ErrDuplicatedValueInType)

	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustGetErrCode("create table t_enum (a enum('e','E')) charset=utf8 collate=utf8_general_ci;", errno.ErrDuplicatedValueInType)
	tk.MustGetErrCode("create table t_enum (a enum('abc','Abc')) charset=utf8 collate=utf8_general_ci;", errno.ErrDuplicatedValueInType)
	tk.MustGetErrCode("create table t_enum (a enum('e','E')) charset=utf8 collate=utf8_unicode_ci;", errno.ErrDuplicatedValueInType)
	tk.MustGetErrCode("create table t_enum (a enum('ss','ß')) charset=utf8 collate=utf8_unicode_ci;", errno.ErrDuplicatedValueInType)
	// test for set column
	tk.MustGetErrCode("create table t_enum (a set('e','e'));", errno.ErrDuplicatedValueInType)
	tk.MustGetErrCode("create table t_enum (a set('e','E')) charset=utf8 collate=utf8_general_ci;", errno.ErrDuplicatedValueInType)
	tk.MustGetErrCode("create table t_enum (a set('abc','Abc')) charset=utf8 collate=utf8_general_ci;", errno.ErrDuplicatedValueInType)
	tk.MustGetErrMsg("create table t_enum (a enum('B','b')) charset=utf8 collate=utf8_general_ci;", "[types:1291]Column 'a' has duplicated value 'b' in ENUM")
	tk.MustGetErrCode("create table t_enum (a set('e','E')) charset=utf8 collate=utf8_unicode_ci;", errno.ErrDuplicatedValueInType)
	tk.MustGetErrCode("create table t_enum (a set('ss','ß')) charset=utf8 collate=utf8_unicode_ci;", errno.ErrDuplicatedValueInType)
	tk.MustGetErrMsg("create table t_enum (a enum('ss','ß')) charset=utf8 collate=utf8_unicode_ci;", "[types:1291]Column 'a' has duplicated value 'ß' in ENUM")

	// test for table option "union" not supported
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE x (a INT) ENGINE = MyISAM;")
	tk.MustExec("CREATE TABLE y (a INT) ENGINE = MyISAM;")
	tk.MustGetErrCode("CREATE TABLE z (a INT) ENGINE = MERGE UNION = (x, y);", errno.ErrTableOptionUnionUnsupported)
	tk.MustGetErrCode("ALTER TABLE x UNION = (y);", errno.ErrTableOptionUnionUnsupported)
	tk.MustExec("drop table x;")
	tk.MustExec("drop table y;")

	// test for table option "insert method" not supported
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE x (a INT) ENGINE = MyISAM;")
	tk.MustExec("CREATE TABLE y (a INT) ENGINE = MyISAM;")
	tk.MustGetErrCode("CREATE TABLE z (a INT) ENGINE = MERGE INSERT_METHOD=LAST;", errno.ErrTableOptionInsertMethodUnsupported)
	tk.MustGetErrCode("ALTER TABLE x INSERT_METHOD=LAST;", errno.ErrTableOptionInsertMethodUnsupported)
	tk.MustExec("drop table x;")
	tk.MustExec("drop table y;")
}
