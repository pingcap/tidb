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

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit"
)

func TestDuplicateForeignKey(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	// Foreign table.
	tk.MustExec("create table t(id int key)")
	// Create target table with duplicate fk.
	tk.MustGetErrCode("create table t1(id int, id_fk int, CONSTRAINT `fk_aaa` FOREIGN KEY (`id_fk`) REFERENCES `t` (`id`), CONSTRAINT `fk_aaa` FOREIGN KEY (`id_fk`) REFERENCES `t` (`id`))", mysql.ErrFkDupName)
	tk.MustGetErrCode("create table t1(id int, id_fk int, CONSTRAINT `fk_aaa` FOREIGN KEY (`id_fk`) REFERENCES `t` (`id`), CONSTRAINT `fk_aaA` FOREIGN KEY (`id_fk`) REFERENCES `t` (`id`))", mysql.ErrFkDupName)

	tk.MustExec("create table t1(id int, id_fk int, CONSTRAINT `fk_aaa` FOREIGN KEY (`id_fk`) REFERENCES `t` (`id`))")
	// Alter target table with duplicate fk.
	tk.MustGetErrCode("alter table t1 add CONSTRAINT `fk_aaa` FOREIGN KEY (`id_fk`) REFERENCES `t` (`id`)", mysql.ErrFkDupName)
	tk.MustGetErrCode("alter table t1 add CONSTRAINT `fk_aAa` FOREIGN KEY (`id_fk`) REFERENCES `t` (`id`)", mysql.ErrFkDupName)
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
}

func TestTemporaryTableForeignKey(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b int);")
	tk.MustExec("drop table if exists t1_tmp;")
	tk.MustExec("create global temporary table t1_tmp (a int, b int) on commit delete rows;")
	tk.MustExec("create temporary table t2_tmp (a int, b int)")
	// test add foreign key.
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (a int, b int);")
	failSQL := "alter table t1_tmp add foreign key (c) REFERENCES t2(a);"
	tk.MustGetErrCode(failSQL, mysql.ErrCannotAddForeign)
	failSQL = "alter table t2_tmp add foreign key (c) REFERENCES t2(a);"
	tk.MustGetErrCode(failSQL, errno.ErrUnsupportedDDLOperation)
	// Test drop column with foreign key.
	failSQL = "create global temporary table t3 (c int,d int,foreign key (d) references t1 (b)) on commit delete rows;"
	tk.MustGetErrCode(failSQL, mysql.ErrCannotAddForeign)
	failSQL = "create temporary table t4(c int,d int,foreign key (d) references t1 (b));"
	tk.MustGetErrCode(failSQL, mysql.ErrCannotAddForeign)
	tk.MustExec("drop table if exists t1,t2,t3, t4,t1_tmp,t2_tmp;")
}
