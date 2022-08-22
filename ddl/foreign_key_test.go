// Copyright 2016 PingCAP, Inc.
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

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestCreateTableWithForeignKeyMetaInfo(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@tidb_enable_foreign_key=1")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, a int,b int as (a) virtual);")
	tk.MustExec("create database test2")
	tk.MustExec("use test2")
	tk.MustExec("create table t2 (id int key, b int, foreign key fk_b(b) references test.t1(id) ON UPDATE RESTRICT ON DELETE CASCADE)")
	tb1Info := getTableInfo(t, dom, "test", "t1")
	tb2Info := getTableInfo(t, dom, "test2", "t2")
	require.Equal(t, 1, len(dom.InfoSchema().GetTableReferredForeignKeys("test", "t1")))
	require.Equal(t, 0, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t2")))
	require.Equal(t, 0, len(tb1Info.ForeignKeys))
	tb1ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("t2"),
		ChildFKName: model.NewCIStr("fk_b"),
	}, *tb1ReferredFKs[0])
	tb2ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test2", "t2")
	require.Equal(t, 0, len(tb2ReferredFKs))
	require.Equal(t, 1, len(tb2Info.ForeignKeys))
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_b"),
		RefSchema: model.NewCIStr("test"),
		RefTable:  model.NewCIStr("t1"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("b")},
		OnDelete:  2,
		OnUpdate:  1,
		State:     model.StatePublic,
		Version:   1,
	}, *tb2Info.ForeignKeys[0])
	// Auto create index for foreign key usage.
	require.Equal(t, 1, len(tb2Info.Indices))
	require.Equal(t, "fk_b", tb2Info.Indices[0].Name.L)
	require.Equal(t, "`test2`.`t2`, CONSTRAINT `fk_b` FOREIGN KEY (`b`) REFERENCES `test`.`t1` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT", tb2Info.ForeignKeys[0].String("test2", "t2"))

	tk.MustExec("create table t3 (id int, b int, index idx_b(b), foreign key fk_b(b) references t2(id) ON UPDATE SET NULL ON DELETE NO ACTION)")
	tb2Info = getTableInfo(t, dom, "test2", "t2")
	tb3Info := getTableInfo(t, dom, "test2", "t3")
	require.Equal(t, 1, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t2")))
	require.Equal(t, 0, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t3")))
	require.Equal(t, 1, len(tb2Info.ForeignKeys))
	tb2ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test2", "t2")
	require.Equal(t, 1, len(tb2ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("t3"),
		ChildFKName: model.NewCIStr("fk_b"),
	}, *tb2ReferredFKs[0])
	tb3ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test2", "t3")
	require.Equal(t, 0, len(tb3ReferredFKs))
	require.Equal(t, 1, len(tb3Info.ForeignKeys))
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_b"),
		RefSchema: model.NewCIStr("test2"),
		RefTable:  model.NewCIStr("t2"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("b")},
		OnDelete:  4,
		OnUpdate:  3,
		State:     model.StatePublic,
		Version:   1,
	}, *tb3Info.ForeignKeys[0])
	require.Equal(t, 1, len(tb3Info.Indices))
	require.Equal(t, "idx_b", tb3Info.Indices[0].Name.L)
	require.Equal(t, "`test2`.`t3`, CONSTRAINT `fk_b` FOREIGN KEY (`b`) REFERENCES `t2` (`id`) ON DELETE NO ACTION ON UPDATE SET NULL", tb3Info.ForeignKeys[0].String("test2", "t3"))

	tk.MustExec("create table t5 (id int key, a int, b int, foreign key (a) references t5(id));")
	tb5Info := getTableInfo(t, dom, "test2", "t5")
	require.Equal(t, 1, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t5")))
	require.Equal(t, 1, len(tb5Info.ForeignKeys))
	tb5ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test2", "t5")
	require.Equal(t, 1, len(tb5ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("t5"),
		ChildFKName: model.NewCIStr("fk_1"),
	}, *tb5ReferredFKs[0])
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_1"),
		RefSchema: model.NewCIStr("test2"),
		RefTable:  model.NewCIStr("t5"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("a")},
		State:     model.StatePublic,
		Version:   1,
	}, *tb5Info.ForeignKeys[0])
	require.Equal(t, 1, len(tb5Info.Indices))
	require.Equal(t, "fk_1", tb5Info.Indices[0].Name.L)
	require.Equal(t, 1, len(dom.InfoSchema().GetTableReferredForeignKeys("test", "t1")))
	require.Equal(t, 1, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t2")))
	require.Equal(t, 0, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t3")))
	require.Equal(t, 1, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t5")))

	tk.MustExec("set @@tidb_enable_foreign_key=0")
	tk.MustExec("drop database test2")
	require.Equal(t, 0, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t2")))
	require.Equal(t, 0, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t3")))
	require.Equal(t, 0, len(dom.InfoSchema().GetTableReferredForeignKeys("test2", "t5")))
}

func TestCreateTableWithForeignKeyMetaInfo2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@tidb_enable_foreign_key=1")
	tk.MustExec("create database test2")
	tk.MustExec("set @@foreign_key_checks=0")
	tk.MustExec("use test2")
	tk.MustExec("create table t2 (id int key, b int, foreign key fk_b(b) references test.t1(id) ON UPDATE RESTRICT ON DELETE CASCADE)")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, a int, b int as (a) virtual);")
	tb1Info := getTableInfo(t, dom, "test", "t1")
	tb2Info := getTableInfo(t, dom, "test2", "t2")
	require.Equal(t, 0, len(tb1Info.ForeignKeys))
	tb1ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("t2"),
		ChildFKName: model.NewCIStr("fk_b"),
	}, *tb1ReferredFKs[0])
	tb2ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test2", "t2")
	require.Equal(t, 0, len(tb2ReferredFKs))
	require.Equal(t, 1, len(tb2Info.ForeignKeys))
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_b"),
		RefSchema: model.NewCIStr("test"),
		RefTable:  model.NewCIStr("t1"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("b")},
		OnDelete:  2,
		OnUpdate:  1,
		State:     model.StatePublic,
		Version:   1,
	}, *tb2Info.ForeignKeys[0])
	// Auto create index for foreign key usage.
	require.Equal(t, 1, len(tb2Info.Indices))
	require.Equal(t, "fk_b", tb2Info.Indices[0].Name.L)
	require.Equal(t, "`test2`.`t2`, CONSTRAINT `fk_b` FOREIGN KEY (`b`) REFERENCES `test`.`t1` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT", tb2Info.ForeignKeys[0].String("test2", "t2"))

	tk.MustExec("create table t3 (id int key, a int, foreign key fk_a(a) references test.t1(id) ON DELETE CASCADE ON UPDATE RESTRICT, foreign key fk_a2(a) references test2.t2(id))")
	tb1Info = getTableInfo(t, dom, "test", "t1")
	tb3Info := getTableInfo(t, dom, "test", "t3")
	require.Equal(t, 0, len(tb1Info.ForeignKeys))
	tb1ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 2, len(tb1ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test"),
		ChildTable:  model.NewCIStr("t3"),
		ChildFKName: model.NewCIStr("fk_a"),
	}, *tb1ReferredFKs[0])
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("t2"),
		ChildFKName: model.NewCIStr("fk_b"),
	}, *tb1ReferredFKs[1])
	tb3ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t3")
	require.Equal(t, 0, len(tb3ReferredFKs))
	require.Equal(t, 2, len(tb3Info.ForeignKeys))
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_a"),
		RefSchema: model.NewCIStr("test"),
		RefTable:  model.NewCIStr("t1"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("a")},
		OnDelete:  2,
		OnUpdate:  1,
		State:     model.StatePublic,
		Version:   1,
	}, *tb3Info.ForeignKeys[0])
	require.Equal(t, model.FKInfo{
		ID:        2,
		Name:      model.NewCIStr("fk_a2"),
		RefSchema: model.NewCIStr("test2"),
		RefTable:  model.NewCIStr("t2"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("a")},
		State:     model.StatePublic,
		Version:   1,
	}, *tb3Info.ForeignKeys[1])
	// Auto create index for foreign key usage.
	require.Equal(t, 1, len(tb3Info.Indices))
	require.Equal(t, "fk_a", tb3Info.Indices[0].Name.L)
	require.Equal(t, "`test`.`t3`, CONSTRAINT `fk_a` FOREIGN KEY (`a`) REFERENCES `t1` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT", tb3Info.ForeignKeys[0].String("test", "t3"))
	require.Equal(t, "`test`.`t3`, CONSTRAINT `fk_a2` FOREIGN KEY (`a`) REFERENCES `test2`.`t2` (`id`)", tb3Info.ForeignKeys[1].String("test", "t3"))

	tk.MustExec("set @@foreign_key_checks=0")
	tk.MustExec("drop table test2.t2")
	tb1Info = getTableInfo(t, dom, "test", "t1")
	tb3Info = getTableInfo(t, dom, "test", "t3")
	require.Equal(t, 0, len(tb1Info.ForeignKeys))
	tb1ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test"),
		ChildTable:  model.NewCIStr("t3"),
		ChildFKName: model.NewCIStr("fk_a"),
	}, *tb1ReferredFKs[0])
	tb3ReferredFKs = getTableInfoReferredForeignKeys(t, dom, "test", "t3")
	require.Equal(t, 0, len(tb3ReferredFKs))
	require.Equal(t, 2, len(tb3Info.ForeignKeys))
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_a"),
		RefSchema: model.NewCIStr("test"),
		RefTable:  model.NewCIStr("t1"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("a")},
		OnDelete:  2,
		OnUpdate:  1,
		State:     model.StatePublic,
		Version:   1,
	}, *tb3Info.ForeignKeys[0])
	require.Equal(t, model.FKInfo{
		ID:        2,
		Name:      model.NewCIStr("fk_a2"),
		RefSchema: model.NewCIStr("test2"),
		RefTable:  model.NewCIStr("t2"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("a")},
		State:     model.StatePublic,
		Version:   1,
	}, *tb3Info.ForeignKeys[1])
}

func TestCreateTableWithForeignKeyMetaInfo3(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, a int, b int as (a) virtual);")
	tk.MustExec("create table t2 (id int key, b int, foreign key fk_b(b) references test.t1(id))")
	tk.MustExec("create table t3 (id int key, b int, foreign key fk_b(b) references test.t1(id))")
	tk.MustExec("create table t4 (id int key, b int, foreign key fk_b(b) references test.t1(id))")
	tb1ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	tk.MustExec("drop table t3")
	tk.MustExec("create table t5 (id int key, b int, foreign key fk_b(b) references test.t1(id))")
	require.Equal(t, 3, len(tb1ReferredFKs))
	require.Equal(t, "t2", tb1ReferredFKs[0].ChildTable.L)
	require.Equal(t, "t3", tb1ReferredFKs[1].ChildTable.L)
	require.Equal(t, "t4", tb1ReferredFKs[2].ChildTable.L)
}

func TestCreateTableWithForeignKeyPrivilegeCheck(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create user 'u1'@'%' identified by '';")
	tk.MustExec("grant create on *.* to 'u1'@'%';")
	tk.MustExec("create table t1 (id int key);")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost", CurrentUser: true, AuthUsername: "u1", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	err := tk2.ExecToErr("create table t2 (a int, foreign key fk(a) references t1(id));")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]REFERENCES command denied to user 'u1'@'%' for table 't1'", err.Error())

	tk.MustExec("grant references on test.t1 to 'u1'@'%';")
	tk2.MustExec("create table t2 (a int, foreign key fk(a) references t1(id));")
	tk2.MustExec("create table t3 (id int key)")
	err = tk2.ExecToErr("create table t4 (a int, foreign key fk(a) references t1(id), foreign key (a) references t3(id));")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]REFERENCES command denied to user 'u1'@'%' for table 't3'", err.Error())

	tk.MustExec("grant references on test.t3 to 'u1'@'%';")
	tk2.MustExec("create table t4 (a int, foreign key fk(a) references t1(id), foreign key (a) references t3(id));")
}

func TestRenameTableWithForeignKeyMetaInfo(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@tidb_enable_foreign_key=1")
	tk.MustExec("create database test2")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, a int, b int as (a) virtual);")
	tk.MustExec("create table t2 (id int key, b int, foreign key fk_b(b) references test.t1(id))")
	tk.MustExec("use test2")
	tk.MustExec("rename table test.t2 to test2.t2")
	tb1Info := getTableInfo(t, dom, "test", "t1")
	tb2Info := getTableInfo(t, dom, "test2", "t2")
	require.Equal(t, 0, len(tb1Info.ForeignKeys))
	tb1ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test", "t1")
	require.Equal(t, 1, len(tb1ReferredFKs))
	require.Equal(t, model.ReferredFKInfo{
		Cols:        []model.CIStr{model.NewCIStr("id")},
		ChildSchema: model.NewCIStr("test2"),
		ChildTable:  model.NewCIStr("t2"),
		ChildFKName: model.NewCIStr("fk_b"),
	}, *tb1ReferredFKs[0])
	tb2ReferredFKs := getTableInfoReferredForeignKeys(t, dom, "test2", "t2")
	require.Equal(t, 0, len(tb2ReferredFKs))
	require.Equal(t, 1, len(tb2Info.ForeignKeys))
	require.Equal(t, model.FKInfo{
		ID:        1,
		Name:      model.NewCIStr("fk_b"),
		RefSchema: model.NewCIStr("test"),
		RefTable:  model.NewCIStr("t1"),
		RefCols:   []model.CIStr{model.NewCIStr("id")},
		Cols:      []model.CIStr{model.NewCIStr("b")},
		State:     model.StatePublic,
		Version:   1,
	}, *tb2Info.ForeignKeys[0])
	// Auto create index for foreign key usage.
	require.Equal(t, 1, len(tb2Info.Indices))
	require.Equal(t, "fk_b", tb2Info.Indices[0].Name.L)
	require.Equal(t, "`test2`.`t2`, CONSTRAINT `fk_b` FOREIGN KEY (`b`) REFERENCES `test`.`t1` (`id`)", tb2Info.ForeignKeys[0].String("test2", "t2"))

	// TODO(crazycs520): add test for "rename table t1"
}

func TestCreateTableWithForeignKeyDML(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@tidb_enable_foreign_key=1")
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key, a int);")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (1, 1)")
	tk.MustExec("update t1 set a = 2 where id = 1")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("create table t2 (id int key, b int, foreign key fk_b(b) references test.t1(id))")

	tk.MustExec("commit")
}

func TestCreateTableWithForeignKeyError(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@tidb_enable_foreign_key=1")
	tk.MustExec("use test")

	cases := []struct {
		prepare []string
		refer   string
		create  string
		err     string
	}{
		{
			refer:  "create table t1 (id int, a int, b int);",
			create: "create table t2 (a int, b int, foreign key fk_b(b) references T_unknown(b));",
			err:    "[schema:1824]Failed to open the referenced table 'T_unknown'",
		},
		{
			refer:  "create table t1 (id int, a int, b int);",
			create: "create table t2 (a int, b int, foreign key fk_b(b) references t1(c_unknown));",
			err:    "[schema:3734]Failed to add the foreign key constraint. Missing column 'c_unknown' for constraint 'fk_b' in the referenced table 't1'",
		},
		{
			refer:  "create table t1 (id int key, a int, b int);",
			create: "create table t2 (a int, b int, foreign key fk(c_unknown) references t1(id));",
			err:    "[ddl:1072]Key column 'c_unknown' doesn't exist in table",
		},
		{
			refer:  "create table t1 (id int, a int, b int);",
			create: "create table t2 (a int, b int, foreign key fk_b(b) references t1(b));",
			err:    "[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_b' in the referenced table 't1'",
		},
		{
			refer:  "create table t1 (id int, a int, b int not null, index(b));",
			create: "create table t2 (a int, b int not null, foreign key fk_b(b) references t1(b) on update set null);",
			err:    "[schema:1830]Column 'b' cannot be NOT NULL: needed in a foreign key constraint 'fk_b' SET NULL",
		},
		{
			refer:  "create table t1 (id int, a int, b int not null, index(b));",
			create: "create table t2 (a int, b int not null, foreign key fk_b(b) references t1(b) on delete set null);",
			err:    "[schema:1830]Column 'b' cannot be NOT NULL: needed in a foreign key constraint 'fk_b' SET NULL",
		},
		{
			refer:  "create table t1 (id int key, a int, b int as (a) virtual, index(b));",
			create: "create table t2 (a int, b int, foreign key fk_b(b) references t1(b));",
			err:    "[schema:3733]Foreign key 'fk_b' uses virtual column 'b' which is not supported.",
		},
		{
			refer:  "create table t1 (id int key, a int, b int, index(b));",
			create: "create table t2 (a int, b int as (a) virtual, foreign key fk_b(b) references t1(b));",
			err:    "[schema:3733]Foreign key 'fk_b' uses virtual column 'b' which is not supported.",
		},
		{
			refer:  "create table t1 (id int key, a int);",
			create: "create table t2 (a int, b varchar(10), foreign key fk(b) references t1(id));",
			err:    "[ddl:3780]Referencing column 'b' and referenced column 'id' in foreign key constraint 'fk' are incompatible.",
		},
		{
			refer:  "create table t1 (id int key, a int not null, index(a));",
			create: "create table t2 (a int, b int unsigned, foreign key fk_b(b) references t1(a));",
			err:    "[ddl:3780]Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
		},
		{
			refer:  "create table t1 (id int key, a bigint, index(a));",
			create: "create table t2 (a int, b int, foreign key fk_b(b) references t1(a));",
			err:    "[ddl:3780]Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
		},
		{
			refer:  "create table t1 (id int key, a varchar(10) charset utf8, index(a));",
			create: "create table t2 (a int, b varchar(10) charset utf8mb4, foreign key fk_b(b) references t1(a));",
			err:    "[ddl:3780]Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
		},
		{
			refer:  "create table t1 (id int key, a varchar(10) collate utf8_bin, index(a));",
			create: "create table t2 (a int, b varchar(10) collate utf8mb4_bin, foreign key fk_b(b) references t1(a));",
			err:    "[ddl:3780]Referencing column 'b' and referenced column 'a' in foreign key constraint 'fk_b' are incompatible.",
		},
		{
			refer:  "create table t1 (id int key, a varchar(10));",
			create: "create table t2 (a int, b varchar(10), foreign key fk_b(b) references t1(a));",
			err:    "[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_b' in the referenced table 't1'",
		},
		{
			refer:  "create table t1 (id int key, a varchar(10), index (a(5)));",
			create: "create table t2 (a int, b varchar(10), foreign key fk_b(b) references t1(a));",
			err:    "[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_b' in the referenced table 't1'",
		},
		{
			refer:  "create table t1 (id int key, a int, index(a));",
			create: "create table t2 (a int, b int, foreign key fk_b(b) references t1(id, a));",
			err:    "[schema:1239]Incorrect foreign key definition for 'fk_b': Key reference and table reference don't match",
		},
		{
			create: "create table t2 (a int key, foreign key (a) references t2(a));",
			err:    "[schema:1215]Cannot add foreign key constraint",
		},
		{
			create: "create table t2 (a int, b int, index(a,b), index(b,a), foreign key (a,b) references t2(a,b));",
			err:    "[schema:1215]Cannot add foreign key constraint",
		},
		{
			create: "create table t2 (a int, b int, index(a,b), foreign key (a,b) references t2(b,a));",
			err:    "[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_1' in the referenced table 't2'",
		},
		{
			prepare: []string{
				"set @@foreign_key_checks=0;",
				"create table t2 (a int, b int, index(a), foreign key (a) references t1(id));",
			},
			create: "create table t1 (id int, a int);",
			err:    "[schema:1822]Failed to add the foreign key constraint. Missing index for constraint 'fk_1' in the referenced table 't1'",
		},
		{
			prepare: []string{
				"set @@foreign_key_checks=0;",
				"create table t2 (a int, b int, index(a), foreign key (a) references t1(id));",
			},
			create: "create table t1 (id bigint key, a int);",
			err:    "[ddl:3780]Referencing column 'a' and referenced column 'id' in foreign key constraint 'fk_1' are incompatible.",
		},
		{
			// foreign key is not support in temporary table.
			refer:  "create temporary table t1 (id int key, b int, index(b))",
			create: "create table t2 (a int, b int, foreign key fk(b) references t1(b))",
			err:    "[schema:1824]Failed to open the referenced table 't1'",
		},
		{
			// foreign key is not support in temporary table.
			refer:  "create global temporary table t1 (id int key, b int, index(b)) on commit delete rows",
			create: "create table t2 (a int, b int, foreign key fk(b) references t1(b))",
			err:    "[schema:1215]Cannot add foreign key constraint",
		},
		{
			// foreign key is not support in temporary table.
			refer:  "create table t1 (id int key, b int, index(b))",
			create: "create temporary table t2 (a int, b int, foreign key fk(b) references t1(b))",
			err:    "[schema:1215]Cannot add foreign key constraint",
		},
		{
			// foreign key is not support in temporary table.
			refer:  "create table t1 (id int key, b int, index(b))",
			create: "create global temporary table t2 (a int, b int, foreign key fk(b) references t1(b)) on commit delete rows",
			err:    "[schema:1215]Cannot add foreign key constraint",
		},
	}
	for _, ca := range cases {
		tk.MustExec("drop table if exists t2")
		tk.MustExec("drop table if exists t1")
		tk.MustExec("set @@foreign_key_checks=1")
		for _, sql := range ca.prepare {
			tk.MustExec(sql)
		}
		if ca.refer != "" {
			tk.MustExec(ca.refer)
		}
		err := tk.ExecToErr(ca.create)
		require.Error(t, err, ca.create)
		require.Equal(t, ca.err, err.Error(), ca.create)
	}

	passCases := [][]string{
		{
			"create table t1 (id int key, a int, b int, foreign key fk(a) references t1(id))",
		},
		{
			"create table t1 (id int key, b int not null, index(b))",
			"create table t2 (a int, b int, foreign key fk_b(b) references t1(b));",
		},
		{
			"create table t1 (id int key, a varchar(10), index(a));",
			"create table t2 (a int, b varchar(20), foreign key fk_b(b) references t1(a));",
		},
		{
			"create table t1 (id int key, a decimal(10,5), index(a));",
			"create table t2 (a int, b decimal(20, 10), foreign key fk_b(b) references t1(a));",
		},
		{
			"create table t1 (id int key, a varchar(10), index (a(10)));",
			"create table t2 (a int, b varchar(20), foreign key fk_b(b) references t1(a));",
		},
		{
			"set @@foreign_key_checks=0;",
			"create table t2 (a int, b int, foreign key fk_b(b) references t_unknown(b));",
			"set @@foreign_key_checks=1;",
		},
		{
			"create table t2 (a int, b int, index(a,b), index(b,a), foreign key (a,b) references t2(b,a));",
		},
	}
	for _, ca := range passCases {
		tk.MustExec("drop table if exists t2")
		tk.MustExec("drop table if exists t1")
		for _, sql := range ca {
			tk.MustExec(sql)
		}
	}
}

func getTableInfo(t *testing.T, dom *domain.Domain, db, tb string) *model.TableInfo {
	err := dom.Reload()
	require.NoError(t, err)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr(db), model.NewCIStr(tb))
	require.NoError(t, err)
	_, exist := is.TableByID(tbl.Meta().ID)
	require.True(t, exist)
	return tbl.Meta()
}

func getTableInfoReferredForeignKeys(t *testing.T, dom *domain.Domain, db, tb string) []*model.ReferredFKInfo {
	err := dom.Reload()
	require.NoError(t, err)
	return dom.InfoSchema().GetTableReferredForeignKeys(db, tb)
}
