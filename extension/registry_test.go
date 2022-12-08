// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package extension_test

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/sem"
	"github.com/stretchr/testify/require"
)

func TestSetupExtensions(t *testing.T) {
	defer func() {
		extension.Reset()
	}()

	extension.Reset()
	require.NoError(t, extension.Setup())
	extensions, err := extension.GetExtensions()
	require.NoError(t, err)
	require.Equal(t, 0, len(extensions.Manifests()))

	extension.Reset()
	require.NoError(t, extension.Register("test1"))
	require.NoError(t, extension.Register("test2"))
	require.NoError(t, extension.Setup())
	extensions, err = extension.GetExtensions()
	require.NoError(t, err)
	require.Equal(t, 2, len(extensions.Manifests()))
	require.Equal(t, "test1", extensions.Manifests()[0].Name())
	require.Equal(t, "test2", extensions.Manifests()[1].Name())
}

func TestExtensionRegisterName(t *testing.T) {
	defer extension.Reset()

	// test empty name
	extension.Reset()
	require.EqualError(t, extension.Register(""), "extension name should not be empty")

	// test dup name
	extension.Reset()
	require.NoError(t, extension.Register("test"))
	require.EqualError(t, extension.Register("test"), "extension with name 'test' already registered")
}

func TestRegisterExtensionWithClose(t *testing.T) {
	defer extension.Reset()

	// normal register
	extension.Reset()
	cnt := 0
	require.NoError(t, extension.Register("test1", extension.WithClose(func() {
		cnt++
	})))
	require.NoError(t, extension.Setup())
	require.Equal(t, 0, cnt)

	// reset will call close
	extension.Reset()
	require.Equal(t, 1, cnt)

	// reset again has no effect
	extension.Reset()
	require.Equal(t, 1, cnt)

	// Auto close when error
	cnt = 0
	extension.Reset()
	require.NoError(t, extension.Register("test1", extension.WithClose(func() {
		cnt++
	})))
	require.NoError(t, extension.RegisterFactory("test2", func() ([]extension.Option, error) {
		return nil, errors.New("error abc")
	}))
	require.EqualError(t, extension.Setup(), "error abc")
	require.Equal(t, 1, cnt)
}

func TestRegisterExtensionWithDyncPrivs(t *testing.T) {
	defer extension.Reset()

	origDynPrivs := privileges.GetDynamicPrivileges()
	origDynPrivs = append([]string{}, origDynPrivs...)

	extension.Reset()
	require.NoError(t, extension.Register("test", extension.WithCustomDynPrivs([]string{"priv1", "priv2"})))
	require.NoError(t, extension.Setup())
	privs := privileges.GetDynamicPrivileges()
	require.Equal(t, origDynPrivs, privs[:len(origDynPrivs)])
	require.Equal(t, []string{"PRIV1", "PRIV2"}, privs[len(origDynPrivs):])

	// test for empty dynamic privilege name
	extension.Reset()
	require.NoError(t, extension.Register("test", extension.WithCustomDynPrivs([]string{"priv1", ""})))
	require.EqualError(t, extension.Setup(), "privilege name should not be empty")
	require.Equal(t, origDynPrivs, privileges.GetDynamicPrivileges())

	// test for duplicate name with builtin
	extension.Reset()
	require.NoError(t, extension.Register("test", extension.WithCustomDynPrivs([]string{"priv1", "ROLE_ADMIN"})))
	require.EqualError(t, extension.Setup(), "privilege is already registered")
	require.Equal(t, origDynPrivs, privileges.GetDynamicPrivileges())

	// test for duplicate name with other extension
	extension.Reset()
	require.NoError(t, extension.Register("test1", extension.WithCustomDynPrivs([]string{"priv1"})))
	require.NoError(t, extension.Register("test2", extension.WithCustomDynPrivs([]string{"priv2", "priv1"})))
	require.EqualError(t, extension.Setup(), "privilege is already registered")
	require.Equal(t, origDynPrivs, privileges.GetDynamicPrivileges())
}

func TestRegisterExtensionWithSysVars(t *testing.T) {
	defer extension.Reset()

	sysVar1 := &variable.SysVar{
		Scope: variable.ScopeGlobal | variable.ScopeSession,
		Name:  "var1",
		Value: variable.On,
		Type:  variable.TypeBool,
	}

	sysVar2 := &variable.SysVar{
		Scope: variable.ScopeSession,
		Name:  "var2",
		Value: "val2",
		Type:  variable.TypeStr,
	}

	// normal register
	extension.Reset()
	require.NoError(t, extension.Register("test", extension.WithCustomSysVariables([]*variable.SysVar{sysVar1, sysVar2})))
	require.NoError(t, extension.Setup())
	require.Same(t, sysVar1, variable.GetSysVar("var1"))
	require.Same(t, sysVar2, variable.GetSysVar("var2"))

	// test for empty name
	extension.Reset()
	require.NoError(t, extension.Register("test", extension.WithCustomSysVariables([]*variable.SysVar{
		{Scope: variable.ScopeGlobal, Name: "", Value: "val3"},
	})))
	require.EqualError(t, extension.Setup(), "system var name should not be empty")
	require.Nil(t, variable.GetSysVar(""))

	// test for duplicate name with builtin
	extension.Reset()
	require.NoError(t, extension.Register("test", extension.WithCustomSysVariables([]*variable.SysVar{
		sysVar1,
		{Scope: variable.ScopeGlobal, Name: variable.TiDBSnapshot, Value: "val3"},
	})))
	require.EqualError(t, extension.Setup(), "system var 'tidb_snapshot' has already registered")
	require.Nil(t, variable.GetSysVar("var1"))
	require.Equal(t, "", variable.GetSysVar(variable.TiDBSnapshot).Value)
	require.Equal(t, variable.ScopeSession, variable.GetSysVar(variable.TiDBSnapshot).Scope)

	// test for duplicate name with other extension
	extension.Reset()
	require.NoError(t, extension.Register("test1", extension.WithCustomSysVariables([]*variable.SysVar{sysVar1, sysVar2})))
	require.NoError(t, extension.Register("test2", extension.WithCustomSysVariables([]*variable.SysVar{sysVar1})))
	require.EqualError(t, extension.Setup(), "system var 'var1' has already registered")
	require.Nil(t, variable.GetSysVar("var1"))
	require.Nil(t, variable.GetSysVar("var2"))
}

func TestSetVariablePrivilege(t *testing.T) {
	defer extension.Reset()

	sysVar1 := &variable.SysVar{
		Scope:    variable.ScopeGlobal | variable.ScopeSession,
		Name:     "var1",
		Value:    "1",
		MinValue: 0,
		MaxValue: 100,
		Type:     variable.TypeInt,
		RequireDynamicPrivileges: func(isGlobal bool, sem bool) []string {
			privs := []string{"priv1"}
			if isGlobal {
				privs = append(privs, "priv2")
			}

			if sem {
				privs = append(privs, "restricted_priv3")
			}

			return privs
		},
	}

	extension.Reset()
	require.NoError(t, extension.Register(
		"test",
		extension.WithCustomSysVariables([]*variable.SysVar{sysVar1}),
		extension.WithCustomDynPrivs([]string{"priv1", "priv2", "restricted_priv3"}),
	))
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create user u2@localhost")

	tk1 := testkit.NewTestKit(t, store)
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))

	tk2 := testkit.NewTestKit(t, store)
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, nil, nil))

	sem.Disable()
	tk1.MustExec("set @@var1=7")
	tk1.MustQuery("select @@var1").Check(testkit.Rows("7"))

	require.EqualError(t, tk2.ExecToErr("set @@var1=10"), "[planner:1227]Access denied; you need (at least one of) the SUPER or priv1 privilege(s) for this operation")
	tk2.MustQuery("select @@var1").Check(testkit.Rows("1"))

	tk.MustExec("GRANT priv1 on *.* TO u2@localhost")
	tk2.MustExec("set @@var1=8")
	tk2.MustQuery("select @@var1").Check(testkit.Rows("8"))

	tk1.MustExec("set @@global.var1=17")
	tk1.MustQuery("select @@global.var1").Check(testkit.Rows("17"))

	tk.MustExec("GRANT SYSTEM_VARIABLES_ADMIN on *.* TO u2@localhost")
	require.EqualError(t, tk2.ExecToErr("set @@global.var1=18"), "[planner:1227]Access denied; you need (at least one of) the SUPER or priv2 privilege(s) for this operation")
	tk2.MustQuery("select @@global.var1").Check(testkit.Rows("17"))

	tk.MustExec("GRANT priv2 on *.* TO u2@localhost")
	tk2.MustExec("set @@global.var1=18")
	tk2.MustQuery("select @@global.var1").Check(testkit.Rows("18"))

	sem.Enable()
	defer sem.Disable()

	require.EqualError(t, tk1.ExecToErr("set @@global.var1=27"), "[planner:1227]Access denied; you need (at least one of) the restricted_priv3 privilege(s) for this operation")
	tk1.MustQuery("select @@global.var1").Check(testkit.Rows("18"))

	require.EqualError(t, tk2.ExecToErr("set @@global.var1=27"), "[planner:1227]Access denied; you need (at least one of) the restricted_priv3 privilege(s) for this operation")
	tk2.MustQuery("select @@global.var1").Check(testkit.Rows("18"))

	tk.MustExec("GRANT restricted_priv3 on *.* TO u2@localhost")
	tk2.MustExec("set @@global.var1=28")
	tk2.MustQuery("select @@global.var1").Check(testkit.Rows("28"))
}

func TestCustomAccessCheck(t *testing.T) {
	defer extension.Reset()
	extension.Reset()

	require.NoError(t, extension.Register(
		"test",
		extension.WithCustomDynPrivs([]string{"priv1", "priv2", "restricted_priv3"}),
		extension.WithCustomAccessCheck(func(db, tbl, column string, priv mysql.PrivilegeType, sem bool) []string {
			if db != "test" || tbl != "t1" {
				return nil
			}

			var privs []string
			if priv == mysql.SelectPriv {
				privs = append(privs, "priv1")
			} else if priv == mysql.UpdatePriv {
				privs = append(privs, "priv2")
				if sem {
					privs = append(privs, "restricted_priv3")
				}
			} else {
				return nil
			}

			return privs
		}),
	))
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create user u2@localhost")

	tk1 := testkit.NewTestKit(t, store)
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))
	tk1.MustExec("use test")

	tk2 := testkit.NewTestKit(t, store)
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, nil, nil))
	tk.MustExec("GRANT all on test.t1 TO u2@localhost")
	tk2.MustExec("use test")

	tk1.MustExec("create table t1(id int primary key, v int)")
	tk1.MustExec("insert into t1 values (1, 10), (2, 20)")

	tk1.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	tk1.MustQuery("select * from t1").Check(testkit.Rows("1 10", "2 20"))

	require.EqualError(t, tk2.ExecToErr("select * from t1 where id=1"), "[planner:1142]SELECT command denied to user 'u2'@'localhost' for table 't1'")
	require.EqualError(t, tk2.ExecToErr("select * from t1"), "[planner:1142]SELECT command denied to user 'u2'@'localhost' for table 't1'")

	tk.MustExec("GRANT priv1 on *.* TO u2@localhost")
	tk2.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 10", "2 20"))

	require.EqualError(t, tk2.ExecToErr("update t1 set v=11 where id=1"), "[planner:8121]privilege check for 'Update' fail")
	require.EqualError(t, tk2.ExecToErr("update t1 set v=11 where id<2"), "[planner:8121]privilege check for 'Update' fail")
	tk2.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))

	tk.MustExec("GRANT priv2 on *.* TO u2@localhost")
	tk2.MustExec("update t1 set v=11 where id=1")
	tk2.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 11"))

	tk2.MustExec("update t1 set v=12 where id<2")
	tk2.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 12"))

	sem.Enable()
	defer sem.Disable()

	require.EqualError(t, tk1.ExecToErr("update t1 set v=21 where id=1"), "[planner:8121]privilege check for 'Update' fail")
	require.EqualError(t, tk1.ExecToErr("update t1 set v=21 where id<2"), "[planner:8121]privilege check for 'Update' fail")
	tk1.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 12"))

	require.EqualError(t, tk2.ExecToErr("update t1 set v=21 where id=1"), "[planner:8121]privilege check for 'Update' fail")
	require.EqualError(t, tk2.ExecToErr("update t1 set v=21 where id<2"), "[planner:8121]privilege check for 'Update' fail")
	tk2.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 12"))

	tk.MustExec("GRANT restricted_priv3 on *.* TO u2@localhost")
	tk2.MustExec("update t1 set v=31 where id=1")
	tk2.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 31"))

	tk2.MustExec("update t1 set v=32 where id<2")
	tk2.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 32"))
}
