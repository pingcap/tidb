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
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sem"
	"github.com/stretchr/testify/require"
)

var customFunc1 = &extension.FunctionDef{
	Name:   "custom_func1",
	EvalTp: types.ETString,
	ArgTps: []types.EvalType{
		types.ETInt,
		types.ETString,
	},
	EvalStringFunc: func(ctx extension.FunctionContext, row chunk.Row) (string, bool, error) {
		args, err := ctx.EvalArgs(row)
		if err != nil {
			return "", false, err
		}

		if args[1].GetString() == "error" {
			return "", false, errors.New("custom error")
		}

		return fmt.Sprintf("%d,%s", args[0].GetInt64(), args[1].GetString()), false, nil
	},
}

var customFunc2 = &extension.FunctionDef{
	Name:   "custom_func2",
	EvalTp: types.ETInt,
	ArgTps: []types.EvalType{
		types.ETInt,
		types.ETInt,
	},
	EvalIntFunc: func(ctx extension.FunctionContext, row chunk.Row) (int64, bool, error) {
		args, err := ctx.EvalArgs(row)
		if err != nil {
			return 0, false, err
		}
		return args[0].GetInt64()*100 + args[1].GetInt64(), false, nil
	},
}

func TestExtensionFuncCtx(t *testing.T) {
	defer extension.Reset()
	extension.Reset()

	invoked := false
	var user *auth.UserIdentity
	var currentDB string
	var activeRoles []*auth.RoleIdentity
	var conn *variable.ConnectionInfo

	require.NoError(t, extension.Register("test", extension.WithCustomFunctions([]*extension.FunctionDef{
		{
			Name:   "custom_get_ctx",
			EvalTp: types.ETString,
			EvalStringFunc: func(ctx extension.FunctionContext, row chunk.Row) (string, bool, error) {
				require.False(t, invoked)
				invoked = true
				user = ctx.User()
				currentDB = ctx.CurrentDB()
				activeRoles = ctx.ActiveRoles()
				conn = ctx.ConnectionInfo()
				return "done", false, nil
			},
		},
	})))

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create user u1@localhost")
	tk.MustExec("create role r1")
	tk.MustExec("grant r1 to u1@localhost")
	tk.MustExec("grant ALL ON test.* to u1@localhost")

	tk1 := testkit.NewTestKit(t, store)
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil))
	tk1.MustExec("set role r1")
	tk1.MustExec("use test")
	tk1.Session().GetSessionVars().ConnectionInfo = &variable.ConnectionInfo{
		ConnectionID: 12345,
		User:         "u1",
	}

	tk1.MustQuery("select custom_get_ctx()").Check(testkit.Rows("done"))

	require.True(t, invoked)
	require.NotNil(t, user)
	require.Equal(t, *tk1.Session().GetSessionVars().User, *user)
	require.Equal(t, "test", currentDB)
	require.NotNil(t, conn)
	require.Equal(t, *tk1.Session().GetSessionVars().ConnectionInfo, *conn)
	require.Equal(t, 1, len(activeRoles))
	require.Equal(t, auth.RoleIdentity{Username: "r1", Hostname: "%"}, *activeRoles[0])
}

func TestInvokeExtensionFunc(t *testing.T) {
	defer extension.Reset()
	extension.Reset()

	extension.Reset()
	orgFuncList := expression.GetBuiltinList()
	checkFuncList(t, orgFuncList)
	require.NoError(t, extension.Register("test", extension.WithCustomFunctions([]*extension.FunctionDef{
		customFunc1,
		customFunc2,
	})))
	require.NoError(t, extension.Setup())
	checkFuncList(t, orgFuncList, "custom_func1", "custom_func2")

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select custom_func1(1, 'abc')").Check(testkit.Rows("1,abc"))
	tk.MustQuery("select custom_func2(7, 8)").Check(testkit.Rows("708"))
	require.EqualError(t, tk.QueryToErr("select custom_func1(1, 'error')"), "custom error")
	require.EqualError(t, tk.ExecToErr("select custom_func1(1)"), "[expression:1582]Incorrect parameter count in the call to native function 'custom_func1'")

	extension.Reset()
	checkFuncList(t, orgFuncList)
	store2 := testkit.CreateMockStore(t)
	tk2 := testkit.NewTestKit(t, store2)
	tk2.MustExec("use test")
	require.EqualError(t, tk2.ExecToErr("select custom_func1(1, 'abc')"), "[expression:1305]FUNCTION test.custom_func1 does not exist")
	require.EqualError(t, tk2.ExecToErr("select custom_func2(1, 2)"), "[expression:1305]FUNCTION test.custom_func2 does not exist")
}

func TestExtensionFuncDynamicArgLen(t *testing.T) {
	defer extension.Reset()
	extension.Reset()

	fnDef := &extension.FunctionDef{
		Name:            "dynamic_arg_func",
		EvalTp:          types.ETInt,
		OptionalArgsLen: 1,
		ArgTps: []types.EvalType{
			types.ETInt,
			types.ETInt,
			types.ETInt,
			types.ETInt,
		},
		EvalIntFunc: func(ctx extension.FunctionContext, row chunk.Row) (int64, bool, error) {
			args, err := ctx.EvalArgs(row)
			if err != nil {
				return 0, false, err
			}

			result := int64(0)
			for _, arg := range args {
				result = result*10 + arg.GetInt64()
			}

			return result, false, nil
		},
	}

	require.NoError(t, extension.Register("test", extension.WithCustomFunctions([]*extension.FunctionDef{fnDef})))
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select dynamic_arg_func(1, 2, 3)").Check(testkit.Rows("123"))
	tk.MustQuery("select dynamic_arg_func(1, 2, 3, 4)").Check(testkit.Rows("1234"))

	expectedErrMsg := "[expression:1582]Incorrect parameter count in the call to native function 'dynamic_arg_func'"
	require.EqualError(t, tk.ExecToErr("select dynamic_arg_func()"), expectedErrMsg)
	require.EqualError(t, tk.ExecToErr("select dynamic_arg_func(1)"), expectedErrMsg)
	require.EqualError(t, tk.ExecToErr("select dynamic_arg_func(1, 2)"), expectedErrMsg)
}

func TestRegisterExtensionFunc(t *testing.T) {
	defer extension.Reset()

	// nil func
	extension.Reset()
	orgFuncList := expression.GetBuiltinList()
	checkFuncList(t, orgFuncList)
	require.NoError(t, extension.Register("test", extension.WithCustomFunctions([]*extension.FunctionDef{
		customFunc1,
		nil,
	})))
	require.EqualError(t, extension.Setup(), "extension function def is nil")
	checkFuncList(t, orgFuncList)

	// dup name with builtin
	extension.Reset()
	var def extension.FunctionDef
	def = *customFunc1
	def.Name = "substring"
	require.NoError(t, extension.Register("test", extension.WithCustomFunctions([]*extension.FunctionDef{
		customFunc1,
		&def,
	})))
	require.EqualError(t, extension.Setup(), "extension function name 'substring' conflict with builtin")
	checkFuncList(t, orgFuncList)

	// empty func name
	extension.Reset()
	def = *customFunc1
	def.Name = ""
	require.NoError(t, extension.Register("test", extension.WithCustomFunctions([]*extension.FunctionDef{
		&def,
	})))
	require.EqualError(t, extension.Setup(), "extension function name should not be empty")
	checkFuncList(t, orgFuncList)

	// dup name with other func in one extension
	extension.Reset()
	require.NoError(t, extension.Register("test", extension.WithCustomFunctions([]*extension.FunctionDef{
		customFunc1,
		customFunc1,
	})))
	require.EqualError(t, extension.Setup(), "duplicated extension function name 'custom_func1'")
	checkFuncList(t, orgFuncList)

	// dup name with other func in different extension
	extension.Reset()
	require.NoError(t, extension.Register("test1", extension.WithCustomFunctions([]*extension.FunctionDef{
		customFunc1,
	})))
	require.NoError(t, extension.Register("test2", extension.WithCustomFunctions([]*extension.FunctionDef{
		customFunc1,
	})))
	require.EqualError(t, extension.Setup(), "duplicated extension function name 'custom_func1'")
	checkFuncList(t, orgFuncList)
}

func checkFuncList(t *testing.T, orgList []string, customFuncs ...string) {
	for _, name := range orgList {
		require.False(t, strings.HasPrefix(name, "custom_"), name)
	}

	checkList := make([]string, 0, len(orgList)+len(customFuncs))
	checkList = append(checkList, orgList...)
	checkList = append(checkList, customFuncs...)
	sort.Strings(checkList)
	require.Equal(t, checkList, expression.GetBuiltinList())
}

func TestExtensionFuncPrivilege(t *testing.T) {
	defer func() {
		extension.Reset()
		sem.Disable()
	}()

	extension.Reset()
	require.NoError(t, extension.Register("test",
		extension.WithCustomFunctions([]*extension.FunctionDef{
			{
				Name:   "custom_no_priv_func",
				EvalTp: types.ETString,
				EvalStringFunc: func(ctx extension.FunctionContext, row chunk.Row) (string, bool, error) {
					return "zzz", false, nil
				},
			},
			{
				Name:   "custom_only_dyn_priv_func",
				EvalTp: types.ETString,
				RequireDynamicPrivileges: func(sem bool) []string {
					return []string{"CUSTOM_DYN_PRIV_1"}
				},
				EvalStringFunc: func(ctx extension.FunctionContext, row chunk.Row) (string, bool, error) {
					return "abc", false, nil
				},
			},
			{
				Name:   "custom_only_sem_dyn_priv_func",
				EvalTp: types.ETString,
				RequireDynamicPrivileges: func(sem bool) []string {
					if sem {
						return []string{"RESTRICTED_CUSTOM_DYN_PRIV_2"}
					}
					return nil
				},
				EvalStringFunc: func(ctx extension.FunctionContext, row chunk.Row) (string, bool, error) {
					return "def", false, nil
				},
			},
			{
				Name:   "custom_both_dyn_priv_func",
				EvalTp: types.ETString,
				RequireDynamicPrivileges: func(sem bool) []string {
					if sem {
						return []string{"RESTRICTED_CUSTOM_DYN_PRIV_2"}
					}
					return []string{"CUSTOM_DYN_PRIV_1"}
				},
				EvalStringFunc: func(ctx extension.FunctionContext, row chunk.Row) (string, bool, error) {
					return "ghi", false, nil
				},
			},
		}),
		extension.WithCustomDynPrivs([]string{
			"CUSTOM_DYN_PRIV_1",
			"RESTRICTED_CUSTOM_DYN_PRIV_2",
		}),
	))
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create user u1@localhost")

	tk.MustExec("create user u2@localhost")
	tk.MustExec("GRANT CUSTOM_DYN_PRIV_1 on *.* TO u2@localhost")

	tk.MustExec("create user u3@localhost")
	tk.MustExec("GRANT RESTRICTED_CUSTOM_DYN_PRIV_2 on *.* TO u3@localhost")

	tk.MustExec("create user u4@localhost")
	tk.MustExec("GRANT CUSTOM_DYN_PRIV_1, RESTRICTED_CUSTOM_DYN_PRIV_2 on *.* TO u4@localhost")

	tk1 := testkit.NewTestKit(t, store)

	// root has all privileges by default
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))
	tk1.MustQuery("select custom_no_priv_func()").Check(testkit.Rows("zzz"))
	tk1.MustQuery("select custom_only_dyn_priv_func()").Check(testkit.Rows("abc"))
	tk1.MustQuery("select custom_only_sem_dyn_priv_func()").Check(testkit.Rows("def"))
	tk1.MustQuery("select custom_both_dyn_priv_func()").Check(testkit.Rows("ghi"))

	// u1 in non-sem
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil))
	tk1.MustQuery("select custom_no_priv_func()").Check(testkit.Rows("zzz"))
	require.EqualError(t, tk1.ExecToErr("select custom_only_dyn_priv_func()"), "[expression:1227]Access denied; you need (at least one of) the SUPER or CUSTOM_DYN_PRIV_1 privilege(s) for this operation")
	tk1.MustQuery("select custom_only_sem_dyn_priv_func()").Check(testkit.Rows("def"))
	require.EqualError(t, tk1.ExecToErr("select custom_both_dyn_priv_func()"), "[expression:1227]Access denied; you need (at least one of) the SUPER or CUSTOM_DYN_PRIV_1 privilege(s) for this operation")

	// u2 in non-sem
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, nil, nil))
	tk1.MustQuery("select custom_no_priv_func()").Check(testkit.Rows("zzz"))
	tk1.MustQuery("select custom_only_dyn_priv_func()").Check(testkit.Rows("abc"))
	tk1.MustQuery("select custom_only_sem_dyn_priv_func()").Check(testkit.Rows("def"))
	tk1.MustQuery("select custom_both_dyn_priv_func()").Check(testkit.Rows("ghi"))

	// u3 in non-sem
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "u3", Hostname: "localhost"}, nil, nil))
	tk1.MustQuery("select custom_no_priv_func()").Check(testkit.Rows("zzz"))
	require.EqualError(t, tk1.ExecToErr("select custom_only_dyn_priv_func()"), "[expression:1227]Access denied; you need (at least one of) the SUPER or CUSTOM_DYN_PRIV_1 privilege(s) for this operation")
	tk1.MustQuery("select custom_only_sem_dyn_priv_func()").Check(testkit.Rows("def"))
	require.EqualError(t, tk1.ExecToErr("select custom_both_dyn_priv_func()"), "[expression:1227]Access denied; you need (at least one of) the SUPER or CUSTOM_DYN_PRIV_1 privilege(s) for this operation")

	// u4 in non-sem
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "u4", Hostname: "localhost"}, nil, nil))
	tk1.MustQuery("select custom_no_priv_func()").Check(testkit.Rows("zzz"))
	tk1.MustQuery("select custom_only_dyn_priv_func()").Check(testkit.Rows("abc"))
	tk1.MustQuery("select custom_only_sem_dyn_priv_func()").Check(testkit.Rows("def"))
	tk1.MustQuery("select custom_both_dyn_priv_func()").Check(testkit.Rows("ghi"))

	sem.Enable()

	// root in sem
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil))
	tk1.MustQuery("select custom_no_priv_func()").Check(testkit.Rows("zzz"))
	tk1.MustQuery("select custom_only_dyn_priv_func()").Check(testkit.Rows("abc"))
	require.EqualError(t, tk1.ExecToErr("select custom_only_sem_dyn_priv_func()"), "[expression:1227]Access denied; you need (at least one of) the RESTRICTED_CUSTOM_DYN_PRIV_2 privilege(s) for this operation")
	require.EqualError(t, tk1.ExecToErr("select custom_both_dyn_priv_func()"), "[expression:1227]Access denied; you need (at least one of) the RESTRICTED_CUSTOM_DYN_PRIV_2 privilege(s) for this operation")

	// u1 in sem
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil))
	tk1.MustQuery("select custom_no_priv_func()").Check(testkit.Rows("zzz"))
	require.EqualError(t, tk1.ExecToErr("select custom_only_dyn_priv_func()"), "[expression:1227]Access denied; you need (at least one of) the CUSTOM_DYN_PRIV_1 privilege(s) for this operation")
	require.EqualError(t, tk1.ExecToErr("select custom_only_sem_dyn_priv_func()"), "[expression:1227]Access denied; you need (at least one of) the RESTRICTED_CUSTOM_DYN_PRIV_2 privilege(s) for this operation")
	require.EqualError(t, tk1.ExecToErr("select custom_both_dyn_priv_func()"), "[expression:1227]Access denied; you need (at least one of) the RESTRICTED_CUSTOM_DYN_PRIV_2 privilege(s) for this operation")

	// u2 in sem
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, nil, nil))
	tk1.MustQuery("select custom_no_priv_func()").Check(testkit.Rows("zzz"))
	tk1.MustQuery("select custom_only_dyn_priv_func()").Check(testkit.Rows("abc"))
	require.EqualError(t, tk1.ExecToErr("select custom_only_sem_dyn_priv_func()"), "[expression:1227]Access denied; you need (at least one of) the RESTRICTED_CUSTOM_DYN_PRIV_2 privilege(s) for this operation")
	require.EqualError(t, tk1.ExecToErr("select custom_both_dyn_priv_func()"), "[expression:1227]Access denied; you need (at least one of) the RESTRICTED_CUSTOM_DYN_PRIV_2 privilege(s) for this operation")

	// u3 in sem
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "u3", Hostname: "localhost"}, nil, nil))
	tk1.MustQuery("select custom_no_priv_func()").Check(testkit.Rows("zzz"))
	require.EqualError(t, tk1.ExecToErr("select custom_only_dyn_priv_func()"), "[expression:1227]Access denied; you need (at least one of) the CUSTOM_DYN_PRIV_1 privilege(s) for this operation")
	tk1.MustQuery("select custom_only_sem_dyn_priv_func()").Check(testkit.Rows("def"))
	tk1.MustQuery("select custom_both_dyn_priv_func()").Check(testkit.Rows("ghi"))

	// u4 in sem
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "u4", Hostname: "localhost"}, nil, nil))
	tk1.MustQuery("select custom_no_priv_func()").Check(testkit.Rows("zzz"))
	tk1.MustQuery("select custom_only_dyn_priv_func()").Check(testkit.Rows("abc"))
	tk1.MustQuery("select custom_only_sem_dyn_priv_func()").Check(testkit.Rows("def"))
	tk1.MustQuery("select custom_both_dyn_priv_func()").Check(testkit.Rows("ghi"))
}
