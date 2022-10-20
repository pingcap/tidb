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

	"github.com/pingcap/tidb/expression"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
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

func TestInvokeFunc(t *testing.T) {
	defer func() {
		extension.Reset()
	}()

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

func TestRegisterFunc(t *testing.T) {
	defer func() {
		extension.Reset()
	}()

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
