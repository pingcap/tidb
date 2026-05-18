// Copyright 2026 PingCAP, Inc.
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
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestLoadableAndStoredFunctionInSameSelect(t *testing.T) {
	expression.RegisterLoadableFunctionForTest("myfunc_int")
	t.Cleanup(func() {
		expression.UnregisterLoadableFunctionForTest("myfunc_int")
	})

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.InProcedure()
	tk.MustExec("set global tidb_enable_procedure = ON")
	tk.MustExec("use test")
	tk.MustExec("drop function if exists f1")
	tk.MustExec("create function f1() returns int return 1")

	stmt, err := parser.New().ParseOneStmt("select myfunc_int(1), f1()", "", "")
	require.NoError(t, err)

	nodeW := resolve.NewNodeW(stmt)
	ret := &plannercore.PreprocessorReturn{}
	err = plannercore.Preprocess(context.Background(), tk.Session(), nodeW, plannercore.WithPreprocessorReturn(ret))
	require.NoError(t, err)

	key := [2]string{"test", "f1"}
	sc := tk.Session().GetSessionVars().StmtCtx
	sc.UserFuncCtx.Lock()
	retType, ok := sc.UserFuncCtx.StoredFuncName[key]
	sc.UserFuncCtx.Unlock()
	require.True(t, ok)
	require.NotNil(t, retType)
}
