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

package expression

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege/lbac"
	lbacmodel "github.com/pingcap/tidb/pkg/privilege/lbac/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

type mockPrivilegeChecker struct{}

func (mockPrivilegeChecker) RequestVerification(db, table, column string, priv mysql.PrivilegeType) bool {
	return true
}

func (mockPrivilegeChecker) RequestDynamicVerification(privName string, grantable bool) bool {
	return true
}

func (mockPrivilegeChecker) GetSecurityLabelCache() *lbac.SecurityLabelCache {
	return nil
}

func TestSeclabelFunctions(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		fn     string
		args   []any
		want   string
		isNull bool
	}{
		{ast.Seclabel, []any{"DATA_ACCESS", "TOP_SECRET|FIN|USA"}, "TOP_SECRET|FIN|USA", false},
		{ast.Seclabel, []any{nil, "TOP_SECRET|FIN|USA"}, "", true},
		{ast.Seclabel, []any{"DATA_ACCESS", nil}, "", true},
		{ast.SeclabelToChar, []any{"DATA_ACCESS", "CONFIDENTIAL|HR|CANADA"}, "CONFIDENTIAL|HR|CANADA", false},
		{ast.SeclabelToChar, []any{nil, "CONFIDENTIAL|HR|CANADA"}, "", true},
		{ast.SeclabelToChar, []any{"DATA_ACCESS", nil}, "", true},
	}

	for i, tc := range cases {
		fc := funcs[tc.fn]
		datums := types.MakeDatums(tc.args...)
		f, err := fc.getFunction(ctx, datumsToConstants(datums))
		require.NoError(t, err)
		require.NotNil(t, f)
		got, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		if tc.isNull {
			require.Truef(t, got.IsNull(), "[%d]: args: %v", i, tc.args)
			continue
		}
		require.Equalf(t, tc.want, got.GetString(), "[%d]: args: %v", i, tc.args)
	}
}

func TestSeclabelEvalContextUnavailable(t *testing.T) {
	oldEnable := variable.EnableLBAC.Load()
	variable.EnableLBAC.Store(true)
	defer variable.EnableLBAC.Store(oldEnable)

	evalCtx := exprstatic.NewEvalContext(exprstatic.WithOptionalProperty(expropt.PrivilegeCheckerProvider(func() expropt.PrivilegeChecker {
		return mockPrivilegeChecker{}
	})))
	exprCtx := exprstatic.NewExprContext(exprstatic.WithEvalCtx(evalCtx))
	fc := funcs[ast.Seclabel]
	datums := types.MakeDatums("DATA_ACCESS", "TOP_SECRET|FIN|USA")
	f, err := fc.getFunction(exprCtx, datumsToConstants(datums))
	require.NoError(t, err)
	_, err = evalBuiltinFunc(f, evalCtx, chunk.Row{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "lbac privilege cache unavailable")
}

func TestLBACDominatesFunction(t *testing.T) {
	ctx := createContext(t)
	grant := lbacmodel.Label{
		Version:    lbacmodel.CurrentVersion,
		Components: []lbacmodel.Component{{ID: 1, Type: lbacmodel.ComponentTypeArray, Value: lbacmodel.ArrayValue{Ordinal: 1}}},
	}
	rowOK := lbacmodel.Label{
		Version:    lbacmodel.CurrentVersion,
		Components: []lbacmodel.Component{{ID: 1, Type: lbacmodel.ComponentTypeArray, Value: lbacmodel.ArrayValue{Ordinal: 3}}},
	}
	rowNoAccess := lbacmodel.Label{
		Version:    lbacmodel.CurrentVersion,
		Components: []lbacmodel.Component{{ID: 1, Type: lbacmodel.ComponentTypeArray, Value: lbacmodel.ArrayValue{Ordinal: 0}}},
	}
	grantBytes, err := lbacmodel.EncodeLabel(grant)
	require.NoError(t, err)
	rowOKBytes, err := lbacmodel.EncodeLabel(rowOK)
	require.NoError(t, err)
	rowNoAccessBytes, err := lbacmodel.EncodeLabel(rowNoAccess)
	require.NoError(t, err)

	fc := funcs[ast.LBACDominates]
	datums := types.MakeDatums(grantBytes, rowOKBytes)
	f, err := fc.getFunction(ctx, datumsToConstants(datums))
	require.NoError(t, err)
	got, err := evalBuiltinFunc(f, ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, int64(1), got.GetInt64())

	datums = types.MakeDatums(grantBytes, rowNoAccessBytes)
	f, err = fc.getFunction(ctx, datumsToConstants(datums))
	require.NoError(t, err)
	got, err = evalBuiltinFunc(f, ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, int64(0), got.GetInt64())
}
