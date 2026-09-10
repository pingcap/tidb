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

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// TestTiDBOnlyCollationNotPushedDown verifies that expressions using a collation only TiDB can
// evaluate are kept out of the TiKV/TiFlash coprocessor, while standard collations still push down.
func TestTiDBOnlyCollationNotPushedDown(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	ctx := mock.NewContext()
	client := new(mock.Client)
	pushDownCtx := NewPushDownContextFromSessionVars(ctx, ctx.GetSessionVars(), client)

	colWith := func(coll string) *Column {
		ft := types.NewFieldType(mysql.TypeVarchar)
		ft.SetCharset("utf8mb4")
		ft.SetCollate(coll)
		return &Column{RetType: ft, Index: 0}
	}

	icuCol := []Expression{colWith("utf8mb4_de_0900_as_ci_kn")}
	require.False(t, CanExprsPushDown(pushDownCtx, icuCol, kv.TiKV))
	require.False(t, CanExprsPushDown(pushDownCtx, icuCol, kv.TiFlash))
	// TiDB itself can evaluate it.
	require.True(t, CanExprsPushDown(pushDownCtx, icuCol, kv.TiDB))

	// A coprocessor-supported collation is unaffected.
	stdCol := []Expression{colWith("utf8mb4_0900_ai_ci")}
	require.True(t, CanExprsPushDown(pushDownCtx, stdCol, kv.TiKV))
}
