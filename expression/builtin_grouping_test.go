// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func createGroupingFunc(ctx sessionctx.Context, args []Expression) (*builtinGroupingSig, error) {
	argTp := []types.EvalType{types.ETInt}
	bf, err := newBaseBuiltinFuncWithTp(ctx, ast.Grouping, args, types.ETInt, argTp...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)
	sig := &builtinGroupingSig{bf, 0, map[int64]struct{}{}}
	sig.setPbCode(tipb.ScalarFuncSig_GroupingSig)
	return sig, nil
}

func TestGrouping(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		groupingID   uint64
		mode         tipb.GroupingMode
		groupingIDs  map[int64]struct{}
		expectResult uint64
	}{
		// GroupingMode_ModeBitAnd
		{1, 1, map[int64]struct{}{1: {}}, 1},
		{1, 1, map[int64]struct{}{3: {}}, 1},
		{1, 1, map[int64]struct{}{6: {}}, 0},
		{2, 1, map[int64]struct{}{1: {}}, 0},
		{2, 1, map[int64]struct{}{3: {}}, 1},
		{2, 1, map[int64]struct{}{6: {}}, 1},
		{4, 1, map[int64]struct{}{2: {}}, 0},
		{4, 1, map[int64]struct{}{4: {}}, 1},
		{4, 1, map[int64]struct{}{6: {}}, 1},

		// GroupingMode_ModeNumericCmp
		{0, 2, map[int64]struct{}{0: {}}, 0},
		{0, 2, map[int64]struct{}{2: {}}, 0},
		{2, 2, map[int64]struct{}{0: {}}, 1},
		{2, 2, map[int64]struct{}{1: {}}, 1},
		{2, 2, map[int64]struct{}{2: {}}, 0},
		{2, 2, map[int64]struct{}{3: {}}, 0},

		// GroupingMode_ModeNumericSet
		{1, 3, map[int64]struct{}{1: {}, 2: {}}, 0},
		{1, 3, map[int64]struct{}{2: {}}, 1},
		{2, 3, map[int64]struct{}{1: {}, 3: {}}, 1},
		{2, 3, map[int64]struct{}{2: {}, 3: {}}, 0},
	}

	for _, testCase := range tests {
		comment := fmt.Sprintf(`for grouping = "%d", version = "%d", groupingIDs = "%v", expectRes = "%d""`, testCase.groupingID, testCase.mode, testCase.groupingIDs, testCase.expectResult)
		args := datumsToConstants(types.MakeDatums(testCase.groupingID))

		groupingFunc, err := createGroupingFunc(ctx, args)
		groupingFunc.SetMetaVersion(testCase.mode)
		groupingFunc.SetMetaGroupingIDs(testCase.groupingIDs)
		require.NoError(t, err, comment)

		actualResult, err := evalBuiltinFunc(groupingFunc, chunk.Row{})
		require.NoError(t, err, comment)
		testutil.DatumEqual(t, types.NewDatum(testCase.expectResult), actualResult, comment)
	}
}
