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

	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

// This is an temporary function, and should not be existence for a long time.
// After the completion of full rollup feature, the construction of 'bf' in
// 'createGroupingFunc' should be implemented by 'newBaseBuiltinFuncWithTp'
func constructFieldType() types.FieldType {
	var tp types.FieldType
	tp.Init(8)
	tp.AddFlag(128)
	tp.SetFlen(20)
	tp.SetCharset("binary")
	tp.SetCollate("binary")
	return tp
}

func createGroupingFunc(args []Expression) (*BuiltinGroupingImplSig, error) {
	// TODO We should use the commented codes after the completion of rollup
	// argTp := []types.EvalType{types.ETInt}
	tp := constructFieldType()
	// bf, err := newBaseBuiltinFuncWithTp(ctx, groupingImplName, args, types.ETInt, argTp...)
	bf, err := newBaseBuiltinFuncWithFieldType(&tp, args)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)
	sig := &BuiltinGroupingImplSig{bf, 0, []map[uint64]struct{}{}, false}
	sig.setPbCode(tipb.ScalarFuncSig_GroupingSig)
	return sig, nil
}

func TestGrouping(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		groupingID   uint64
		mode         tipb.GroupingMode
		groupingIDs  map[uint64]struct{}
		expectResult uint64
	}{
		// GroupingMode_ModeBitAnd
		{1, 1, map[uint64]struct{}{1: {}}, 0},
		{1, 1, map[uint64]struct{}{3: {}}, 0},
		{1, 1, map[uint64]struct{}{6: {}}, 1},
		{2, 1, map[uint64]struct{}{1: {}}, 1},
		{2, 1, map[uint64]struct{}{3: {}}, 0},
		{2, 1, map[uint64]struct{}{6: {}}, 0},
		{4, 1, map[uint64]struct{}{2: {}}, 1},
		{4, 1, map[uint64]struct{}{4: {}}, 0},
		{4, 1, map[uint64]struct{}{6: {}}, 0},

		// GroupingMode_ModeNumericCmp
		{0, 2, map[uint64]struct{}{0: {}}, 1},
		{0, 2, map[uint64]struct{}{2: {}}, 1},
		{2, 2, map[uint64]struct{}{0: {}}, 0},
		{2, 2, map[uint64]struct{}{1: {}}, 0},
		{2, 2, map[uint64]struct{}{2: {}}, 1},
		{2, 2, map[uint64]struct{}{3: {}}, 1},

		// GroupingMode_ModeNumericSet
		{1, 3, map[uint64]struct{}{1: {}, 2: {}}, 0},
		{1, 3, map[uint64]struct{}{2: {}}, 1},
		{2, 3, map[uint64]struct{}{1: {}, 3: {}}, 1},
		{2, 3, map[uint64]struct{}{2: {}, 3: {}}, 0},
	}

	for _, testCase := range tests {
		comment := fmt.Sprintf(`for grouping = "%d", version = "%d", groupingIDs = "%v", expectRes = "%d"`, testCase.groupingID, testCase.mode, testCase.groupingIDs, testCase.expectResult)
		args := datumsToConstants(types.MakeDatums(testCase.groupingID))

		groupingFunc, err := createGroupingFunc(args)
		require.NoError(t, err, comment)

		err = groupingFunc.SetMetadata(testCase.mode, []map[uint64]struct{}{testCase.groupingIDs})
		require.NoError(t, err, comment)

		actualResult, err := evalBuiltinFunc(groupingFunc, ctx, chunk.Row{})
		require.NoError(t, err, comment)
		testutil.DatumEqual(t, types.NewDatum(testCase.expectResult), actualResult, comment)
	}
}
