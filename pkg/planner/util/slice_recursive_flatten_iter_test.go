// Copyright 2024 PingCAP, Inc.
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

package util_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/stretchr/testify/require"
)

type input struct {
	slice        [][][]string
	continueVals []string
	breakVal     string
}

type outputItem struct {
	idx   int
	value string
}

func TestSliceRecursiveFlattenIter(t *testing.T) {
	var testCases = []struct {
		in  input
		out []outputItem
	}{
		{
			in: input{
				slice: nil,
			},
			out: []outputItem{},
		},
		{
			in: input{
				slice: [][][]string{{{}, nil}, nil, {}, {{}, {}}, nil, {}, {{}}, {nil, nil}},
			},
			out: []outputItem{},
		},
		{
			in: input{
				slice: [][][]string{{{"111", "", "333"}}},
			},
			out: []outputItem{
				{0, "111"},
				{1, ""},
				{2, "333"},
			},
		},
		{
			in: input{
				slice: [][][]string{nil, {{"111", "", "333"}, nil, {"234"}}, nil},
			},
			out: []outputItem{
				{0, "111"},
				{1, ""},
				{2, "333"},
				{3, "234"},
			},
		},
		{
			in: input{
				slice:        [][][]string{{{"111", "", "333"}, {}, {"444", "555", "666"}}},
				continueVals: []string{"444"},
			},
			out: []outputItem{
				{0, "111"},
				{1, ""},
				{2, "333"},
				{4, "555"},
				{5, "666"},
			},
		},
		{
			in: input{
				slice:        [][][]string{{{"111", "", "333"}, nil, {"444", "555", "666"}}},
				continueVals: []string{"111"},
				breakVal:     "555",
			},
			out: []outputItem{
				{1, ""},
				{2, "333"},
				{3, "444"},
			},
		},
		{
			in: input{
				slice:        [][][]string{{nil, {"", "", "998877"}, nil, {}, {}, nil, {"321"}}, {{"555", "222", "1"}}},
				continueVals: []string{"321"},
				breakVal:     "222",
			},
			out: []outputItem{
				{0, ""},
				{1, ""},
				{2, "998877"},
				{4, "555"},
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Case #%d", i), func(t *testing.T) {
			out := make([]outputItem, 0, len(tc.out))
			for idx, val := range util.SliceRecursiveFlattenIter[string](tc.in.slice) {
				if slices.Contains(tc.in.continueVals, val) {
					continue
				}
				if tc.in.breakVal != "" && val == tc.in.breakVal {
					break
				}
				out = append(out, outputItem{idx, val})
			}
			require.Equal(t, tc.out, out)
		})
	}
}
