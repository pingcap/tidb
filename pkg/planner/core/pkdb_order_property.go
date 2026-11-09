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

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
)

// matchStrictIndexOrder checks whether an index can provide the requested
// order without merging multiple ranges. It deliberately contains no access
// path or range side effects so logical prechecks and physical matching share
// the same column-order semantics.
func matchStrictIndexOrder(
	idxCols []*expression.Column,
	idxColLens []int,
	constCols []bool,
	sortItems []property.SortItem,
) bool {
	if len(sortItems) == 0 || len(idxCols) < len(sortItems) {
		return false
	}
	for _, item := range sortItems[1:] {
		if item.Desc != sortItems[0].Desc {
			return false
		}
	}
	idxPos := 0
	for _, item := range sortItems {
		matched := false
		for idxPos < len(idxCols) {
			if matchFullLengthIndexColumn(idxCols[idxPos], idxColLens, idxPos, item) {
				idxPos++
				matched = true
				break
			}
			if isConstantIndexColumn(constCols, idxPos) {
				idxPos++
				continue
			}
			break
		}
		if !matched {
			return false
		}
	}
	return true
}

func matchFullLengthIndexColumn(idxCol *expression.Column, idxColLens []int, idxPos int, sortItem property.SortItem) bool {
	return idxPos < len(idxColLens) && idxColLens[idxPos] == types.UnspecifiedLength &&
		idxCol != nil && sortItem.Col != nil && sortItem.Col.EqualColumn(idxCol)
}

func isConstantIndexColumn(constCols []bool, idxPos int) bool {
	return idxPos < len(constCols) && constCols[idxPos]
}
