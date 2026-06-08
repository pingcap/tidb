// Copyright 2025 PingCAP, Inc.
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

package util

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// indexCol2Col finds the corresponding column of the IndexColumn in a column slice.
func indexCol2Col(colInfos []*model.ColumnInfo, cols []*expression.Column, col *model.IndexColumn) *expression.Column {
	for i, info := range colInfos {
		if info.Name.L == col.Name.L {
			if col.Length > 0 && info.FieldType.GetFlen() > col.Length {
				c := *cols[i]
				c.IsPrefix = true
				return &c
			}
			return cols[i]
		}
	}
	return nil
}

type indexInfo2ColsFlags uint

const (
	extractPrefixCols indexInfo2ColsFlags = 1 << iota
	extractFullCols
)

func indexInfo2ColsImpl(
	colInfos []*model.ColumnInfo,
	cols []*expression.Column,
	index *model.IndexInfo,
	flags indexInfo2ColsFlags,
) (
	prefixCols []*expression.Column,
	prefixLens []int,
	fullCols []*expression.Column,
	fullLens []int,
) {
	intest.Assert(flags != 0, "at least one of indexInfo2ColsFlags must be set")
	if flags&extractPrefixCols != 0 {
		prefixCols = make([]*expression.Column, 0, len(index.Columns))
		prefixLens = make([]int, 0, len(index.Columns))
	}
	if flags&extractFullCols != 0 {
		fullCols = make([]*expression.Column, 0, len(index.Columns))
		fullLens = make([]int, 0, len(index.Columns))
	}
	prefixComplete := false

	for _, c := range index.Columns {
		col := indexCol2Col(colInfos, cols, c)
		if col == nil {
			prefixComplete = true
			if flags&extractFullCols == 0 {
				return
			}
			fullCols = append(fullCols, col)
			fullLens = append(fullLens, types.UnspecifiedLength)
			continue
		}

		// We use `types.UnspecifiedLength` to specifically indicate that a column does not
		// have a prefix length (see the `hasPrefix` function in util/ranger, for example).
		// In other words, `length` is only used for indexed columns with a prefix length
		// (as long as it is less than the full length).
		length := c.Length
		if length != types.UnspecifiedLength && length == col.RetType.GetFlen() {
			length = types.UnspecifiedLength
		}
		if flags&extractPrefixCols != 0 && !prefixComplete {
			prefixCols = append(prefixCols, col)
			prefixLens = append(prefixLens, length)
		}
		if flags&extractFullCols != 0 {
			fullCols = append(fullCols, col)
			fullLens = append(fullLens, length)
		}
	}
	return
}

// IndexInfo2PrefixCols gets the corresponding []*expression.Column of the indexInfo's []*IndexColumn,
// together with a []int containing their lengths.
// If this index has three IndexColumn that the 1st and 3rd IndexColumn has corresponding *expression.Column,
// the return value will be only the 1st corresponding *expression.Column and its length.
// TODO: Use a struct to represent {*expression.Column, int}.
func IndexInfo2PrefixCols(colInfos []*model.ColumnInfo, cols []*expression.Column, index *model.IndexInfo) (
	prefixCols []*expression.Column, prefixLens []int,
) {
	prefixCols, prefixLens, _, _ = indexInfo2ColsImpl(colInfos, cols, index, extractPrefixCols)
	return
}

// IndexInfo2FullCols gets the corresponding []*expression.Column of the indexInfo's []*IndexColumn,
// together with a []int containing their lengths.
// If this index has three IndexColumn that the 1st and 3rd IndexColumn has corresponding *expression.Column,
// the return value will be [col1, nil, col2].
func IndexInfo2FullCols(colInfos []*model.ColumnInfo, cols []*expression.Column, index *model.IndexInfo) (
	fullCols []*expression.Column, fullLens []int,
) {
	_, _, fullCols, fullLens = indexInfo2ColsImpl(colInfos, cols, index, extractFullCols)
	return
}

// IndexInfo2Cols returns the combined result of IndexInfo2PrefixCols and IndexInfo2FullCols.
func IndexInfo2Cols(colInfos []*model.ColumnInfo, cols []*expression.Column, index *model.IndexInfo) (
	prefixCols []*expression.Column, prefixLens []int,
	fullCols []*expression.Column, fullLens []int,
) {
	return indexInfo2ColsImpl(colInfos, cols, index, extractPrefixCols|extractFullCols)
}
