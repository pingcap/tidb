// Copyright 2021 PingCAP, Inc.
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

//go:build !codes

package testutil

import (
	"sort"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
)

// MustNewCommonHandle create a common handle with given values.
func MustNewCommonHandle(t *testing.T, values ...interface{}) kv.Handle {
	encoded, err := codec.EncodeKey(new(stmtctx.StatementContext), nil, types.MakeDatums(values...)...)
	require.NoError(t, err)
	ch, err := kv.NewCommonHandle(encoded)
	require.NoError(t, err)
	return ch
}

// MaskSortHandles sorts the handles by lowest (fieldTypeBits - 1 - shardBitsCount) bits.
func MaskSortHandles(handles []int64, shardBitsCount int, fieldType byte) []int64 {
	typeBitsLength := mysql.DefaultLengthOfMysqlTypes[fieldType] * 8
	const signBitCount = 1
	shiftBitsCount := 64 - typeBitsLength + shardBitsCount + signBitCount
	ordered := make([]int64, len(handles))
	for i, h := range handles {
		ordered[i] = h << shiftBitsCount >> shiftBitsCount
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return ordered
}
