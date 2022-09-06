// Copyright 2022 PingCAP, Inc.
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

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestCompareString(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	require.Equal(t, 0, types.CompareString("a", "A", "utf8_general_ci"))
	require.Equal(t, 0, types.CompareString("À", "A", "utf8_general_ci"))
	require.Equal(t, 0, types.CompareString("😜", "😃", "utf8_general_ci"))
	require.Equal(t, 0, types.CompareString("a ", "a  ", "utf8_general_ci"))
	require.Equal(t, 0, types.CompareString("ß", "s", "utf8_general_ci"))
	require.NotEqual(t, 0, types.CompareString("ß", "ss", "utf8_general_ci"))

	require.Equal(t, 0, types.CompareString("a", "A", "utf8_unicode_ci"))
	require.Equal(t, 0, types.CompareString("À", "A", "utf8_unicode_ci"))
	require.Equal(t, 0, types.CompareString("😜", "😃", "utf8_unicode_ci"))
	require.Equal(t, 0, types.CompareString("a ", "a  ", "utf8_unicode_ci"))
	require.NotEqual(t, 0, types.CompareString("ß", "s", "utf8_unicode_ci"))
	require.Equal(t, 0, types.CompareString("ß", "ss", "utf8_unicode_ci"))

	require.NotEqual(t, 0, types.CompareString("a", "A", "binary"))
	require.NotEqual(t, 0, types.CompareString("À", "A", "binary"))
	require.NotEqual(t, 0, types.CompareString("😜", "😃", "binary"))
	require.NotEqual(t, 0, types.CompareString("a ", "a  ", "binary"))

	ctx := mock.NewContext()
	ft := types.NewFieldType(mysql.TypeVarString)
	col1 := &Column{
		RetType: ft,
		Index:   0,
	}
	col2 := &Column{
		RetType: ft,
		Index:   1,
	}
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 4)
	chk.Column(0).AppendString("a")
	chk.Column(1).AppendString("A")
	chk.Column(0).AppendString("À")
	chk.Column(1).AppendString("A")
	chk.Column(0).AppendString("😜")
	chk.Column(1).AppendString("😃")
	chk.Column(0).AppendString("a ")
	chk.Column(1).AppendString("a  ")
	for i := 0; i < 4; i++ {
		v, isNull, err := CompareStringWithCollationInfo(ctx, col1, col2, chk.GetRow(0), chk.GetRow(0), "utf8_general_ci")
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, int64(0), v)
	}
}
