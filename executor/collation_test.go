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

package executor

import (
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestVecGroupChecker(t *testing.T) {
	tp := types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).BuildP()
	col0 := &expression.Column{
		RetType: tp,
		Index:   0,
	}
	ctx := mock.NewContext()
	groupChecker := newVecGroupChecker(ctx, []expression.Expression{col0})

	chk := chunk.New([]*types.FieldType{tp}, 6, 6)
	chk.Reset()
	chk.Column(0).AppendString("aaa")
	chk.Column(0).AppendString("AAA")
	chk.Column(0).AppendString("😜")
	chk.Column(0).AppendString("😃")
	chk.Column(0).AppendString("À")
	chk.Column(0).AppendString("A")

	tp.SetCollate("bin")
	groupChecker.reset()
	_, err := groupChecker.splitIntoGroups(chk)
	require.NoError(t, err)
	for i := 0; i < 6; i++ {
		b, e := groupChecker.getNextGroup()
		require.Equal(t, b, i)
		require.Equal(t, e, i+1)
	}
	require.True(t, groupChecker.isExhausted())

	tp.SetCollate("utf8_general_ci")
	groupChecker.reset()
	_, err = groupChecker.splitIntoGroups(chk)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		b, e := groupChecker.getNextGroup()
		require.Equal(t, b, i*2)
		require.Equal(t, e, i*2+2)
	}
	require.True(t, groupChecker.isExhausted())

	tp.SetCollate("utf8_unicode_ci")
	groupChecker.reset()
	_, err = groupChecker.splitIntoGroups(chk)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		b, e := groupChecker.getNextGroup()
		require.Equal(t, b, i*2)
		require.Equal(t, e, i*2+2)
	}
	require.True(t, groupChecker.isExhausted())

	// test padding
	tp.SetCollate("utf8_bin")
	tp.SetFlen(6)
	chk.Reset()
	chk.Column(0).AppendString("a")
	chk.Column(0).AppendString("a  ")
	chk.Column(0).AppendString("a    ")
	groupChecker.reset()
	_, err = groupChecker.splitIntoGroups(chk)
	require.NoError(t, err)
	b, e := groupChecker.getNextGroup()
	require.Equal(t, b, 0)
	require.Equal(t, e, 3)
	require.True(t, groupChecker.isExhausted())
}
