// Copyright 2020 PingCAP, Inc.
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

package codec

import (
	"hash"
	"hash/crc32"
	"hash/fnv"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

func prepareCollationData() (int, *chunk.Chunk, *chunk.Chunk) {
	tp := types.NewFieldType(mysql.TypeString)
	tps := []*types.FieldType{tp}
	chk1 := chunk.New(tps, 3, 3)
	chk2 := chunk.New(tps, 3, 3)
	chk1.Reset()
	chk2.Reset()
	chk1.Column(0).AppendString("aaa")
	chk2.Column(0).AppendString("AAA")
	chk1.Column(0).AppendString("😜")
	chk2.Column(0).AppendString("😃")
	chk1.Column(0).AppendString("À")
	chk2.Column(0).AppendString("A")
	return 3, chk1, chk2
}

func TestHashGroupKeyCollation(t *testing.T) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	tp := types.NewFieldType(mysql.TypeString)
	n, chk1, chk2 := prepareCollationData()

	tp.SetCollate("utf8_general_ci")
	buf1 := make([][]byte, n)
	buf2 := make([][]byte, n)
	buf1, err := HashGroupKey(sc, n, chk1.Column(0), buf1, tp)
	require.NoError(t, err)

	buf2, err = HashGroupKey(sc, n, chk2.Column(0), buf2, tp)
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		require.Equal(t, len(buf2[i]), len(buf1[i]))
		for j := range buf1 {
			require.Equal(t, buf2[i][j], buf1[i][j])
		}
	}

	tp.SetCollate("utf8_unicode_ci")
	buf1 = make([][]byte, n)
	buf2 = make([][]byte, n)
	buf1, err = HashGroupKey(sc, n, chk1.Column(0), buf1, tp)
	require.NoError(t, err)
	buf2, err = HashGroupKey(sc, n, chk2.Column(0), buf2, tp)
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		require.Equal(t, len(buf2[i]), len(buf1[i]))
		for j := range buf1 {
			require.Equal(t, buf2[i][j], buf1[i][j])
		}
	}
}

func TestHashChunkRowCollation(t *testing.T) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	tp := types.NewFieldType(mysql.TypeString)
	tps := []*types.FieldType{tp}
	n, chk1, chk2 := prepareCollationData()
	cols := []int{0}
	buf := make([]byte, 1)

	tp.SetCollate("binary")
	for i := 0; i < n; i++ {
		h1 := crc32.NewIEEE()
		h2 := crc32.NewIEEE()
		require.NoError(t, HashChunkRow(sc, h1, chk1.GetRow(i), tps, cols, buf))
		require.NoError(t, HashChunkRow(sc, h2, chk2.GetRow(i), tps, cols, buf))
		require.NotEqual(t, h2.Sum32(), h1.Sum32())
		h1.Reset()
		h2.Reset()
	}

	tp.SetCollate("utf8_general_ci")
	for i := 0; i < n; i++ {
		h1 := crc32.NewIEEE()
		h2 := crc32.NewIEEE()
		require.NoError(t, HashChunkRow(sc, h1, chk1.GetRow(i), tps, cols, buf))
		require.NoError(t, HashChunkRow(sc, h2, chk2.GetRow(i), tps, cols, buf))
		require.Equal(t, h2.Sum32(), h1.Sum32())
		h1.Reset()
		h2.Reset()
	}

	tp.SetCollate("utf8_unicode_ci")
	for i := 0; i < n; i++ {
		h1 := crc32.NewIEEE()
		h2 := crc32.NewIEEE()
		require.NoError(t, HashChunkRow(sc, h1, chk1.GetRow(i), tps, cols, buf))
		require.NoError(t, HashChunkRow(sc, h2, chk2.GetRow(i), tps, cols, buf))
		require.Equal(t, h2.Sum32(), h1.Sum32())
		h1.Reset()
		h2.Reset()
	}
}

func TestHashChunkColumnsCollation(t *testing.T) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	tp := types.NewFieldType(mysql.TypeString)
	n, chk1, chk2 := prepareCollationData()
	buf := make([]byte, 1)
	hasNull := []bool{false, false, false}
	h1s := []hash.Hash64{fnv.New64(), fnv.New64(), fnv.New64()}
	h2s := []hash.Hash64{fnv.New64(), fnv.New64(), fnv.New64()}

	tp.SetCollate("binary")
	require.NoError(t, HashChunkColumns(sc, h1s, chk1, tp, 0, buf, hasNull))
	require.NoError(t, HashChunkColumns(sc, h2s, chk2, tp, 0, buf, hasNull))

	for i := 0; i < n; i++ {
		require.NotEqual(t, h2s[i].Sum64(), h1s[i].Sum64())
		h1s[i].Reset()
		h2s[i].Reset()
	}

	tp.SetCollate("utf8_general_ci")
	require.NoError(t, HashChunkColumns(sc, h1s, chk1, tp, 0, buf, hasNull))
	require.NoError(t, HashChunkColumns(sc, h2s, chk2, tp, 0, buf, hasNull))
	for i := 0; i < n; i++ {
		require.Equal(t, h2s[i].Sum64(), h1s[i].Sum64())
	}

	tp.SetCollate("utf8_unicode_ci")
	require.NoError(t, HashChunkColumns(sc, h1s, chk1, tp, 0, buf, hasNull))
	require.NoError(t, HashChunkColumns(sc, h2s, chk2, tp, 0, buf, hasNull))
	for i := 0; i < n; i++ {
		require.Equal(t, h2s[i].Sum64(), h1s[i].Sum64())
	}
}
