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

package tables

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func fullTextTestTable(t *testing.T, parser model.FullTextParserType) (*TableCommon, *index) {
	bodyType := types.NewFieldType(mysql.TypeBlob)
	bodyType.SetCharset(charset.CharsetUTF8MB4)
	bodyType.SetCollate(charset.CollationUTF8MB4)
	tableInfo := &model.TableInfo{
		ID:    1,
		Name:  ast.NewCIStr("t"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("id"), Offset: 0, State: model.StatePublic, FieldType: *types.NewFieldType(mysql.TypeLong)},
			{ID: 2, Name: ast.NewCIStr("body"), Offset: 1, State: model.StatePublic, FieldType: *bodyType},
		},
		Indices: []*model.IndexInfo{{
			ID:      1,
			Name:    ast.NewCIStr("idx"),
			State:   model.StatePublic,
			Tp:      ast.IndexTypeFulltext,
			Columns: []*model.IndexColumn{{Name: ast.NewCIStr("body"), Offset: 1, Length: types.UnspecifiedLength}},
			TiKVFullText: &model.TiKVFullTextIndexInfo{
				ParserType:     parser,
				MinTokenSize:   3,
				MaxTokenSize:   84,
				EnableStopword: true,
				NgramTokenSize: 2,
			},
		}},
		PKIsHandle: true,
	}
	tbl, err := TableFromMeta(autoid.NewAllocators(false), tableInfo)
	require.NoError(t, err)
	tc := tbl.(*TableCommon)
	idx := asIndex(tc.indices[0])
	require.NotNil(t, idx.fullText)
	return tc, idx
}

// TestFullTextIndexKVGeneration covers the fan-out of one row into its term
// entries: keys hold the term and the handle, values hold the positions.
func TestFullTextIndexKVGeneration(t *testing.T) {
	_, idx := fullTextTestTable(t, model.FullTextParserTypeStandardV1)
	ec := errctx.StrictNoWarningContext
	handle := kv.IntHandle(7)

	iter := idx.GenIndexKVIter(ec, time.UTC, []types.Datum{types.NewStringDatum("Hello world, hello SQL")}, handle, nil)
	got := make(map[string][]int)
	for iter.Valid() {
		key, value, distinct, err := iter.Next(nil, nil)
		require.NoError(t, err)
		require.False(t, distinct)
		term, err := DecodeTiKVFullTextIndexKey(key)
		require.NoError(t, err)
		h, err := tablecodec.DecodeIndexHandle(key, value, 1)
		require.NoError(t, err)
		require.Equal(t, int64(7), h.IntValue())
		positions, err := tablecodec.DecodeTiKVFullTextIndexValue(value)
		require.NoError(t, err)
		got[string(term)] = positions
	}
	require.Equal(t, map[string][]int{"hello": {0, 2}, "world": {1}, "sql": {3}}, got)

	// Terms come out in key order, so writes are sorted.
	vals, err := idx.fullTextIndexedValues([]types.Datum{types.NewStringDatum("zebra apple mango")})
	require.NoError(t, err)
	require.Len(t, vals, 3)
	require.Equal(t, "apple", string(vals[0][0].GetBytes()))
	require.Equal(t, "mango", string(vals[1][0].GetBytes()))
	require.Equal(t, "zebra", string(vals[2][0].GetBytes()))

	// NULL, empty, and stopword-only documents have no entries.
	for _, doc := range []types.Datum{types.NewDatum(nil), types.NewStringDatum(""), types.NewStringDatum("the of")} {
		vals, err := idx.fullTextIndexedValues([]types.Datum{doc})
		require.NoError(t, err)
		require.Empty(t, vals)
		iter := idx.GenIndexKVIter(ec, time.UTC, []types.Datum{doc}, handle, nil)
		require.False(t, iter.Valid())
	}

	// The NGRAM analyzer positions grams by character.
	_, ngram := fullTextTestTable(t, model.FullTextParserTypeNgramV1)
	vals, err = ngram.fullTextIndexedValues([]types.Datum{types.NewStringDatum("abab")})
	require.NoError(t, err)
	require.Len(t, vals, 2)
	require.Equal(t, "ab", string(vals[0][0].GetBytes()))
	positions, err := tablecodec.DecodeTiKVFullTextIndexValue(tablecodec.EncodeTiKVFullTextIndexValue(nil, vals[0][1].GetBytes(), false))
	require.NoError(t, err)
	require.Equal(t, []int{0, 2}, positions)
}

// TestFullTextIndexMutationCheck covers the membership check the mutation
// checker applies to a FULLTEXT index entry: the term must be one the row's
// document analyzes to.
func TestFullTextIndexMutationCheck(t *testing.T) {
	tbl, idx := fullTextTestTable(t, model.FullTextParserTypeStandardV1)
	ec := errctx.StrictNoWarningContext
	row := []types.Datum{types.NewIntDatum(1), types.NewStringDatum("hello world")}
	keyFor := func(term string) []byte {
		key, _, err := idx.GenIndexKey(ec, time.UTC, []types.Datum{
			types.NewBytesDatum([]byte(term)),
			types.NewBytesDatum(tablecodec.EncodeTiKVFullTextPositions(nil, []int{0})),
		}, kv.IntHandle(1), nil)
		require.NoError(t, err)
		return key
	}
	require.NoError(t, checkFullTextIndexKey(tbl, idx.idxInfo, keyFor("hello"), row, nil))
	require.NoError(t, checkFullTextIndexKey(tbl, idx.idxInfo, keyFor("world"), row, nil))
	err := checkFullTextIndexKey(tbl, idx.idxInfo, keyFor("absent"), row, nil)
	require.ErrorContains(t, err, "inconsistent")
	// An entry for a row whose document is NULL cannot be right either.
	err = checkFullTextIndexKey(tbl, idx.idxInfo, keyFor("hello"), []types.Datum{types.NewIntDatum(1), types.NewDatum(nil)}, nil)
	require.ErrorContains(t, err, "inconsistent")
}
