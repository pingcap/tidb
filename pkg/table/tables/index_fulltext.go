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
	"bytes"
	"slices"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// A FULLTEXT index built in TiKV holds one entry per distinct term of the
// tokenized column per row. The tokenized column is the last column of the
// index; the columns before it are key columns, whose values are encoded
// ahead of the term as they would be in an ordinary index, so that the
// entries of one value of them, such as one tenant, form a contiguous range.
// The index object tokenizes the column itself, with the analyzer frozen in
// its metadata, and fans the row out into one (key values..., term,
// positions) tuple per term. The term is encoded as binary bytes so that no
// collation is involved; the positions become the value. See
// tablecodec.EncodeTiKVFullTextIndexValue for the value layout.
//
// The tuples this file produces have the shape [key values..., term, encoded
// positions], the last two bytes datums, which is what GenIndexKey and
// GenIndexValue expect for this index kind.

// fullTextIndex is the per-index state of a FULLTEXT index built in TiKV.
type fullTextIndex struct {
	analyzer fulltext.Analyzer
}

func (c *index) initFullText() error {
	if c.idxInfo.TiKVFullText == nil {
		return nil
	}
	analyzer, err := fulltext.GetAnalyzer(fulltext.AnalyzerConfigFromTiKVFullTextIndex(c.idxInfo.TiKVFullText))
	if err != nil {
		return errors.Trace(err)
	}
	c.fullText = &fullTextIndex{analyzer: analyzer}
	return nil
}

// fullTextKeyColumnCount is the number of key columns encoded ahead of the
// term.
func (c *index) fullTextKeyColumnCount() int {
	return len(c.idxInfo.Columns) - 1
}

// fullTextIndexedValues tokenizes the document, the last of indexedValues,
// and returns one [key values..., term, encoded positions] tuple per distinct
// term, in term order. A NULL document, or one that analyzes to no term,
// yields no tuple: the row then has no entry in the index, and no query can
// find it through the index, which is what matching nothing means.
func (c *index) fullTextIndexedValues(indexedValues []types.Datum) ([][]types.Datum, error) {
	if len(indexedValues) != len(c.idxInfo.Columns) {
		return nil, errors.Errorf("fulltext index %s expects %d indexed columns, got %d", c.idxInfo.Name.O, len(c.idxInfo.Columns), len(indexedValues))
	}
	keyValues := indexedValues[:c.fullTextKeyColumnCount()]
	if err := c.castIndexValuesToChangingTypes(keyValues); err != nil {
		return nil, errors.Trace(err)
	}
	positions, err := c.fullTextPositions(indexedValues[len(indexedValues)-1])
	if err != nil || len(positions) == 0 {
		return nil, err
	}
	terms := make([]string, 0, len(positions))
	for term := range positions {
		terms = append(terms, term)
	}
	slices.Sort(terms)
	vals := make([][]types.Datum, 0, len(terms))
	for _, term := range terms {
		tuple := make([]types.Datum, 0, len(keyValues)+2)
		tuple = append(tuple, keyValues...)
		tuple = append(tuple,
			types.NewBytesDatum([]byte(term)),
			types.NewBytesDatum(tablecodec.EncodeTiKVFullTextPositions(nil, positions[term])))
		vals = append(vals, tuple)
	}
	return vals, nil
}

// fullTextPositions tokenizes a document and returns the positions of each
// distinct term in it; a NULL document has none.
func (c *index) fullTextPositions(doc types.Datum) (map[string][]int, error) {
	if doc.IsNull() {
		return nil, nil
	}
	tokens, err := c.fullText.analyzer.Analyze(doc.GetString())
	if err != nil {
		return nil, errors.Trace(err)
	}
	positions := make(map[string][]int, len(tokens))
	for _, token := range tokens {
		positions[token.Text] = append(positions[token.Text], token.Position)
	}
	return positions, nil
}

// fullTextTerms returns the distinct terms of the document, for consistency
// checks that need to know whether a key belongs to a row.
func (c *index) fullTextTerms(doc types.Datum) (map[string]struct{}, error) {
	positions, err := c.fullTextPositions(doc)
	if err != nil {
		return nil, err
	}
	terms := make(map[string]struct{}, len(positions))
	for term := range positions {
		terms[term] = struct{}{}
	}
	return terms, nil
}

// fullTextKeyColumnMismatch compares the encoded key-column values of an
// entry with the values the row would write them as, and returns the position
// of the first key column that differs, or -1 when they all agree. The
// comparison is on the encoded form, which is what the key holds: under a
// collation the key holds the sort key, and the entry carries no restored
// value to compare the original against.
func (c *index) fullTextKeyColumnMismatch(loc *time.Location, keyValues [][]byte, rowValues []types.Datum) (int, error) {
	if len(keyValues) != c.fullTextKeyColumnCount() || len(rowValues) != c.fullTextKeyColumnCount() {
		return 0, errors.Errorf("fulltext index %s has %d key columns, got %d encoded and %d row values",
			c.idxInfo.Name.O, c.fullTextKeyColumnCount(), len(keyValues), len(rowValues))
	}
	expected := slices.Clone(rowValues)
	if err := c.castIndexValuesToChangingTypes(expected); err != nil {
		return 0, errors.Trace(err)
	}
	for i, v := range expected {
		encoded, err := c.encoder.EncodeKey(loc, nil, v)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if !bytes.Equal(encoded, keyValues[i]) {
			return i, nil
		}
	}
	return -1, nil
}

// FullTextIndexTerms returns the distinct terms the document analyzes to under
// a FULLTEXT index built in TiKV, for consistency checks that need to know
// which entries a row should have.
func FullTextIndexTerms(idx table.Index, doc types.Datum) (map[string]struct{}, error) {
	c, err := asFullTextIndex(idx)
	if err != nil {
		return nil, err
	}
	return c.fullTextTerms(doc)
}

// FullTextIndexKeyColumnMismatch reports which key column of an entry of a
// FULLTEXT index built in TiKV, if any, holds a value other than the one the
// row's values encode to, as the position of the first such column or -1.
// keyValues are the encoded key-column values of the entry, as
// DecodeTiKVFullTextIndexKey returns them; rowValues are the row's values of
// the key columns in index order.
func FullTextIndexKeyColumnMismatch(idx table.Index, loc *time.Location, keyValues [][]byte, rowValues []types.Datum) (int, error) {
	c, err := asFullTextIndex(idx)
	if err != nil {
		return 0, err
	}
	return c.fullTextKeyColumnMismatch(loc, keyValues, rowValues)
}

func asFullTextIndex(idx table.Index) (*index, error) {
	c, ok := idx.(*index)
	if !ok || c.fullText == nil {
		return nil, errors.Errorf("index %s is not a fulltext index built in TiKV", idx.Meta().Name.O)
	}
	return c, nil
}

// genFullTextIndexKey encodes the key of one [key values..., term, positions]
// tuple: the key values and the term, followed by the handle.
func (c *index) genFullTextIndexKey(ec errctx.Context, loc *time.Location, indexedValues []types.Datum, h kv.Handle, buf []byte) ([]byte, bool, error) {
	if len(indexedValues) != len(c.idxInfo.Columns)+1 {
		return nil, false, errors.Errorf("fulltext index %s expects a [key values..., term, positions] tuple of %d values, got %d",
			c.idxInfo.Name.O, len(c.idxInfo.Columns)+1, len(indexedValues))
	}
	key, distinct, err := tablecodec.GenIndexKey(c.encoder, loc, c.tblInfo, c.idxInfo, c.phyTblID, indexedValues[:len(c.idxInfo.Columns)], h, buf)
	return key, distinct, ec.HandleError(err)
}

// genFullTextIndexValue encodes the value of one [key values..., term,
// positions] tuple: the positions.
func (c *index) genFullTextIndexValue(untouched bool, indexedValues []types.Datum, buf []byte) ([]byte, error) {
	if len(indexedValues) != len(c.idxInfo.Columns)+1 {
		return nil, errors.Errorf("fulltext index %s expects a [key values..., term, positions] tuple of %d values, got %d",
			c.idxInfo.Name.O, len(c.idxInfo.Columns)+1, len(indexedValues))
	}
	return tablecodec.EncodeTiKVFullTextIndexValue(buf, indexedValues[len(indexedValues)-1].GetBytes(), untouched), nil
}

// DecodeTiKVFullTextIndexKey splits the key of an entry of a FULLTEXT index
// built in TiKV into the encoded values of its key columns, one per column
// before the tokenized one, and the term the entry was written under.
func DecodeTiKVFullTextIndexKey(idxInfo *model.IndexInfo, key []byte) (keyValues [][]byte, term []byte, err error) {
	n := len(idxInfo.Columns)
	values, _, err := tablecodec.CutIndexKeyNew(key, n)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	_, termDatum, err := codec.DecodeOne(values[n-1])
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return values[:n-1], termDatum.GetBytes(), nil
}
