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
	"slices"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// A FULLTEXT index built in TiKV holds one entry per distinct term of the
// indexed column per row. The index object tokenizes the column itself, with
// the analyzer frozen in its metadata, and fans the row out into one
// (term, positions) tuple per term. The term becomes the key, encoded as
// binary bytes so that no collation is involved; the positions become the
// value. See tablecodec.EncodeTiKVFullTextIndexValue for the value layout.
//
// The tuples this file produces have the shape [term, encoded positions],
// both bytes datums, which is what GenIndexKey and GenIndexValue expect for
// this index kind.

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

// fullTextIndexedValues tokenizes the document in indexedValues and returns one
// [term, encoded positions] tuple per distinct term, in term order. A NULL
// document, or one that analyzes to no term, yields no tuple: the row then has
// no entry in the index, and no query can find it through the index, which is
// what matching nothing means.
func (c *index) fullTextIndexedValues(indexedValues []types.Datum) ([][]types.Datum, error) {
	if len(indexedValues) != 1 {
		return nil, errors.Errorf("fulltext index %s expects one indexed column, got %d", c.idxInfo.Name.O, len(indexedValues))
	}
	doc := indexedValues[0]
	if doc.IsNull() {
		return nil, nil
	}
	tokens, err := c.fullText.analyzer.Analyze(doc.GetString())
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(tokens) == 0 {
		return nil, nil
	}
	positions := make(map[string][]int, len(tokens))
	for _, token := range tokens {
		positions[token.Text] = append(positions[token.Text], token.Position)
	}
	terms := make([]string, 0, len(positions))
	for term := range positions {
		terms = append(terms, term)
	}
	slices.Sort(terms)
	vals := make([][]types.Datum, 0, len(terms))
	for _, term := range terms {
		vals = append(vals, []types.Datum{
			types.NewBytesDatum([]byte(term)),
			types.NewBytesDatum(tablecodec.EncodeTiKVFullTextPositions(nil, positions[term])),
		})
	}
	return vals, nil
}

// fullTextTerms returns the distinct terms of the document, for consistency
// checks that need to know whether a key belongs to a row.
func (c *index) fullTextTerms(doc types.Datum) (map[string]struct{}, error) {
	vals, err := c.fullTextIndexedValues([]types.Datum{doc})
	if err != nil {
		return nil, err
	}
	terms := make(map[string]struct{}, len(vals))
	for _, val := range vals {
		terms[string(val[0].GetBytes())] = struct{}{}
	}
	return terms, nil
}

// genFullTextIndexKey encodes the key of one [term, positions] tuple.
func (c *index) genFullTextIndexKey(ec errctx.Context, loc *time.Location, indexedValues []types.Datum, h kv.Handle, buf []byte) ([]byte, bool, error) {
	if len(indexedValues) != 2 {
		return nil, false, errors.Errorf("fulltext index %s expects a [term, positions] tuple, got %d values", c.idxInfo.Name.O, len(indexedValues))
	}
	key, distinct, err := tablecodec.GenIndexKey(c.encoder, loc, c.tblInfo, c.idxInfo, c.phyTblID, indexedValues[:1], h, buf)
	return key, distinct, ec.HandleError(err)
}

// genFullTextIndexValue encodes the value of one [term, positions] tuple.
func (c *index) genFullTextIndexValue(untouched bool, indexedValues []types.Datum, buf []byte) ([]byte, error) {
	if len(indexedValues) != 2 {
		return nil, errors.Errorf("fulltext index %s expects a [term, positions] tuple, got %d values", c.idxInfo.Name.O, len(indexedValues))
	}
	return tablecodec.EncodeTiKVFullTextIndexValue(buf, indexedValues[1].GetBytes(), untouched), nil
}

// DecodeTiKVFullTextIndexKey returns the term an entry of a FULLTEXT index
// built in TiKV was written under.
func DecodeTiKVFullTextIndexKey(key []byte) ([]byte, error) {
	values, _, err := tablecodec.CutIndexKeyNew(key, 1)
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, term, err := codec.DecodeOne(values[0])
	if err != nil {
		return nil, errors.Trace(err)
	}
	return term.GetBytes(), nil
}
