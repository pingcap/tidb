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

package fulltext

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

// memIndex is an inverted index over in-memory documents, built with the same
// analyzer a query is compiled with, keyed by int handle.
type memIndex struct {
	analyzer Analyzer
	postings map[string][]Posting // term -> postings in handle order
	docs     map[int64]string
	opened   int
	closed   int
}

func newMemIndex(t *testing.T, config AnalyzerConfig) *memIndex {
	analyzer, err := GetAnalyzer(config)
	require.NoError(t, err)
	return &memIndex{analyzer: analyzer, postings: make(map[string][]Posting), docs: make(map[int64]string)}
}

func (m *memIndex) add(t *testing.T, handle int64, doc string) {
	m.docs[handle] = doc
	tokens, err := m.analyzer.Analyze(doc)
	require.NoError(t, err)
	positions := make(map[string][]int)
	for _, token := range tokens {
		positions[token.Text] = append(positions[token.Text], token.Position)
	}
	for term, pos := range positions {
		m.postings[term] = append(m.postings[term], Posting{Handle: kv.IntHandle(handle), Positions: pos})
		slices.SortFunc(m.postings[term], func(a, b Posting) int { return a.Handle.Compare(b.Handle) })
	}
}

type memCursor struct {
	index    *memIndex
	postings []Posting
	i        int
}

func (c *memCursor) Next() (Posting, bool, error) {
	if c.i >= len(c.postings) {
		return Posting{}, false, nil
	}
	p := c.postings[c.i]
	c.i++
	return p, true, nil
}

func (c *memCursor) Close() error {
	c.index.closed++
	return nil
}

func (m *memIndex) Term(term string) (PostingCursor, error) {
	m.opened++
	return &memCursor{index: m, postings: m.postings[term]}, nil
}

func (m *memIndex) Prefix(prefix string) (PostingCursor, error) {
	m.opened++
	terms := make([]string, 0)
	for term := range m.postings {
		if strings.HasPrefix(term, prefix) {
			terms = append(terms, term)
		}
	}
	slices.Sort(terms)
	var postings []Posting
	for _, term := range terms {
		postings = append(postings, m.postings[term]...)
	}
	return &memCursor{index: m, postings: postings}, nil
}

// scan is the reference answer: every document matched one by one.
func (m *memIndex) scan(t *testing.T, query *Query) []int64 {
	handles := make([]int64, 0)
	for handle, doc := range m.docs {
		document, err := BuildDocument([]ColumnInput{{Text: doc}}, m.analyzer)
		require.NoError(t, err)
		if query.Match(document) {
			handles = append(handles, handle)
		}
	}
	slices.Sort(handles)
	return handles
}

func drain(t *testing.T, iter PostingIterator) []int64 {
	handles := make([]int64, 0)
	var previous kv.Handle
	for {
		handle, ok, err := iter.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		if previous != nil {
			require.Less(t, previous.Compare(handle), 0, "handles must ascend without duplicates")
		}
		previous = handle
		handles = append(handles, handle.IntValue())
	}
	require.NoError(t, iter.Close())
	return handles
}

func standardConfig() AnalyzerConfig {
	return AnalyzerConfig{ParserType: model.FullTextParserTypeStandardV1, InnodbFtMinTokenSize: 3, InnodbFtMaxTokenSize: 84, InnodbFtEnableStopword: true}
}

func ngramConfig() AnalyzerConfig {
	return AnalyzerConfig{ParserType: model.FullTextParserTypeNgramV1, NgramTokenSize: 2}
}

// TestOpenPostingsBooleanForms covers each boolean form against a small
// corpus with known answers.
func TestOpenPostingsBooleanForms(t *testing.T) {
	index := newMemIndex(t, standardConfig())
	index.add(t, 1, "distributed sql database")
	index.add(t, 2, "sql database with distributed storage")
	index.add(t, 3, "relational storage engine")
	index.add(t, 4, "distributed database storage database")
	index.add(t, 5, "")
	index.add(t, 6, "sql sql sql")

	for _, tc := range []struct {
		search string
		want   []int64
	}{
		{"+distributed", []int64{1, 2, 4}},
		{"+distributed +storage", []int64{2, 4}},
		{"distributed storage", []int64{1, 2, 3, 4}},
		{"+distributed -storage", []int64{1}},
		{"+sql -distributed", []int64{6}},
		{"-distributed", nil},
		{`"distributed sql"`, []int64{1}},
		{`"sql distributed"`, nil},
		{`"distributed database"`, []int64{4}},
		{`"database storage database"`, []int64{4}},
		{`"sql sql"`, []int64{6}},
		{`+"sql database" +distributed`, []int64{1, 2}},
		{`dist*`, []int64{1, 2, 4}},
		{`+dist* +stor*`, []int64{2, 4}},
		{`+nosuchterm`, nil},
		{`+of`, nil},
	} {
		t.Run(tc.search, func(t *testing.T) {
			query, err := CompileBooleanQuery(tc.search, standardConfig())
			require.NoError(t, err)
			iter, err := query.OpenPostings(index)
			require.NoError(t, err)
			got := drain(t, iter)
			if tc.want == nil {
				require.Empty(t, got)
			} else {
				require.Equal(t, tc.want, got)
			}
			require.Equal(t, index.scan(t, query), got, "index and scan must agree")
		})
	}
	// Every cursor that was opened was closed.
	require.Equal(t, index.opened, index.closed)
}

// TestOpenPostingsNgramPhrases covers the case that motivates positions: an
// NGRAM query is a phrase over grams, and rows that merely contain the grams
// somewhere must not be returned.
func TestOpenPostingsNgramPhrases(t *testing.T) {
	index := newMemIndex(t, ngramConfig())
	index.add(t, 1, "数据库系统")
	index.add(t, 2, "系统数据")
	index.add(t, 3, "库数据系统")
	index.add(t, 4, "abcabc")
	index.add(t, 5, "cbacba")
	index.add(t, 6, strings.Repeat("ab", 500)+"cd")

	for _, tc := range []struct {
		search string
		want   []int64
	}{
		{"数据库", []int64{1}},
		{"数据", []int64{1, 2, 3}},
		{"库数据", []int64{3}},
		{"abc", []int64{4, 6}},
		{"cba", []int64{5}},
		{"bcd", []int64{6}},
		{"+ab +cd", []int64{6}},
		{"ab cd", []int64{4, 6}},
		{"+ab -cd", []int64{4}},
	} {
		t.Run(tc.search, func(t *testing.T) {
			query, err := CompileBooleanQuery(tc.search, ngramConfig())
			require.NoError(t, err)
			iter, err := query.OpenPostings(index)
			require.NoError(t, err)
			got := drain(t, iter)
			require.Equal(t, tc.want, got)
			require.Equal(t, index.scan(t, query), got)
		})
	}
}

// TestOpenPostingsAgreesWithScan is the equivalence check: over random
// corpora and random boolean queries, the index path returns exactly the rows
// the per-document matcher accepts.
func TestOpenPostingsAgreesWithScan(t *testing.T) {
	words := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa", "of", "the"}
	rng := rand.New(rand.NewSource(20260922))
	compiled := 0
	for _, config := range []AnalyzerConfig{standardConfig(), ngramConfig()} {
		for round := range 40 {
			index := newMemIndex(t, config)
			docCount := 1 + rng.Intn(40)
			for h := range docCount {
				n := rng.Intn(12)
				parts := make([]string, n)
				for i := range parts {
					parts[i] = words[rng.Intn(len(words))]
				}
				index.add(t, int64(h), strings.Join(parts, " "))
			}
			for q := range 25 {
				search := randomBooleanQuery(rng, words)
				query, err := CompileBooleanQuery(search, config)
				if err != nil {
					continue
				}
				compiled++
				iter, err := query.OpenPostings(index)
				require.NoError(t, err)
				got := drain(t, iter)
				require.Equal(t, index.scan(t, query), got, "config %v round %d query %d: %q", config.ParserType, round, q, search)
			}
			require.Equal(t, index.opened, index.closed)
		}
	}
	// The generator must produce mostly valid queries, or the check is vacuous.
	require.Greater(t, compiled, 1500)
}

// randomBooleanQuery builds a query from terms, prefixes and phrases under
// the three modifiers. Parenthesised groups are not part of the boolean
// syntax this branch accepts.
func randomBooleanQuery(rng *rand.Rand, words []string) string {
	n := 1 + rng.Intn(3)
	clauses := make([]string, n)
	for i := range clauses {
		var expr string
		switch r := rng.Intn(10); {
		case r < 6:
			expr = words[rng.Intn(len(words))]
		case r < 8:
			w := words[rng.Intn(len(words))]
			expr = w[:1+rng.Intn(len(w))] + "*"
		default:
			m := 2 + rng.Intn(2)
			parts := make([]string, m)
			for j := range parts {
				parts[j] = words[rng.Intn(len(words))]
			}
			expr = `"` + strings.Join(parts, " ") + `"`
		}
		switch rng.Intn(4) {
		case 0:
			expr = "+" + expr
		case 1:
			expr = "-" + expr
		}
		clauses[i] = expr
	}
	return strings.Join(clauses, " ")
}

type failingSource struct {
	memIndex
	failTerm string
}

type failingCursor struct{}

func (failingCursor) Next() (Posting, bool, error) { return Posting{}, false, errors.New("boom") }
func (failingCursor) Close() error                 { return nil }

func (f *failingSource) Term(term string) (PostingCursor, error) {
	if term == f.failTerm {
		return failingCursor{}, nil
	}
	return f.memIndex.Term(term)
}

// TestOpenPostingsPropagatesErrors covers a cursor failure surfacing through
// every kind of stream rather than being read as the end of the list.
func TestOpenPostingsPropagatesErrors(t *testing.T) {
	index := newMemIndex(t, standardConfig())
	index.add(t, 1, "alpha beta gamma")
	index.add(t, 2, "alpha gamma")
	for _, search := range []string{"+alpha", "+alpha +beta", "alpha beta", "+beta -alpha", `"alpha beta"`, `"gamma alpha"`} {
		for _, failTerm := range []string{"alpha", "beta"} {
			src := &failingSource{memIndex: *index, failTerm: failTerm}
			query, err := CompileBooleanQuery(search, standardConfig())
			require.NoError(t, err)
			iter, err := query.OpenPostings(src)
			require.NoError(t, err)
			var lastErr error
			for {
				_, ok, err := iter.Next()
				if err != nil {
					lastErr = err
					break
				}
				if !ok {
					break
				}
			}
			if strings.Contains(search, failTerm) {
				require.ErrorContains(t, lastErr, "boom", fmt.Sprintf("%s failing %s", search, failTerm))
			} else {
				require.NoError(t, lastErr, fmt.Sprintf("%s failing %s", search, failTerm))
			}
			require.NoError(t, iter.Close())
		}
	}
}

// TestOpenPostingsMatchesNothing covers a query normalization proved empty:
// no cursor is opened at all.
func TestOpenPostingsMatchesNothing(t *testing.T) {
	index := newMemIndex(t, standardConfig())
	index.add(t, 1, "alpha")
	query, err := CompileBooleanQuery("+of", standardConfig())
	require.NoError(t, err)
	require.True(t, query.MatchesNothing())
	iter, err := query.OpenPostings(index)
	require.NoError(t, err)
	require.Empty(t, drain(t, iter))
	require.Zero(t, index.opened)
}
