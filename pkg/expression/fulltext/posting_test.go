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
	"math"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
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
	// seekable makes the cursors PostingSeekers; read counts the postings
	// the cursors hand out or step over, a seek counting as one.
	seekable bool
	read     int
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
	c.index.read++
	return p, true, nil
}

// seekingMemCursor is a memCursor that jumps to a handle at the cost of one
// read.
type seekingMemCursor struct {
	*memCursor
}

func (c seekingMemCursor) Seek(target kv.Handle) (Posting, bool, error) {
	c.i += sort.Search(len(c.postings)-c.i, func(j int) bool { return c.postings[c.i+j].Handle.Compare(target) >= 0 })
	return c.Next()
}

func (c *memCursor) Close() error {
	c.index.closed++
	return nil
}

func (m *memIndex) Term(term string) (PostingCursor, error) {
	m.opened++
	cursor := &memCursor{index: m, postings: m.postings[term]}
	if m.seekable {
		return seekingMemCursor{cursor}, nil
	}
	return cursor, nil
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
			index.seekable = round%2 == 1
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

// recordingSource records the posting lists a query opens.
type recordingSource struct {
	memIndex
	reads []PostingRead
}

func (r *recordingSource) Term(term string) (PostingCursor, error) {
	r.reads = append(r.reads, PostingRead{Term: term})
	return r.memIndex.Term(term)
}

func (r *recordingSource) Prefix(prefix string) (PostingCursor, error) {
	r.reads = append(r.reads, PostingRead{Term: prefix, Prefix: true})
	return r.memIndex.Prefix(prefix)
}

// TestPostingReadsMatchOpenPostings checks that PostingReads, from which the
// planner costs a search, names exactly the posting lists OpenPostings opens.
func TestPostingReadsMatchOpenPostings(t *testing.T) {
	words := []string{"alpha", "beta", "gamma", "delta", "of", "the"}
	rng := rand.New(rand.NewSource(20260930))
	sortReads := func(reads []PostingRead) []PostingRead {
		reads = slices.Clone(reads)
		slices.SortFunc(reads, func(a, b PostingRead) int {
			if c := strings.Compare(a.Term, b.Term); c != 0 {
				return c
			}
			return compareBool(a.Prefix, b.Prefix)
		})
		return reads
	}
	checked := 0
	for _, config := range []AnalyzerConfig{standardConfig(), ngramConfig()} {
		index := newMemIndex(t, config)
		index.add(t, 1, "alpha beta gamma delta")
		for range 300 {
			search := randomBooleanQuery(rng, words)
			query, err := CompileBooleanQuery(search, config)
			if err != nil {
				continue
			}
			src := &recordingSource{memIndex: *index}
			iter, err := query.OpenPostings(src)
			require.NoError(t, err)
			require.NoError(t, iter.Close())
			require.Equal(t, sortReads(src.reads), sortReads(query.PostingReads()), "%v %q", config.ParserType, search)
			checked++
		}
	}
	require.Greater(t, checked, 400)

	// A group with required clauses does not read its optional ones, and a
	// phrase reads a repeated term once.
	query, err := CompileBooleanQuery(`+alpha beta -gamma`, standardConfig())
	require.NoError(t, err)
	require.Equal(t, []PostingRead{{Term: "alpha"}, {Term: "gamma"}}, query.PostingReads())
	query, err = CompileBooleanQuery(`abab*`, ngramConfig())
	require.NoError(t, err)
	require.Equal(t, []PostingRead{{Term: "ab"}, {Term: "ba"}}, query.PostingReads())
	query, err = CompileBooleanQuery(`a*`, ngramConfig())
	require.NoError(t, err)
	require.Equal(t, []PostingRead{{Term: "a", Prefix: true}}, query.PostingReads())
}

func compareBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return 1
	default:
		return -1
	}
}

// TestOpenPostingsSeeksPastCommonTerms covers the intersections letting the
// rarest list drive: a common term's list, and the list a prohibited term
// subtracts, are only read around the rows the rare term proposes.
func TestOpenPostingsSeeksPastCommonTerms(t *testing.T) {
	index := newMemIndex(t, standardConfig())
	const rows = 5000
	for h := range rows {
		doc := "common words here"
		if h%1000 == 7 {
			doc = "rare common words here"
		}
		index.add(t, int64(h), doc)
	}
	cases := []struct {
		search string
		want   []int64
	}{
		{"+rare +common", []int64{7, 1007, 2007, 3007, 4007}},
		{"+common +rare", []int64{7, 1007, 2007, 3007, 4007}},
		{`+"rare common"`, []int64{7, 1007, 2007, 3007, 4007}},
		{"+rare -common", []int64{}},
		{"+rare -words +here", []int64{}},
	}
	for _, seekable := range []bool{false, true} {
		for _, c := range cases {
			index.seekable, index.read = seekable, 0
			query, err := CompileBooleanQuery(c.search, standardConfig())
			require.NoError(t, err)
			iter, err := query.OpenPostings(index)
			require.NoError(t, err)
			require.Equal(t, c.want, drain(t, iter), c.search)
			if seekable {
				require.Less(t, index.read, 50, "%q read %d postings", c.search, index.read)
			} else {
				require.Greater(t, index.read, rows/2, "%q read %d postings", c.search, index.read)
			}
		}
	}
}

// memoryRecordingSource records what a search charges for the postings it
// holds in memory.
type memoryRecordingSource struct {
	*memIndex
	charged, peak int64
}

func (m *memoryRecordingSource) ConsumeMemory(bytes int64) {
	m.charged += bytes
	m.peak = max(m.peak, m.charged)
}

// TestOpenPostingsChargesPrefixMemory covers a prefix charging the rows it
// collects while it is open, and releasing them when it is closed, also when
// the search ends early or fails.
func TestOpenPostingsChargesPrefixMemory(t *testing.T) {
	index := newMemIndex(t, standardConfig())
	for h := range 1000 {
		index.add(t, int64(h), fmt.Sprintf("prefix%d shared", h%37))
	}
	src := &memoryRecordingSource{memIndex: index}
	query, err := CompileBooleanQuery("+prefix* +shared", standardConfig())
	require.NoError(t, err)
	iter, err := query.OpenPostings(src)
	require.NoError(t, err)
	require.GreaterOrEqual(t, src.charged, int64(1000*8))
	_, ok, err := iter.Next()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, iter.Close())
	require.Zero(t, src.charged)
	require.GreaterOrEqual(t, src.peak, int64(1000*8))

	// A cursor failing part way through releases what was collected.
	failing := &memoryRecordingSource{memIndex: index}
	failingQuery, err := CompileBooleanQuery("+prefix*", standardConfig())
	require.NoError(t, err)
	_, err = failingQuery.OpenPostings(&failingPrefixSource{memoryRecordingSource: failing, after: 500})
	require.Error(t, err)
	require.Zero(t, failing.charged)
	require.Positive(t, failing.peak)
}

type failingPrefixSource struct {
	*memoryRecordingSource
	after int
}

func (f *failingPrefixSource) Prefix(prefix string) (PostingCursor, error) {
	cursor, err := f.memIndex.Prefix(prefix)
	if err != nil {
		return nil, err
	}
	return &failAfterCursor{PostingCursor: cursor, after: f.after}, nil
}

type failAfterCursor struct {
	PostingCursor
	after int
}

func (c *failAfterCursor) Next() (Posting, bool, error) {
	if c.after == 0 {
		return Posting{}, false, errors.New("posting read failed")
	}
	c.after--
	return c.PostingCursor.Next()
}

// quotaSource panics when a search holds more than its quota, as the memory
// tracker of a statement does.
type quotaSource struct {
	*memIndex
	charged, quota int64
}

func (q *quotaSource) ConsumeMemory(bytes int64) {
	q.charged += bytes
	if bytes > 0 && q.charged > q.quota {
		panic("memory quota exceeded")
	}
}

// TestOpenPostingsReleasesOnMemoryPanic covers the memory quota stopping a
// prefix by a panic: the cursors opened so far are closed and the memory
// charged is released, wherever in the query the prefix is.
func TestOpenPostingsReleasesOnMemoryPanic(t *testing.T) {
	for _, search := range []string{
		"+shared +prefix*",
		"shared prefix*",
		"+shared -prefix*",
		"+shared +other +prefix*",
	} {
		index := newMemIndex(t, standardConfig())
		for h := range 1000 {
			index.add(t, int64(h), fmt.Sprintf("prefix%d shared other", h%37))
		}
		src := &quotaSource{memIndex: index, quota: 1024}
		query, err := CompileBooleanQuery(search, standardConfig())
		require.NoError(t, err)
		require.PanicsWithValue(t, "memory quota exceeded", func() {
			_, _ = query.OpenPostings(src)
		}, search)
		require.Zero(t, src.charged, search)
		require.Greater(t, index.opened, 1, search)
		require.Equal(t, index.opened, index.closed, search)
	}
}

// commonHandleSource hands out the postings of a memIndex under common
// handles, as the index of a table with a clustered primary key does.
type commonHandleSource struct {
	*memIndex
	charged int64
}

func (c *commonHandleSource) ConsumeMemory(bytes int64) { c.charged += bytes }

func commonHandleOf(t *testing.T, handle int64) kv.Handle {
	encoded, err := codec.EncodeKey(time.UTC, nil, types.NewIntDatum(handle))
	require.NoError(t, err)
	common, err := kv.NewCommonHandle(encoded)
	require.NoError(t, err)
	return common
}

type commonHandleCursor struct {
	PostingCursor
	t *testing.T
}

func (c commonHandleCursor) Next() (Posting, bool, error) {
	posting, ok, err := c.PostingCursor.Next()
	if err != nil || !ok {
		return posting, ok, err
	}
	posting.Handle = commonHandleOf(c.t, posting.Handle.IntValue())
	return posting, true, nil
}

func (c *commonHandleSource) wrap(t *testing.T, cursor PostingCursor, err error) (PostingCursor, error) {
	if err != nil {
		return nil, err
	}
	return commonHandleCursor{PostingCursor: cursor, t: t}, nil
}

// TestOpenPostingsCommonHandles covers a table with a clustered primary key:
// a prefix collects, orders and deduplicates common handles, is sought in by
// the streams it is intersected with, and charges and releases their memory.
func TestOpenPostingsCommonHandles(t *testing.T) {
	index := newMemIndex(t, standardConfig())
	// Handles are added out of order and negative ones included, so the
	// order of the encoded handles is what is exercised.
	for h := range 400 {
		handle := int64((h*7919)%400 - 200)
		index.add(t, handle, fmt.Sprintf("prefix%d prefixed%d shared%d", h%13, h%5, h%3))
	}
	for _, search := range []string{
		"prefix*",
		"+prefix* +shared1",
		"+shared1 +prefix1*",
		"+shared2 -prefixed3*",
		"prefix1* shared0",
		"+prefix* +prefixed*",
	} {
		query, err := CompileBooleanQuery(search, standardConfig())
		require.NoError(t, err)
		src := &commonHandleSource{memIndex: index}
		iter, err := query.OpenPostings(commonHandleTestSource{commonHandleSource: src, t: t})
		require.NoError(t, err, search)
		require.Positive(t, src.charged, search)
		got := make([]int64, 0)
		var previous kv.Handle
		for {
			handle, ok, err := iter.Next()
			require.NoError(t, err, search)
			if !ok {
				break
			}
			require.False(t, handle.IsInt(), search)
			if previous != nil {
				require.Less(t, previous.Compare(handle), 0, "handles must ascend without duplicates: %s", search)
			}
			previous = handle
			_, datum, err := codec.DecodeOne(handle.Encoded())
			require.NoError(t, err)
			got = append(got, datum.GetInt64())
		}
		require.NoError(t, iter.Close())
		require.Zero(t, src.charged, search)
		slices.Sort(got)
		require.Equal(t, index.scan(t, query), got, search)
	}
}

// commonHandleTestSource is commonHandleSource bound to a test.
type commonHandleTestSource struct {
	*commonHandleSource
	t *testing.T
}

func (c commonHandleTestSource) Term(term string) (PostingCursor, error) {
	cursor, err := c.memIndex.Term(term)
	return c.wrap(c.t, cursor, err)
}

func (c commonHandleTestSource) Prefix(prefix string) (PostingCursor, error) {
	cursor, err := c.memIndex.Prefix(prefix)
	return c.wrap(c.t, cursor, err)
}

// TestEstimatePostingCost covers the cost of a search following its streams:
// an intersection reads its smallest list in full and only seeks in the
// others, a prohibited list is sought to the rows the positive side yields,
// and a union, a prefix, or a list too dense to skip in is read in full.
func TestEstimatePostingCost(t *testing.T) {
	const rows = 1_000_000
	postings := map[string]float64{"rare": 10, "medium": 20_000, "common": 900_000, "dense": 500_000, "com": 950_000}
	model := PostingCostModel{
		Postings: func(read PostingRead) float64 {
			n, ok := postings[read.Term]
			require.True(t, ok, read.Term)
			return n
		},
		Rows:      rows,
		TermScans: 1,
	}
	seekRead := float64(PostingScanBatch)
	cases := []struct {
		search  string
		entries float64
		scans   float64
	}{
		{"+common", 900_000, 1},
		{"+rare +common", 10 + 10*seekRead, 2 + 10},
		{"+common +rare", 10 + 10*seekRead, 2 + 10},
		{`+"rare common"`, 10 + 10*seekRead, 2 + 10},
		{"+rare +medium +common", 10 + 2*10*seekRead, 3 + 2*10},
		{"+rare -common", 10 + 10*seekRead, 2 + 10},
		// Too dense to skip in: less than a batch between targets is read through.
		{"+dense +common", 500_000 + 900_000, 2},
		{"+medium +common", 20_000 + 900_000, 2},
		// A union reads every list in full, and so does a prefix.
		{"rare common", 900_010, 2},
		{"+rare +com*", 10 + 950_000, 2},
		{"+nothing", 0, 1},
	}
	postings["nothing"] = 0
	for _, c := range cases {
		query, err := CompileBooleanQuery(c.search, standardConfig())
		require.NoError(t, err)
		cost := query.EstimatePostingCost(model)
		require.InDelta(t, c.entries, cost.Entries, 1e-6, c.search)
		require.InDelta(t, c.scans, cost.Scans, 1e-6, c.search)
	}
	// Handle ranges multiply the scans of every exact term.
	model.TermScans = 3
	query, err := CompileBooleanQuery("+rare +common", standardConfig())
	require.NoError(t, err)
	require.InDelta(t, 6+10, query.EstimatePostingCost(model).Scans, 1e-6)
	// A query proved empty, here a required stopword, reads nothing.
	query, err = CompileBooleanQuery("+of", standardConfig())
	require.NoError(t, err)
	require.True(t, query.MatchesNothing())
	require.Zero(t, query.EstimatePostingCost(model))
}

// TestEstimatePostingCostBoundedByFullReads checks over random queries that
// the estimate never reads more entries than reading every list the search
// opens in full would.
func TestEstimatePostingCostBoundedByFullReads(t *testing.T) {
	words := []string{"alpha", "beta", "gamma", "delta", "epsilon", "of", "the"}
	rng := rand.New(rand.NewSource(20261105))
	sizes := make(map[PostingRead]float64)
	model := PostingCostModel{
		Postings: func(read PostingRead) float64 {
			if _, ok := sizes[read]; !ok {
				sizes[read] = float64(rng.Intn(1_000_000))
			}
			return sizes[read]
		},
		Rows:      1_000_000,
		TermScans: 1,
	}
	checked := 0
	for range 2000 {
		search := randomBooleanQuery(rng, words)
		query, err := CompileBooleanQuery(search, standardConfig())
		if err != nil {
			continue
		}
		checked++
		cost := query.EstimatePostingCost(model)
		full := 0.0
		for _, read := range query.PostingReads() {
			full += model.Postings(read)
		}
		require.LessOrEqual(t, cost.Entries, full+1e-6, search)
		require.GreaterOrEqual(t, cost.Entries, 0.0, search)
	}
	require.Greater(t, checked, 1000)
}

// TestEstimatePostingRows checks the rows a search is estimated to match: a
// phrase is bounded by its rarest token, whose fellow tokens keep it company,
// and required terms are combined with exponential backoff, not independence.
func TestEstimatePostingRows(t *testing.T) {
	const rows = 10_000
	postings := map[string]float64{"my": 5, "ys": 5, "sq": 6, "ql": 7, "alpha": 1000, "beta": 100, "gamma": 4000}
	model := PostingCostModel{
		Postings:  func(read PostingRead) float64 { return postings[read.Term] },
		Rows:      rows,
		TermScans: 1,
	}
	estimate := func(search string, config AnalyzerConfig) float64 {
		query, err := CompileBooleanQuery(search, config)
		require.NoError(t, err)
		return query.EstimatePostingCost(model).Rows
	}
	ngram := ngramConfig()
	require.InDelta(t, 5, estimate("mysql", ngram), 1e-9)
	require.InDelta(t, 5, estimate(`+"mysql"`, ngram), 1e-9)
	// beta 1%, then alpha 10% and gamma 40% with weights 1/2 and 1/4.
	require.InDelta(t, rows*0.01*math.Sqrt(0.1)*math.Pow(0.4, 0.25), estimate("+alpha +beta +gamma", standardConfig()), 1e-6)
	require.InDelta(t, 100, estimate("+beta", standardConfig()), 1e-9)
	// Postings say nothing about whether words are adjacent: a phrase of
	// words leaves the estimate a bound, which callers do not take as the
	// MATCH's own; a phrase of grams does not.
	for _, c := range []struct {
		search string
		config AnalyzerConfig
		exact  bool
	}{
		{"+alpha +beta", standardConfig(), true},
		{`+"alpha beta"`, standardConfig(), false},
		{`+gamma -"alpha beta"`, standardConfig(), false},
		{"mysql", ngramConfig(), true},
	} {
		query, err := CompileBooleanQuery(c.search, c.config)
		require.NoError(t, err)
		require.Equal(t, c.exact, query.RowsFromPostings(), c.search)
	}
}
