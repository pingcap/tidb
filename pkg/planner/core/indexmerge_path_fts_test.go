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

package core_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// prepareFTSMVIndexTable builds a table with a single-column FULLTEXT index,
// which DDL materialises as a multi-valued index over the tokenized column.
// That one index both authorises local MATCH evaluation and gives the access
// path something to read. The rare tokens exist so an index path is clearly
// cheaper than scanning.
func prepareFTSMVIndexTable(t *testing.T) *testkit.TestKit {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_local_match_against=ON")
	tk.MustExec(`create table articles (
		id int primary key,
		body varchar(255),
		fulltext index idx_body(body)
	)`)
	for i := range 1000 {
		tk.MustExec(fmt.Sprintf("insert into articles values (%d, 'common filler text number %d')", i, i))
	}
	tk.MustExec("insert into articles values (1001, 'rareone raretwo together here')")
	tk.MustExec("insert into articles values (1002, 'rareone alone here')")
	tk.MustExec("insert into articles values (1003, 'raretwo alone here')")
	tk.MustExec("analyze table articles")
	return tk
}

func usesFTSMVIndex(tk *testkit.TestKit, query string) bool {
	for _, row := range tk.MustQuery("explain " + query).Rows() {
		if strings.Contains(fmt.Sprintf("%v", row[3]), "idx_body") {
			return true
		}
	}
	return false
}

func TestFTSMatchAgainstUsesMVIndex(t *testing.T) {
	tk := prepareFTSMVIndexTable(t)

	for _, tc := range []struct {
		search   string
		indexed  bool
		expected []string
	}{
		// A required term is the token the index looks up.
		{search: "+rareone", indexed: true, expected: []string{"1001", "1002"}},
		// Required terms intersect; either one alone is a sound candidate set,
		// and the MATCH residual removes the rest.
		{search: "+rareone +raretwo", indexed: true, expected: []string{"1001"}},
		// Optional terms union.
		{search: "rareone raretwo", indexed: true, expected: []string{"1001", "1002", "1003"}},
		// A phrase contributes its tokens; adjacency is left to the residual.
		{search: `"rareone raretwo"`, indexed: true, expected: []string{"1001"}},
		// A prohibited term cannot narrow anything, but must not stop the
		// required term from doing so.
		{search: "+rareone -raretwo", indexed: true, expected: []string{"1002"}},
		// A prefix cannot be expressed as a token lookup, so there is nothing
		// to derive and the query scans.
		{search: "+rare*", indexed: false, expected: []string{"1001", "1002", "1003"}},
	} {
		t.Run(tc.search, func(t *testing.T) {
			query := fmt.Sprintf(
				"select id from articles where match(body) against('%s' in boolean mode) order by id",
				tc.search)
			require.Equal(t, tc.indexed, usesFTSMVIndex(tk, query),
				"unexpected access path for %q", tc.search)
			rows := make([][]any, 0, len(tc.expected))
			for _, id := range tc.expected {
				rows = append(rows, []any{id})
			}
			tk.MustQuery(query).Check(rows)
		})
	}
}

// TestFTSMatchAgainstNegatedKeepsScan pins the polarity restriction. The
// derived terms select the rows a MATCH keeps, so using them under a negation
// would build ranges over precisely the rows that must be excluded and drop
// every qualifying row.
func TestFTSMatchAgainstNegatedKeepsScan(t *testing.T) {
	tk := prepareFTSMVIndexTable(t)

	query := "select count(*) from articles where not match(body) against('+rareone' in boolean mode)"
	require.False(t, usesFTSMVIndex(tk, query))
	tk.MustQuery(query).Check([][]any{{"1001"}})

	query = "select count(*) from articles where match(body) against('+rareone' in boolean mode) or id < 5"
	require.False(t, usesFTSMVIndex(tk, query))
	tk.MustQuery(query).Check([][]any{{"7"}})
}

// TestFTSMatchAgainstMismatchedAnalyzerIsNotUsed covers the gate on the index
// being built by the analyzer the query compiled with. A FULLTEXT index now
// carries its own analyzer, so what this rules out is answering the query from
// some other multi-valued index over the same column whose literals differ:
// its entries were produced by a different token stream.
func TestFTSMatchAgainstMismatchedAnalyzerIsNotUsed(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_local_match_against=ON")
	// idx_hand bounds tokens at 5 characters; the FULLTEXT index uses the
	// session default of 3, and is the one the MATCH compiles against.
	tk.MustExec(`create table articles (
		id int primary key,
		body varchar(255),
		fulltext index idx_body(body),
		key idx_hand ((cast(fts_tokenize(body, 'STANDARD', 5, 84, 1) as char(84) array)))
	)`)
	for i := range 1000 {
		tk.MustExec(fmt.Sprintf("insert into articles values (%d, 'common filler text number %d')", i, i))
	}
	tk.MustExec("insert into articles values (1001, 'rareone alone here')")
	tk.MustExec("analyze table articles")

	query := "select id from articles where match(body) against('+rareone' in boolean mode)"
	plan := fmt.Sprintf("%v", tk.MustQuery("explain "+query).Rows())
	require.Contains(t, plan, "idx_body")
	require.NotContains(t, plan, "idx_hand")
	tk.MustQuery(query).Check([][]any{{"1001"}})
}

// TestFTSMatchAgainstUsesIndexAnalyzerNotSession pins where the analyzer comes
// from for each shape of FULLTEXT index.
//
// An index materialised as a multi-valued index froze its settings into the
// expression it is built over, so the tokens it holds were produced by those
// settings. A later variable change describes what a NEW index would be built
// with, and must not silently reinterpret rows already indexed. A metadata-only
// index has no such snapshot, so it still follows the variables - which is what
// the multi-column control here proves is live, rather than leaving this test
// able to pass because the knob does nothing.
func TestFTSMatchAgainstUsesIndexAnalyzerNotSession(t *testing.T) {
	tk := prepareFTSMVIndexTable(t)
	// A multi-column FULLTEXT index stays metadata-only.
	tk.MustExec(`create table multi (
		a varchar(100), b varchar(100),
		fulltext index idx_ab(a, b)
	)`)
	tk.MustExec("insert into multi values ('rareone alone', 'here')")

	indexed := "select id from articles where match(body) against('+rareone' in boolean mode) order by id"
	metadataOnly := "select a from multi where match(a, b) against('+rareone' in boolean mode)"
	tk.MustQuery(indexed).Check([][]any{{"1001"}, {"1002"}})
	tk.MustQuery(metadataOnly).Check([][]any{{"rareone alone"}})

	// innodb_ft_min_token_size is global-only; the store is private to this
	// test, so the change cannot leak. At 8 the 7-character search term is
	// below the minimum and analyzes away.
	tk.MustExec("set global innodb_ft_min_token_size=8")

	// Control: the metadata-only index follows the variable, and now matches
	// nothing. If this ever stops changing, the assertion below is vacuous.
	tk.MustQuery(metadataOnly).Check([][]any{})

	// The MV-backed index keeps its own analyzer, so the query is unchanged
	// and still reaches the index.
	require.True(t, usesFTSMVIndex(tk, indexed))
	tk.MustQuery(indexed).Check([][]any{{"1001"}, {"1002"}})
}

// TestFTSMatchAgainstNgramUsesMVIndex covers the other analyzer. The ngram
// parser sizes its grams from the min_token_size argument, and that size is
// also the element width the index needs.
func TestFTSMatchAgainstNgramUsesMVIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_local_match_against=ON")
	// ngram_token_size is global-only and defaults to 2, which is the size the
	// index below is built with.
	tk.MustExec(`create table articles (
		id int primary key,
		body varchar(255),
		fulltext index idx_body(body) with parser ngram
	)`)
	for i := range 1000 {
		tk.MustExec(fmt.Sprintf("insert into articles values (%d, 'common filler text number %d')", i, i))
	}
	tk.MustExec("insert into articles values (1001, 'zq alone here')")
	tk.MustExec("analyze table articles")

	query := "select id from articles where match(body) against('+zq' in boolean mode)"
	require.True(t, usesFTSMVIndex(tk, query))
	tk.MustQuery(query).Check([][]any{{"1001"}})
}
