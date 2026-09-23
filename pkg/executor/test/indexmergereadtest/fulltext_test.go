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

package indexmergereadtest

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// fullTextCorpus fills a table with documents drawn from a small vocabulary
// and a few rare words, so that most searches are selective enough to be
// planned through the index while the scan plan stays available for
// comparison.
func fullTextCorpus(t *testing.T, tk *testkit.TestKit, table string, rows int, seed int64) {
	words := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "of", "the", "distributed", "storage"}
	rng := rand.New(rand.NewSource(seed))
	values := make([]string, 0, rows)
	for i := 1; i <= rows; i++ {
		n := 3 + rng.Intn(10)
		parts := make([]string, n)
		for j := range parts {
			parts[j] = words[rng.Intn(len(words))]
		}
		if i%97 == 0 {
			parts = append(parts, "rareword")
		}
		values = append(values, fmt.Sprintf("(%d, %d, '%s')", i, i%7, strings.Join(parts, " ")))
	}
	tk.MustExec(fmt.Sprintf("insert into %s values %s", table, strings.Join(values, ",")))
}

// mustUseFullTextIndex asserts that the plan reads the FULLTEXT index under an
// IndexMerge and keeps the MATCH above it as a residual filter.
func mustUseFullTextIndex(t *testing.T, tk *testkit.TestKit, sql, index, search string) {
	plan := tk.MustQuery("explain format = 'brief' " + sql).Rows()
	var text strings.Builder
	for _, row := range plan {
		for _, col := range row {
			text.WriteString(fmt.Sprint(col))
			text.WriteString(" ")
		}
		text.WriteString("\n")
	}
	require.Contains(t, text.String(), "IndexMerge", text.String())
	require.Contains(t, text.String(), "FullTextIndexScan(Build)", text.String())
	require.Contains(t, text.String(), "FullTextIndexScan(Build)", text.String())
	require.Regexp(t, `FullTextIndexScan\(Build\) [0-9.]+ root table:`, text.String())
	require.Regexp(t, `TableRowIDScan(\(Probe\))? [0-9.]+ cop\[tikv\] table:`, text.String())
	require.Contains(t, text.String(), "index:"+index, text.String())
	require.Contains(t, text.String(), "fulltext:"+strconv.Quote(search), text.String())
	require.Contains(t, text.String(), "match_against(", text.String())
}

func mustScan(t *testing.T, tk *testkit.TestKit, sql string) {
	plan := tk.MustQuery("explain format = 'brief' " + sql).Rows()
	var text strings.Builder
	for _, row := range plan {
		text.WriteString(fmt.Sprint(row))
		text.WriteString("\n")
	}
	require.NotContains(t, text.String(), "fulltext:", text.String())
}

// TestFullTextIndexMatchAgainst covers MATCH ... AGAINST answered by a
// FULLTEXT index built in TiKV: the plan reads the index, and every result
// equals the one the full scan produces.
func TestFullTextIndexMatchAgainst(t *testing.T) {
	if !kerneltype.IsClassic() {
		t.Skip("FULLTEXT indexes are held by the columnar engine on the next-gen kernel")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, k int, body text, fulltext index idx (body))")
	fullTextCorpus(t, tk, "t", 1500, 1)
	tk.MustExec("analyze table t")

	searches := []string{
		"+rareword", "+rareword +alpha", "+rareword -alpha", "rareword theta",
		`"distributed storage"`, `+"alpha beta" +rareword`, "rare*", "+rareword +epsi*",
		"+nosuchword", "+of", "+rareword +of",
	}
	for _, search := range searches {
		sql := fmt.Sprintf("select id from t where match(body) against('%s' in boolean mode) order by id", search)
		mustUseFullTextIndex(t, tk, sql, "idx(body)", search)
		expected := tk.MustQuery(fmt.Sprintf("select id from t ignore index (idx) where match(body) against('%s' in boolean mode) order by id", search)).Rows()
		tk.MustQuery(sql).Check(expected)
	}
	// The scan comparison above must itself be a scan, and must be enabled
	// by the index rather than the session variable, which stays off.
	tk.MustQuery("select @@tidb_enable_local_match_against").Check(testkit.Rows("0"))
	mustScan(t, tk, "select id from t ignore index (idx) where match(body) against('+rareword' in boolean mode)")

	// Other predicates ride along on the table side; the MATCH stays a
	// residual above the lookup.
	sql := "select id from t where match(body) against('+rareword' in boolean mode) and k = 3 order by id"
	mustUseFullTextIndex(t, tk, sql, "idx(body)", "+rareword")
	tk.MustQuery(sql).Check(tk.MustQuery("select id from t ignore index (idx) where match(body) against('+rareword' in boolean mode) and k = 3 order by id").Rows())
	tk.MustQuery("select count(*) from t where match(body) against('+rareword' in boolean mode)").Check(testkit.Rows("15"))

	// The index cannot serve a MATCH in negative position, in one branch of
	// an OR, or with a search string the plan cannot bake in.
	mustScan(t, tk, "select id from t where not match(body) against('+rareword' in boolean mode)")
	mustScan(t, tk, "select id from t where match(body) against('+rareword' in boolean mode) or k = 1")
	tk.MustExec("prepare stmt from 'select id from t where match(body) against(? in boolean mode) order by id'")
	tk.MustExec("set @s = '+rareword'")
	tk.MustQuery("execute stmt using @s").Check(tk.MustQuery("select id from t where match(body) against('+rareword' in boolean mode) order by id").Rows())
	tk.MustExec("set @s = '+alpha +beta'")
	tk.MustQuery("execute stmt using @s").Check(tk.MustQuery("select id from t ignore index (idx) where match(body) against('+alpha +beta' in boolean mode) order by id").Rows())
	// A user variable in a plain statement is folded at plan time, so the
	// index serves it; only a prepared parameter stays unknown.
	mustUseFullTextIndex(t, tk, "select id from t where match(body) against(@s in boolean mode)", "idx(body)", "+alpha +beta")
	// Natural language mode is not a boolean predicate the index can serve.
	mustScan(t, tk, "select id from t where match(body) against('rareword')")

	// Changes made earlier in the same transaction are visible through the
	// index, both ways.
	tk.MustExec("begin")
	tk.MustExec("insert into t values (100001, 0, 'rareword freshly inserted')")
	tk.MustExec("update t set body = 'no longer rare' where id = 97")
	tk.MustExec("delete from t where id = 194")
	tk.MustExec("update t set k = 99 where id = 291")
	sql = "select id from t where match(body) against('+rareword' in boolean mode) order by id"
	mustUseFullTextIndex(t, tk, sql, "idx(body)", "+rareword")
	tk.MustQuery(sql).Check(tk.MustQuery("select id from t ignore index (idx) where match(body) against('+rareword' in boolean mode) order by id").Rows())
	tk.MustQuery("select id, k from t where match(body) against('+rareword' in boolean mode) and id = 291").Check(testkit.Rows("291 99"))
	tk.MustExec("rollback")
	tk.MustQuery("select count(*) from t where match(body) against('+rareword' in boolean mode)").Check(testkit.Rows("15"))
	tk.MustExec("admin check table t")
}

// TestFullTextIndexMatchAgainstNgram covers the NGRAM analyzer on long
// documents, the case a token index without positions could not answer.
func TestFullTextIndexMatchAgainstNgram(t *testing.T) {
	if !kerneltype.IsClassic() {
		t.Skip("FULLTEXT indexes are held by the columnar engine on the next-gen kernel")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table ng (id int primary key, body text, fulltext index idx (body) with parser ngram)")
	rng := rand.New(rand.NewSource(2))
	alphabet := []rune("abcdefgh数据库系统")
	values := make([]string, 0, 400)
	for i := 1; i <= 400; i++ {
		n := 200 + rng.Intn(2000)
		doc := make([]rune, n)
		for j := range doc {
			doc[j] = alphabet[rng.Intn(len(alphabet))]
		}
		values = append(values, fmt.Sprintf("(%d, '%s')", i, string(doc)))
	}
	tk.MustExec("insert into ng values " + strings.Join(values, ","))
	tk.MustExec("analyze table ng")

	for _, search := range []string{"数据库", "abcd", "库系统数", "+abc -数据", "abc 数据库系"} {
		sql := fmt.Sprintf("select id from ng where match(body) against('%s' in boolean mode) order by id", search)
		mustUseFullTextIndex(t, tk, sql, "idx(body)", search)
		tk.MustQuery(sql).Check(tk.MustQuery(fmt.Sprintf("select id from ng ignore index (idx) where match(body) against('%s' in boolean mode) order by id", search)).Rows())
	}
}

// TestFullTextIndexMatchAgainstPartitioned covers a partitioned table, where
// each partition holds its own index and the pruned partitions are scanned.
func TestFullTextIndexMatchAgainstPartitioned(t *testing.T) {
	if !kerneltype.IsClassic() {
		t.Skip("FULLTEXT indexes are held by the columnar engine on the next-gen kernel")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table pt (id int primary key, k int, body text, fulltext index idx (body)) partition by hash(id) partitions 4")
	fullTextCorpus(t, tk, "pt", 1500, 3)
	tk.MustExec("analyze table pt")
	for _, mode := range []string{"dynamic", "static"} {
		tk.MustExec("set @@tidb_partition_prune_mode = '" + mode + "'")
		for _, search := range []string{"+rareword", "+rareword +alpha", `"distributed storage"`} {
			sql := fmt.Sprintf("select id from pt where match(body) against('%s' in boolean mode) order by id", search)
			mustUseFullTextIndex(t, tk, sql, "idx(body)", search)
			tk.MustQuery(sql).Check(tk.MustQuery(fmt.Sprintf("select id from pt ignore index (idx) where match(body) against('%s' in boolean mode) order by id", search)).Rows())
		}
		sql := "select id from pt where match(body) against('+rareword' in boolean mode) and id in (97, 194, 291, 5) order by id"
		tk.MustQuery(sql).Check(testkit.Rows("97", "194", "291"))
	}
}
