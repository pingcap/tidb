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
// IndexMerge and that the MATCH is consumed by it rather than re-evaluated.
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
	require.NotRegexp(t, `Selection.*match_against`, text.String())
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

	// Other predicates ride along on the table side.
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

// TestFullTextIndexMatchAgainstKeyColumns covers an index with key columns
// ahead of the tokenized one, the multi-tenant shape: a search is answered
// by the index only within one value of every key column, which the query
// pins with an equality, and every result equals the scan's.
func TestFullTextIndexMatchAgainstKeyColumns(t *testing.T) {
	if !kerneltype.IsClassic() {
		t.Skip("FULLTEXT indexes are held by the columnar engine on the next-gen kernel")
	}
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table tn (id int primary key, tenant int, body text, fulltext index idx (tenant, body))")
	fullTextCorpus(t, tk, "tn", 1500, 4)
	tk.MustExec("update tn set tenant = null where id % 50 = 0")
	tk.MustExec("analyze table tn")

	scanOf := func(sql string) string {
		return strings.Replace(sql, "from tn where", "from tn ignore index (idx) where", 1)
	}
	for _, search := range []string{"+rareword", "+rareword -alpha", "rareword theta", `"distributed storage"`, "rare*", "+of"} {
		for _, tenantCond := range []string{"tenant = 3", "tenant is null", "3 = tenant", "tenant = '3'"} {
			sql := fmt.Sprintf("select id from tn where %s and match(body) against('%s' in boolean mode) order by id", tenantCond, search)
			mustUseFullTextIndex(t, tk, sql, "idx(tenant, body)", search)
			plan := tk.MustQuery("explain format = 'brief' " + sql).Rows()
			var text strings.Builder
			for _, row := range plan {
				text.WriteString(fmt.Sprintln(row...))
			}
			// The equality is served by the index, not re-evaluated on the
			// table side.
			require.Regexp(t, `range:\[(3,3|NULL,NULL)\]`, text.String())
			require.NotContains(t, text.String(), "eq(test.tn.tenant", text.String())
			require.NotContains(t, text.String(), "isnull(test.tn.tenant", text.String())
			tk.MustQuery(sql).Check(tk.MustQuery(scanOf(sql)).Rows())
		}
	}
	tk.MustQuery("select count(*) from tn where tenant = 3 and match(body) against('+rareword' in boolean mode)").
		Check(tk.MustQuery("select count(*) from tn ignore index (idx) where tenant = 3 and match(body) against('+rareword' in boolean mode)").Rows())

	// Without one value for the key column the index cannot confine the
	// search, so the scan stays; the index still authorises the MATCH.
	tk.MustQuery("select @@tidb_enable_local_match_against").Check(testkit.Rows("0"))
	for _, sql := range []string{
		"select id from tn where match(body) against('+rareword' in boolean mode)",
		"select id from tn where tenant in (2, 3) and match(body) against('+rareword' in boolean mode)",
		"select id from tn where (tenant = 2 or tenant = 3) and match(body) against('+rareword' in boolean mode)",
		"select id from tn where tenant > 2 and match(body) against('+rareword' in boolean mode)",
		"select id from tn where tenant = 3 or match(body) against('+rareword' in boolean mode)",
	} {
		mustScan(t, tk, sql)
		tk.MustQuery(sql + " order by id").Check(tk.MustQuery(scanOf(sql) + " order by id").Rows())
	}

	// Changes made earlier in the same transaction are visible through the
	// index, including a row that moves between tenants.
	tk.MustExec("begin")
	tk.MustExec("insert into tn values (100001, 3, 'rareword freshly inserted'), (100002, 4, 'rareword elsewhere')")
	tk.MustExec("update tn set tenant = 3 where id = 97")
	tk.MustExec("update tn set tenant = 4 where id = 194")
	tk.MustExec("delete from tn where id = 291")
	sql := "select id from tn where tenant = 3 and match(body) against('+rareword' in boolean mode) order by id"
	mustUseFullTextIndex(t, tk, sql, "idx(tenant, body)", "+rareword")
	tk.MustQuery(sql).Check(tk.MustQuery(scanOf(sql)).Rows())
	tk.MustQuery("select id from tn where tenant = 3 and match(body) against('+rareword' in boolean mode) and id in (97, 194, 100001, 100002) order by id").Check(testkit.Rows("97", "100001"))
	tk.MustExec("rollback")
	tk.MustExec("admin check table tn")

	// A string key column under a case-insensitive collation: the key holds
	// the sort key, so an equality finds every spelling, as the scan does.
	tk.MustExec("create table ts (id int primary key, tenant varchar(16) collate utf8mb4_general_ci, body text, fulltext index idx (tenant, body))")
	tk.MustExec("insert into ts values (1, 'acme', 'rareword one'), (2, 'Acme', 'rareword two'), (3, 'ACME ', 'rareword three'), (4, 'globex', 'rareword four'), (5, 'acme', 'nothing here')")
	sql = "select id from ts where tenant = 'ACME' and match(body) against('+rareword' in boolean mode) order by id"
	mustUseFullTextIndex(t, tk, sql, "idx(tenant, body)", "+rareword")
	tk.MustQuery(sql).Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery(sql).Check(tk.MustQuery(strings.Replace(sql, "from ts where", "from ts ignore index (idx) where", 1)).Rows())
	tk.MustExec("admin check table ts")

	// Several key columns, and a partitioned table.
	tk.MustExec("create table tp (id int primary key, tenant int, region varchar(8), body text, fulltext index idx (tenant, region, body)) partition by hash(id) partitions 4")
	tk.MustExec("insert into tp select id, tenant, if(id % 2 = 0, 'eu', 'us'), body from tn")
	tk.MustExec("analyze table tp")
	for _, mode := range []string{"dynamic", "static"} {
		tk.MustExec("set @@tidb_partition_prune_mode = '" + mode + "'")
		sql = "select id from tp where tenant = 3 and region = 'eu' and match(body) against('+rareword' in boolean mode) order by id"
		mustUseFullTextIndex(t, tk, sql, "idx(tenant, region, body)", "+rareword")
		tk.MustQuery(sql).Check(tk.MustQuery(strings.Replace(sql, "from tp where", "from tp ignore index (idx) where", 1)).Rows())
		mustScan(t, tk, "select id from tp where tenant = 3 and match(body) against('+rareword' in boolean mode)")
	}
	tk.MustExec("admin check table tp")
}
