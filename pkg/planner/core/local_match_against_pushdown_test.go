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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestMatchAgainstBooleanPushdownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table articles (
		id int primary key,
		title varchar(200),
		body text,
		fulltext index idx_title(title),
		fulltext index idx_title_body(title, body)
	)`)

	dom := domain.GetDomain(tk.Session())
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("articles"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")
	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	tk.MustExec("set @@session.tidb_enable_local_match_against=OFF")
	require.Error(t, tk.ExecToErr("select id from articles where match(title) against('+tidb' in boolean mode)"),
		"OFF must disable TiFlash's row-wise Boolean MATCH as well as TiDB fallback")
	tk.MustExec("set @@session.tidb_enable_local_match_against=ON")
	tk.MustExec("set @@session.collation_server='utf8mb4_general_ci'")

	queries := []struct {
		sql       string
		name      string
		columnNum int
	}{
		{
			sql:       "select id from articles where match(title) against('+tidb -mysql' in boolean mode)",
			name:      "single_column_match",
			columnNum: 1,
		},
		{
			sql:       "select id from articles where match(title, body) against('+tidb -mysql' in boolean mode)",
			name:      "multi_column_match",
			columnNum: 2,
		},
		{
			sql:       "select id from articles where match(body, title) against('+tidb -mysql' in boolean mode)",
			name:      "reordered_multi_column_match",
			columnNum: 2,
		},
	}
	for _, query := range queries {
		t.Run(query.name, func(t *testing.T) {
			plan := compilePhysicalPlan(t, tk, query.sql)
			scan := findLocalMatchAgainstTableScan(t, plan)
			// MATCH columns must be read by TiFlash even when they are not
			// projected by the SQL query; the scalar Selection evaluates them.
			require.Len(t, plan.Schema().Columns, 1)
			scanColumnNames := make(map[string]struct{}, len(scan.Columns))
			for _, col := range scan.Columns {
				scanColumnNames[col.Name.L] = struct{}{}
			}
			require.Contains(t, scanColumnNames, "title")
			if query.columnNum == 2 {
				require.Contains(t, scanColumnNames, "body")
			}

			pb, err := scan.ToPB(tk.Session().GetBuildPBCtx(), kv.TiFlash)
			require.NoError(t, err)
			require.NotNil(t, pb.TblScan)
			metadata := assertLocalMatchAgainstScalarSelection(t, tk, plan, query.columnNum)
			require.NotZero(t, metadata.GetVersion())
			require.Equal(t, "utf8mb4_general_ci", metadata.GetStopwordCollation(),
				"TiFlash stopword lookup must use collation_server, not the MATCH column collation")

			explainRows := tk.MustQuery("explain format='brief' " + query.sql).Rows()
			explain := fmt.Sprint(explainRows)
			require.Contains(t, explain, "tiflash")
			require.Contains(t, strings.ToLower(explain), "selection")
		})
	}
}

func TestMatchAgainstMultiColumnCollation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_local_match_against=ON")
	tk.MustExec("set tidb_allow_tiflash_cop=ON")
	for _, parser := range []string{"", " WITH PARSER NGRAM"} {
		tk.MustExec("drop table if exists mixed_match")
		tk.MustExec(`create table mixed_match(id int primary key,
			title text collate utf8mb4_bin, body text collate utf8mb4_general_ci,
			fulltext ft(title,body)` + parser + `,
			fulltext ft_title(title)` + parser + `, fulltext ft_body(body)` + parser + `)`)
		tk.MustExec("insert into mixed_match values(1,'FOO','BAR')")
		tbl, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mixed_match"))
		require.NoError(t, err)
		for _, replicaAvailable := range []bool{false, true} {
			tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: replicaAvailable}
			for _, engine := range []string{"tikv", "tiflash"} {
				if !replicaAvailable && engine == "tiflash" {
					continue // No TiFlash access path is rejected before MATCH rewriting.
				}
				tk.MustExec("set tidb_isolation_read_engines='" + engine + "'")
				for _, columns := range []string{"title,body", "body,title"} {
					for _, search := range []string{"'+foo'", "NULL", "''"} {
						sql := "select id from mixed_match where match(" + columns + ") against(" + search + " in boolean mode)"
						err := tk.ExecToErr(sql)
						require.ErrorContains(t, err, "different MATCH column collations", sql)
					}
				}
			}
		}
		// Independent MATCH expressions may still use different collations.
		tk.MustExec("set tidb_isolation_read_engines='tikv'")
		tk.MustQuery("select id from mixed_match where match(title) against('+FOO' in boolean mode) or match(body) against('+bar' in boolean mode)").Check(testkit.Rows("1"))
	}
	for _, collation := range []string{"utf8mb4_bin", "utf8mb4_general_ci"} {
		tk.MustExec("drop table if exists same_match")
		tk.MustExec("create table same_match(id int primary key,title text collate " + collation + ",body text collate " + collation + ",fulltext ft(title,body))")
		tk.MustExec("insert into same_match values(1,'FOO','BAR')")
		for _, columns := range []string{"title,body", "body,title"} {
			result := tk.MustQuery("select id from same_match where match(" + columns + ") against('+foo' in boolean mode)")
			if collation == "utf8mb4_bin" {
				result.Check(testkit.Rows())
			} else {
				result.Check(testkit.Rows("1"))
			}
		}
	}
}

func TestMatchAgainstTiDBFallbackWhenTiFlashIsNotSelected(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table fallback_articles (
		id int primary key,
		body text,
		fulltext index idx_body(body)
	)`)
	tk.MustExec("insert into fallback_articles values (1, 'tidb storage'), (2, 'mysql database')")
	dom := domain.GetDomain(tk.Session())
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("fallback_articles"))
	require.NoError(t, err)
	// TiFlash is available, so the rewritten scalar carries TiFlash metadata;
	// restrict this query to TiKV to exercise the same expression's TiDB fallback.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")
	tk.MustExec("set @@session.tidb_isolation_read_engines='tikv'")
	tk.MustExec("set @@session.tidb_enable_local_match_against=ON")

	sql := "select id from fallback_articles where match(body) against('+tidb' in boolean mode) order by id"
	tk.MustQuery(sql).Check(testkit.Rows("1"))
	plan := strings.ToLower(fmt.Sprint(tk.MustQuery("explain format='brief' " + sql).Rows()))
	require.NotContains(t, plan, "mpp[tiflash]")
}

func TestMatchAgainstLocalPlanCacheDisabled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_local_match_against=ON,tidb_enable_prepared_plan_cache=ON,tidb_isolation_read_engines='tikv'")
	for _, parser := range []string{"", " WITH PARSER NGRAM"} {
		tk.MustExec("drop table if exists cache_docs")
		tk.MustExec("create table cache_docs(id int primary key, body text collate utf8mb4_bin, fulltext ft(body)" + parser + ")")
		tk.MustExec("insert into cache_docs values(1,'the'),(2,'The'),(3,'foo'),(4,'aaa'),(5,'AAA')")
		// Ordinary statements still use the enabled prepared plan cache.
		tk.MustExec("prepare ordinary from 'select id from cache_docs where id=?'")
		tk.MustExec("set @id=3")
		for range 2 {
			tk.MustQuery("execute ordinary using @id").Check(testkit.Rows("3"))
		}
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
		tk.MustExec("deallocate prepare ordinary")

		check := func(sql string, rows ...string) {
			t.Helper()
			for range 2 {
				tk.MustQuery(sql).Check(testkit.Rows(rows...))
				tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
			}
		}
		tk.MustExec("set innodb_ft_enable_stopword=OFF,collation_server='utf8mb4_bin'")
		tk.MustExec("prepare p from 'select id from cache_docs where match(body) against(? in boolean mode) order by id'")
		search, upperSearch, id, upperID := "+the", "+The", "1", "2"
		if parser != "" {
			search, upperSearch, id, upperID = "+aaa", "+AAA", "4", "5"
		}
		tk.MustExec("set @q='" + search + "'")
		check("execute p using @q", id)
		tk.MustExec("set innodb_ft_enable_stopword=ON")
		check("execute p using @q")
		tk.MustExec("set innodb_ft_enable_stopword=OFF")
		check("execute p using @q", id)
		tk.MustExec("set innodb_ft_enable_stopword=ON")
		tk.MustExec("set @q='" + upperSearch + "'")
		check("execute p using @q", upperID)
		tk.MustExec("set collation_server='utf8mb4_general_ci'")
		check("execute p using @q")
		tk.MustExec("set collation_server='utf8mb4_bin'")
		check("execute p using @q", upperID)
		tk.MustExec("set @q=NULL")
		check("execute p using @q")
		tk.MustExec("set @q='+foo'")
		check("execute p using @q", "3")
		tk.MustExec("deallocate prepare p")

		// A constant AGAINST still embeds analyzer configuration in the plan,
		// even when the parameter belongs to another predicate.
		tk.MustExec("set innodb_ft_enable_stopword=OFF")
		tk.MustExec("prepare p from 'select id from cache_docs where id>? and match(body) against(\"" + search + "\" in boolean mode)'")
		tk.MustExec("set @id=0")
		check("execute p using @id", id)
		tk.MustExec("set innodb_ft_enable_stopword=ON")
		check("execute p using @id")
		tk.MustExec("deallocate prepare p")

		tk.MustExec("set tidb_enable_non_prepared_plan_cache=ON")
		for _, stopword := range []string{"OFF", "ON", "OFF"} {
			tk.MustExec("set innodb_ft_enable_stopword=" + stopword)
			rows := []string{id}
			if stopword == "ON" {
				rows = nil
			}
			check("select id from cache_docs where match(body) against('"+search+"' in boolean mode)", rows...)
		}
		// Global-only token settings are also planning inputs. Changes here
		// affect only this test's isolated mock store, not an external cluster.
		tk.MustExec("set innodb_ft_enable_stopword=OFF")
		setting, values, tokenSearch := "innodb_ft_min_token_size", []string{"3", "4", "3"}, "+foo"
		if parser != "" {
			setting, values, tokenSearch = "ngram_token_size", []string{"2", "3", "2"}, "+fo"
		}
		tk.MustExec("prepare p from 'select id from cache_docs where match(body) against(? in boolean mode)'")
		tk.MustExec("set @q='" + tokenSearch + "'")
		for i, value := range values {
			tk.MustExec("set global " + setting + "=" + value)
			rows := []string{"3"}
			if i == 1 {
				rows = nil
			}
			check("execute p using @q", rows...)
		}
		tk.MustExec("deallocate prepare p")

		// The same rule applies before TiFlash placement is chosen. A fake
		// replica lets us inspect fresh metadata without running a TiFlash peer.
		tbl, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("cache_docs"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
		tk.MustExec("set tidb_isolation_read_engines='tiflash',tidb_allow_tiflash_cop=ON")
		tk.MustExec("prepare p from 'select id from cache_docs where id>? and match(body) against(\"+foo\" in boolean mode)'")
		for _, stopword := range []string{"OFF", "ON", "OFF"} {
			tk.MustExec("set innodb_ft_enable_stopword=" + stopword)
			for range 2 {
				plan := compilePhysicalPlan(t, tk, "execute p using @id").(*core.Execute).Plan
				metadata := assertLocalMatchAgainstScalarSelection(t, tk, plan, 1)
				mode := tipb.LocalMatchAgainstStopwordMode_LocalMatchAgainstStopwordModeDisabled
				if stopword == "ON" {
					mode = tipb.LocalMatchAgainstStopwordMode_LocalMatchAgainstStopwordModeBuiltin
				}
				require.Equal(t, mode, metadata.GetStopwordMode())
				require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
			}
		}
		tk.MustExec("deallocate prepare p")
		tk.MustExec("set tidb_isolation_read_engines='tikv'")
	}
}

func TestMatchAgainstForcePlanCacheDisabled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cache_docs(id int primary key,body text,fulltext ft(body))")
	tk.MustExec("insert into cache_docs values(1,'the')")
	tk.MustExec("set tidb_enable_local_match_against=ON,tidb_enable_prepared_plan_cache=ON,tidb_isolation_read_engines='tikv',tidb_opt_fix_control='49736:ON',innodb_ft_enable_stopword=OFF")
	tk.MustExec("prepare p from 'select id from cache_docs where match(body) against(? in boolean mode)'")
	tk.MustExec("set @q='+the'")
	for _, stopword := range []string{"OFF", "ON", "OFF"} {
		tk.MustExec("set innodb_ft_enable_stopword=" + stopword)
		for range 2 {
			rows := testkit.Rows("1")
			if stopword == "ON" {
				rows = testkit.Rows()
			}
			tk.MustQuery("execute p using @q").Check(rows)
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		}
	}
	// Clearing the force flag is statement-local, not a session-wide change.
	tk.MustExec("prepare ordinary from 'select id from cache_docs where id=?'")
	tk.MustExec("set @id=1")
	for range 2 {
		tk.MustQuery("execute ordinary using @id").Check(testkit.Rows("1"))
	}
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestMatchAgainstPreparedNumericSearchPushdown(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table numeric_docs(id int primary key, body text, fulltext index ft(body))")
	tk.MustExec("insert into numeric_docs values(1, '123'),(2, '456')")
	tk.MustExec("set tidb_enable_local_match_against=ON,tidb_allow_tiflash_cop=ON,tidb_enable_prepared_plan_cache=ON")
	tk.MustExec("prepare p from 'select id from numeric_docs where match(body) against(? in boolean mode)'")
	tbl, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("numeric_docs"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	for _, tc := range []struct{ value, text, want string }{
		{"123", "123", "1"}, {"456", "456", "2"}, {"'123'", "123", "1"}, {"NULL", "", ""},
	} {
		tk.MustExec("set @q=" + tc.value)
		tk.MustExec("set tidb_isolation_read_engines='tikv'")
		if tc.want == "" {
			tk.MustQuery("execute p using @q").Check(testkit.Rows())
		} else {
			tk.MustQuery("execute p using @q").Check(testkit.Rows(tc.want))
		}
		tk.MustExec("set tidb_isolation_read_engines='tiflash'")
		plan := compilePhysicalPlan(t, tk, "execute p using @q").(*core.Execute).Plan
		if tc.text == "" {
			continue // NULL may be folded to an empty plan rather than serialized.
		}
		metadata := assertLocalMatchAgainstScalarSelection(t, tk, plan, 1)
		require.Len(t, metadata.Nodes, 1)
		require.Equal(t, tc.text, metadata.Nodes[0].Text)
	}
}

func TestMatchAgainstUnsafePushdownUsesTiDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table prefix_docs(id int primary key, body text collate utf8mb4_general_ci, fulltext index ft(body))")
	tk.MustExec("insert into prefix_docs values(1, 'foo barista'),(2, 'foo only'),(3, 'barista only'),(4, 'baz foo'),(5, 'baz barista'),(6, 'baz other'),(7, 'TiDB storage')")
	tk.MustExec("set tidb_enable_local_match_against=ON,tidb_allow_tiflash_cop=ON,tidb_isolation_read_engines='tikv,tiflash'")
	tbl, err := domain.GetDomain(tk.Session()).InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("prefix_docs"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	for _, tc := range []struct {
		search string
		want   []string
	}{
		{"+foo.bar*", []string{"1"}},
		{"foo.bar*", []string{"1", "2", "3", "4", "5"}},
		{"baz -foo.bar*", []string{"6"}},
	} {
		sql := "select id from prefix_docs where match(body) against('" + tc.search + "' in boolean mode) order by id"
		assertMatchAgainstRootSelection(t, tk, sql)
		// The fake replica permits plan inspection, but has no TiFlash peer
		// to serve a coprocessor scan. Execute result assertions through TiKV.
		tk.MustExec("set tidb_isolation_read_engines='tikv'")
		tk.MustQuery(sql).Check(testkit.Rows(tc.want...))
		tk.MustExec("set tidb_isolation_read_engines='tikv,tiflash'")
	}

	oldMode := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(false)
	t.Cleanup(func() { collate.SetNewCollationEnabledForTest(oldMode) })
	for _, tc := range []struct{ search, want string }{{"+tidb", ""}, {"+TiDB", "7"}} {
		sql := "select id from prefix_docs where match(body) against('" + tc.search + "' in boolean mode)"
		assertMatchAgainstRootSelection(t, tk, sql)
		tk.MustExec("set tidb_isolation_read_engines='tikv'")
		if tc.want == "" {
			tk.MustQuery(sql).Check(testkit.Rows())
		} else {
			tk.MustQuery(sql).Check(testkit.Rows(tc.want))
		}
		tk.MustExec("set tidb_isolation_read_engines='tikv,tiflash'")
	}
}

func assertMatchAgainstRootSelection(t *testing.T, tk *testkit.TestKit, sql string) {
	t.Helper()
	found := false
	for _, row := range tk.MustQuery("explain format='brief' " + sql).Rows() {
		text := strings.ToLower(fmt.Sprint(row))
		if strings.Contains(text, "match_against") {
			require.Contains(t, text, "selection")
			require.Contains(t, text, "root")
			require.NotContains(t, text, "mpp[tiflash]")
			found = true
		}
	}
	require.True(t, found)
}

func TestMatchAgainstNgramBooleanPushdownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table ngram_articles (
		id int primary key,
		title varchar(200),
		fulltext index idx_title_ngram(title) with parser ngram
	)`)

	dom := domain.GetDomain(tk.Session())
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("ngram_articles"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")
	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	tk.MustExec("set @@session.tidb_enable_local_match_against=ON")
	tk.MustExec("set @@session.collation_server='utf8mb4_bin'")

	sql := "select id from ngram_articles where match(title) against('+tidb' in boolean mode)"
	plan := compilePhysicalPlan(t, tk, sql)
	scan := findLocalMatchAgainstTableScan(t, plan)
	require.Len(t, plan.Schema().Columns, 1)
	var hasTitle bool
	for _, col := range scan.Columns {
		if col.Name.L == "title" {
			hasTitle = true
			break
		}
	}
	require.True(t, hasTitle, "the TiFlash table scan must include the NGRAM MATCH column")
	metadata := assertLocalMatchAgainstScalarSelection(t, tk, plan, 1)
	require.Equal(t, tipb.LocalMatchAgainstParser_LocalMatchAgainstParserNgram, metadata.GetParser())
	require.Equal(t, uint32(2), metadata.GetNgramTokenSize())
	require.Equal(t, tipb.LocalMatchAgainstStopwordMode_LocalMatchAgainstStopwordModeBuiltin, metadata.GetStopwordMode(),
		"the NGRAM stopword setting must be serialized for TiFlash")
	require.Equal(t, "utf8mb4_bin", metadata.GetStopwordCollation())

	// The same protocol field must preserve an explicit OFF setting too.
	tk.MustExec("set session innodb_ft_enable_stopword=OFF")
	plan = compilePhysicalPlan(t, tk, sql)
	metadata = assertLocalMatchAgainstScalarSelection(t, tk, plan, 1)
	require.Equal(t, tipb.LocalMatchAgainstStopwordMode_LocalMatchAgainstStopwordModeDisabled, metadata.GetStopwordMode())
	scan = findLocalMatchAgainstTableScan(t, plan)

	pb, err := scan.ToPB(tk.Session().GetBuildPBCtx(), kv.TiFlash)
	require.NoError(t, err)
	require.NotNil(t, pb.TblScan)

	explainRows := tk.MustQuery("explain format='brief' " + sql).Rows()
	require.Contains(t, fmt.Sprint(explainRows), "tiflash")
}

func TestMultipleMatchAgainstBooleanPredicatesUseTiFlashSelection(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table multi_match_articles (
		id int primary key,
		title varchar(200),
		fulltext index idx_title(title)
	)`)

	dom := domain.GetDomain(tk.Session())
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("multi_match_articles"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")
	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	tk.MustExec("set @@session.tidb_enable_local_match_against=ON")

	sql := "select id from multi_match_articles where " +
		"match(title) against('+tidb' in boolean mode) OR " +
		"match(title) against('+mysql' in boolean mode)"
	plan := compilePhysicalPlan(t, tk, sql)
	// Multi-predicate Boolean expressions are represented by a TiFlash
	// Selection containing scalar Local MATCH calls, not by one scan-level query.
	selection := findLocalMatchAgainstSelection(t, plan)
	selectionPB, err := selection.ToPB(tk.Session().GetBuildPBCtx(), kv.TiFlash)
	require.NoError(t, err)
	var booleanFunctionCount int
	for _, condition := range selectionPB.GetSelection().GetConditions() {
		booleanFunctionCount += countScalarFunctionExpr(condition, tipb.ScalarFuncSig_LocalMatchAgainstBoolean)
	}
	require.Equal(t, 2, booleanFunctionCount)
	explain := strings.ToLower(fmt.Sprint(tk.MustQuery("explain format='brief' " + sql).Rows()))
	require.Contains(t, explain, "mpp[tiflash]")
	require.Contains(t, explain, "selection")
	require.Contains(t, explain, "match_against")
}

func TestMatchAgainstStandardAnalyzerSettingsPushdownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table standard_articles (
		id int primary key,
		body text,
		fulltext index idx_body(body)
	)`)

	oldMinTokenSize := tk.MustQuery("select @@global.innodb_ft_min_token_size").Rows()[0][0]
	oldMaxTokenSize := tk.MustQuery("select @@global.innodb_ft_max_token_size").Rows()[0][0]
	defer func() {
		tk.MustExec(fmt.Sprintf("set global innodb_ft_min_token_size=%v", oldMinTokenSize))
		tk.MustExec(fmt.Sprintf("set global innodb_ft_max_token_size=%v", oldMaxTokenSize))
	}()
	tk.MustExec("set global innodb_ft_min_token_size=1")
	tk.MustExec("set global innodb_ft_max_token_size=10")
	tk.MustExec("set session innodb_ft_enable_stopword=OFF")

	dom := domain.GetDomain(tk.Session())
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("standard_articles"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")
	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	tk.MustExec("set @@session.tidb_enable_local_match_against=ON")

	plan := compilePhysicalPlan(t, tk, "select id from standard_articles where match(body) against('+the' in boolean mode)")
	scan := findLocalMatchAgainstTableScan(t, plan)
	metadata := assertLocalMatchAgainstScalarSelection(t, tk, plan, 1)
	booleanQuery := metadata
	require.NotNil(t, booleanQuery)
	require.Equal(t, uint32(1), booleanQuery.GetInnodbFtMinTokenSize())
	require.Equal(t, uint32(10), booleanQuery.GetInnodbFtMaxTokenSize())
	require.Equal(t, tipb.LocalMatchAgainstStopwordMode_LocalMatchAgainstStopwordModeDisabled, booleanQuery.GetStopwordMode())

	pb, err := scan.ToPB(tk.Session().GetBuildPBCtx(), kv.TiFlash)
	require.NoError(t, err)
	require.NotNil(t, pb.TblScan)
}

func assertLocalMatchAgainstScalarSelection(t *testing.T, tk *testkit.TestKit, plan base.Plan, matchColumnCount int) *tipb.LocalMatchAgainstBooleanQuery {
	t.Helper()
	selection := findLocalMatchAgainstSelection(t, plan)
	pb, err := selection.ToPB(tk.Session().GetBuildPBCtx(), kv.TiFlash)
	require.NoError(t, err)
	require.NotNil(t, pb.GetSelection())
	var ftsExpr *tipb.Expr
	for _, condition := range pb.GetSelection().GetConditions() {
		if found := findScalarFunctionExpr(condition, tipb.ScalarFuncSig_LocalMatchAgainstBoolean); found != nil {
			ftsExpr = found
			break
		}
	}
	require.NotNil(t, ftsExpr, "Boolean MATCH must be encoded as its dedicated scalar function")
	require.Len(t, ftsExpr.GetChildren(), matchColumnCount+1)
	metadata := &tipb.LocalMatchAgainstBooleanQuery{}
	require.NoError(t, proto.Unmarshal(ftsExpr.GetVal(), metadata))
	require.Equal(t, fulltext.LocalMatchAgainstProtocolVersion, metadata.GetVersion())
	return metadata
}

func findLocalMatchAgainstSelection(t *testing.T, plan base.Plan) *core.PhysicalSelection {
	t.Helper()
	var result *core.PhysicalSelection
	var visit func(base.Plan)
	visit = func(plan base.Plan) {
		if result != nil || plan == nil {
			return
		}
		if selection, ok := plan.(*core.PhysicalSelection); ok {
			result = selection
			return
		}
		if reader, ok := plan.(*core.PhysicalTableReader); ok {
			for _, child := range reader.TablePlans {
				visit(child)
			}
			return
		}
		if physical, ok := plan.(base.PhysicalPlan); ok {
			for _, child := range physical.Children() {
				visit(child)
			}
		}
	}
	visit(plan)
	require.NotNil(t, result, "expected TiFlash scalar Selection for Boolean MATCH")
	return result
}

func findScalarFunctionExpr(expr *tipb.Expr, sig tipb.ScalarFuncSig) *tipb.Expr {
	if expr == nil {
		return nil
	}
	if expr.GetTp() == tipb.ExprType_ScalarFunc && expr.GetSig() == sig {
		return expr
	}
	for _, child := range expr.GetChildren() {
		if found := findScalarFunctionExpr(child, sig); found != nil {
			return found
		}
	}
	return nil
}

func countScalarFunctionExpr(expr *tipb.Expr, sig tipb.ScalarFuncSig) int {
	if expr == nil {
		return 0
	}
	count := 0
	if expr.GetTp() == tipb.ExprType_ScalarFunc && expr.GetSig() == sig {
		count++
	}
	for _, child := range expr.GetChildren() {
		count += countScalarFunctionExpr(child, sig)
	}
	return count
}

func compilePhysicalPlan(t *testing.T, tk *testkit.TestKit, sql string) base.Plan {
	t.Helper()
	ctx := context.Background()
	statements, err := tk.Session().Parse(ctx, sql)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	stmt, err := (&executor.Compiler{Ctx: tk.Session()}).Compile(ctx, statements[0])
	require.NoError(t, err)
	return stmt.Plan
}

func findLocalMatchAgainstTableScan(t *testing.T, plan base.Plan) *core.PhysicalTableScan {
	t.Helper()
	var result *core.PhysicalTableScan
	var visit func(base.Plan)
	visit = func(plan base.Plan) {
		if result != nil || plan == nil {
			return
		}
		if scan, ok := plan.(*core.PhysicalTableScan); ok {
			result = scan
			return
		}
		if reader, ok := plan.(*core.PhysicalTableReader); ok {
			for _, child := range reader.TablePlans {
				visit(child)
			}
			return
		}
		if physical, ok := plan.(base.PhysicalPlan); ok {
			for _, child := range physical.Children() {
				visit(child)
			}
		}
	}
	visit(plan)
	require.NotNil(t, result, "expected a physical table scan in the plan")
	return result
}
