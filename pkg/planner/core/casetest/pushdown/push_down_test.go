// Copyright 2023 PingCAP, Inc.
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

package pushdown

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestPushDownToTiFlashWithKeepOrder(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t(a int primary key, b varchar(20))")
		// since allow-mpp is adjusted to false, there will be no physical plan if TiFlash cop is banned.
		testKit.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

		// Create virtual tiflash replica info.
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_allow_mpp = 0")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestVirtualColumnIndexPushdown(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec(`create table t (id int,
deleted_at datetime(3) NOT NULL DEFAULT '1970-01-01 01:00:01.000',
is_deleted tinyint(1) GENERATED ALWAYS AS ((deleted_at > _utf8mb4'1970-01-01 01:00:01.000')) VIRTUAL NOT NULL,
key k(id, is_deleted))`)
		tk.MustExec(`begin`)
		tk.MustExec(`insert into t (id, deleted_at) values (1, now())`)
		tk.MustHavePlan(`select /* issue:54870 */ 1 from t where id=1 and is_deleted=true`, "IndexRangeScan")
	})
}

func TestPushDownToTiFlashWithKeepOrderInFastMode(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t(a int primary key, b varchar(20))")
		testKit.MustExec("set @@session.tiflash_fastscan=ON")
		// since allow-mpp is adjusted to false, there will be no physical plan if TiFlash cop is banned.
		testKit.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

		// Create virtual tiflash replica info.
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_allow_mpp = 0")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestPushDownProjectionForTiFlash(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t (id int, value decimal(6,3), name char(128))")
		testKit.MustExec("analyze table t")
		testKit.MustExec("set session tidb_allow_mpp=OFF")
		// since allow-mpp is adjusted to false, there will be no physical plan if TiFlash cop is banned.
		testKit.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "t")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestPushDownProjectionForTiFlashCoprocessor(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t (a int, b real, i int, id int, value decimal(6,3), name char(128), d decimal(6,3), s char(128), t datetime, c bigint as ((a+1)) virtual, e real as ((b+a)))")
		testKit.MustExec("analyze table t")
		testKit.MustExec("set session tidb_opt_projection_push_down=1")

		// Create virtual tiflash replica info.
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestSelPushDownTiFlash(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t(a int primary key, b varchar(20))")
		// since allow-mpp is adjusted to false, there will be no physical plan if TiFlash cop is banned.
		testKit.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

		// Create virtual tiflash replica info.
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_allow_mpp = 0")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestJoinNotSupportedByTiFlash(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists table_1")
		testKit.MustExec("create table table_1(id int not null, bit_col bit(2) not null, datetime_col datetime not null, index idx(id, bit_col, datetime_col))")
		testKit.MustExec("insert into table_1 values(1,b'1','2020-01-01 00:00:00'),(2,b'0','2020-01-01 00:00:00')")
		testKit.MustExec("analyze table table_1")

		testKit.MustExec("insert into mysql.expr_pushdown_blacklist values('dayofmonth', 'tiflash', '');")
		testKit.MustExec("admin reload expr_pushdown_blacklist;")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "table_1")

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_allow_mpp = 1")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}

		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_size = 1")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_count = 1")
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestVirtualColumnFilterPushedDownAsExpression(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (
  k varchar(16) not null, id int not null, typ varchar(8) not null,
  label varchar(64) not null, n int, txt varchar(255), body text,
  txt_lower text generated always as (lower(txt)) virtual,
  label_lower text generated always as (lower(label)) virtual,
  txt_lower_short varchar(3) generated always as (left(lower(txt), 3)) virtual,
  label_lower_char char(64) generated always as (lower(label)) virtual,
  n_half decimal(10,1) generated always as (n / 2) virtual,
  txt_lower_lower text generated always as (lower(txt_lower)) virtual,
  body_lower text generated always as (lower(body)) virtual,
  primary key (k, id) clustered,
  key ix_label (k, label),
  key ix_lower (k, typ, txt_lower(10)))`)
	tk.MustExec(`insert into t (k, id, typ, label, n, txt, body) values
  ('t1', 1, 'a', 'Alpha', 1, 'AcmeCorp', 'AcmeCorp'), ('t1', 2, 'a', 'beta', 2, 'acmecorp', 'acmecorp'),
  ('t1', 3, 'b', 'Gamma', 3, 'ACMECORP Mixer', 'ACMECORP Mixer'), ('t1', 4, 'b', 'delta', 4, NULL, NULL),
  ('t1', 5, 'a', 'EPSILON', 5, 'Orbital', 'Orbital'), ('t2', 6, 'a', 'zeta', 6, 'AcmeCorp', 'AcmeCorp'),
  ('t1', 7, 'a', 'Eta ', 7, 'ÄBC acmecorp', 'ÄBC acmecorp'), ('t1', 8, 'b', 'theta', 8, '', '')`)
	testkit.SetTiFlashReplica(t, dom, "test", "t")

	// Filters on these virtual columns are value-preserving rewrites of their
	// generation expressions and must be evaluated in the storage engine.
	pushed := []struct {
		pred, expr string
		ids        []string
	}{
		{"txt_lower = 'acmecorp'", "eq(lower(test.t.txt), \"acmecorp\")", []string{"1", "2"}},
		{"txt_lower like '%acmecorp%'", "like(lower(test.t.txt), \"%acmecorp%\", 92)", []string{"1", "2", "3", "7"}},
		{"label_lower in ('alpha', 'eta ')", "in(lower(test.t.label), \"alpha\", \"eta \")", []string{"1", "7"}},
		{"txt_lower is null", "isnull(lower(test.t.txt))", []string{"4"}},
	}
	// The column type changes the value of the expression (truncation, CHAR
	// padding, decimal rounding), or can for some values (lower() can grow a TEXT
	// value past TEXT's byte limit), so the filter must stay on the virtual column.
	notPushed := []struct {
		pred string
		ids  []string
	}{
		{"txt_lower_short = 'acm'", []string{"1", "2", "3"}},
		{"body_lower = 'acmecorp'", []string{"1", "2"}},
		{"txt_lower_lower = 'acmecorp'", []string{"1", "2"}},
		{"label_lower_char = 'eta'", []string{"7"}},
		{"n_half = 2.5", []string{"5"}},
	}
	hints := []string{
		"/*+ use_index(t, primary) */",
		"/*+ use_index(t, ix_lower) */",
		"/*+ read_from_storage(tiflash[t]) */",
	}
	query := func(hint, pred string) string {
		return "select " + hint + " id from t where k = 't1' and typ in ('a', 'b') and " + pred + " order by label, id limit 100"
	}
	planContains := func(sql, s string) (inStorage, atRoot bool) {
		for _, row := range tk.MustQuery("explain format='brief' " + sql).Rows() {
			info, task := row[4].(string), row[2].(string)
			if !strings.Contains(info, s) {
				continue
			}
			if task == "root" {
				atRoot = true
			} else {
				inStorage = true
			}
		}
		return
	}
	for _, hint := range hints {
		for _, c := range pushed {
			sql := query(hint, c.pred)
			inStorage, atRoot := planContains(sql, c.expr)
			// On ix_lower, `txt_lower is null` is fully answered by the index range.
			if !(strings.Contains(hint, "ix_lower") && strings.HasSuffix(c.pred, "is null")) {
				require.True(t, inStorage, "%s: expected %s in a storage-side Selection", sql, c.expr)
			}
			require.False(t, atRoot, sql)
			_, vcolAtRoot := planContains(sql, strings.SplitN(c.pred, " ", 2)[0]+",")
			require.False(t, vcolAtRoot, sql)
			// The mock store cannot run TiFlash plans; TestVirtualColumnFilterOnTiFlash
			// checks those results.
			if !strings.Contains(hint, "tiflash") {
				tk.MustQuery(sql).Sort().Check(testkit.Rows(c.ids...))
			}
		}
		for _, c := range notPushed {
			sql := query(hint, c.pred)
			col := strings.SplitN(c.pred, " ", 2)[0]
			inStorage, _ := planContains(sql, "test.t."+col)
			_, atRoot := planContains(sql, "test.t."+col)
			require.False(t, inStorage, sql)
			require.True(t, atRoot, sql)
			if !strings.Contains(hint, "tiflash") {
				tk.MustQuery(sql).Sort().Check(testkit.Rows(c.ids...))
			}
		}
	}
}

func TestGeneratedColumnIndexForValuePreservingExpr(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (k varchar(16) not null, id int not null, typ varchar(8) not null, txt text, label varchar(64) not null,
  v varchar(255), v_lower text generated always as (lower(v)) virtual,
  txt_lower text generated always as (lower(txt)) virtual,
  label_lower varchar(64) generated always as (lower(label)) virtual,
  txt_lower_short varchar(2) generated always as (lower(txt)) virtual,
  txt_lower_nn text generated always as (lower(txt)) virtual not null,
  s varchar(100), s_lower_tiny tinytext generated always as (lower(s)) virtual,
  s10 varchar(10), s10_lower_tiny tinytext generated always as (lower(s10)) virtual,
  primary key (k, id) clustered,
  key ix_txt (k, typ, txt_lower(10)),
  key ix_v (k, typ, v_lower(10)),
  key ix_label (k, label_lower),
  key ix_short (k, txt_lower_short),
  key ix_nn (k, txt_lower_nn(10)),
  key ix_tiny (k, s_lower_tiny(20)),
  key ix_tiny10 (k, s10_lower_tiny(20)))`)
	tk.MustExec("set @@sql_mode = ''")
	tk.MustExec(`insert into t (k, id, typ, txt, label, v) values ('t1', 1, 'a', 'AcmeCorp', 'Alpha', 'AcmeCorp'), ('t1', 2, 'a', 'Beijing', 'BETA', 'Beijing'),
  ('t1', 3, 'b', 'acmecorp', 'alpha', 'acmecorp'), ('t1', 4, 'a', NULL, 'Gamma', NULL)`)
	// lower() grows the last character from 2 bytes to 3, past TEXT's limit:
	// txt_lower drops it and reads 'x' plus trailing spaces (see #73782).
	tk.MustExec(`insert into t (k, id, typ, txt, label) values ('t1', 6, 'a', concat('x', repeat(' ', 65532), _utf8mb4 0xC8BA), 'Zeta')`)
	// 100 four-byte characters are 400 bytes: s_lower_tiny (TINYTEXT, 255 bytes)
	// holds a truncated value.
	tk.MustExec(`insert into t (k, id, typ, txt, label, s, s10) values ('t1', 5, 'c', 'x', 'Delta', repeat(_utf8mb4 0xF09F9880, 100), 'Ab')`)
	tk.MustExec("set @@sql_mode = default")
	require.Equal(t, "0", tk.MustQuery("select @@tidb_enable_unsafe_substitute").Rows()[0][0])

	rangeOf := func(sql string) string {
		for _, row := range tk.MustQuery("explain format='brief' " + sql).Rows() {
			if strings.Contains(row[0].(string), "IndexRangeScan") {
				return row[3].(string) + " " + row[4].(string)
			}
		}
		return ""
	}
	// lower(varchar(255)) is var_string(255) and v_lower is TEXT: not the same
	// type, but TEXT holds any value of the expression, so the index is usable.
	sql := "select /*+ use_index(t, ix_v) */ id from t where k = 't1' and typ = 'a' and lower(v) = 'acmecorp'"
	require.Contains(t, rangeOf(sql), `index:ix_v(k, typ, v_lower) range:["t1" "a" "acmecorp","t1" "a" "acmecorp"]`)
	tk.MustQuery(sql).Check(testkit.Rows("1"))
	// lower(text) can be longer in bytes than TEXT holds, so txt_lower and
	// lower(txt) differ for id 6, and ix_txt must not answer lower(txt).
	sql = "select /*+ use_index(t, ix_txt) */ id from t where k = 't1' and typ = 'a' and lower(txt) = 'x'"
	require.NotContains(t, rangeOf(sql), `"x"`)
	tk.MustQuery(sql).Check(testkit.Rows())
	tk.MustQuery("select id from t where k = 't1' and typ = 'a' and txt_lower = 'x'").Check(testkit.Rows("6"))
	// varchar(64) column for lower(varchar(64)): already the same type.
	sql = "select /*+ use_index(t, ix_label) */ id from t where k = 't1' and lower(label) = 'alpha'"
	require.Contains(t, rangeOf(sql), `range:["t1" "alpha","t1" "alpha"]`)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1", "3"))
	// varchar(2) holds a truncated value ('Beijing' -> 'be' under the non-strict
	// insert above), so lower(txt) must not be answered from ix_short.
	sql = "select /*+ use_index(t, ix_short) */ id from t where k = 't1' and lower(txt) = 'beijing'"
	require.NotContains(t, rangeOf(sql), `"beijing"`)
	tk.MustQuery(sql).Check(testkit.Rows("2"))
	tk.MustQuery("select id from t where k = 't1' and txt_lower_short = 'be'").Check(testkit.Rows("2"))
	// txt_lower_nn is NOT NULL, so the NULL txt of id 4 is stored as '' (non-strict
	// insert above). lower(txt) is NULL there: the column and the expression are
	// not interchangeable in either direction.
	sql = "select /*+ use_index(t, ix_nn) */ id from t where k = 't1' and lower(txt) is null"
	require.NotContains(t, rangeOf(sql), "NULL")
	tk.MustQuery(sql).Check(testkit.Rows("4"))
	tk.MustQuery("select /*+ use_index(t, primary) */ id from t where k = 't1' and txt_lower_nn = ''").Check(testkit.Rows("4"))
	tk.MustQuery("select /*+ use_index(t, primary) */ id from t where k = 't1' and txt_lower_nn is null").Check(testkit.Rows())
	// lower(varchar(100)) is up to 400 bytes in utf8mb4, more than TINYTEXT holds,
	// so ix_tiny must not answer it; lower(varchar(10)) fits in 40 bytes.
	sql = "select /*+ use_index(t, ix_tiny) */ id from t where k = 't1' and lower(s) = repeat(_utf8mb4 0xF09F9880, 100)"
	require.NotContains(t, rangeOf(sql), "ix_tiny(k, s_lower_tiny) range:[\"t1\" ")
	tk.MustQuery(sql).Check(testkit.Rows("5"))
	sql = "select /*+ use_index(t, ix_tiny10) */ id from t where k = 't1' and lower(s10) = 'ab'"
	require.Contains(t, rangeOf(sql), `index:ix_tiny10(k, s10_lower_tiny) range:["t1" "ab","t1" "ab"]`)
	tk.MustQuery(sql).Check(testkit.Rows("5"))
}
