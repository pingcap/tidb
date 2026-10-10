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

// Package ftse2e tests the real TiDB -> TiFlash Boolean MATCH execution path.
// It is opt-in because it requires a running TiUP cluster with a TiFlash node.
package ftse2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

const replicaTimeout = 4 * time.Minute

type fixture struct {
	t      *testing.T
	db     *sql.DB
	admin  *sql.Conn
	native *sql.Conn
	local  *sql.Conn
	schema string
}

func TestBooleanMatchTiFlashE2E(t *testing.T) {
	dsn := os.Getenv("TIDB_FTS_E2E_DSN")
	if dsn == "" {
		t.Skip("set TIDB_FTS_E2E_DSN to run against a TiUP cluster with TiFlash")
	}
	f := newFixture(t, dsn)
	t.Run("standard", f.testStandard)
	t.Run("ngram", f.testNgram)
	t.Run("collations", f.testCollations)
	t.Run("review_regressions", f.testReviewRegressions)
	t.Run("token_semantics", f.testTokenSemantics)
}

func TestLocalMatchRandomDifferentialTiFlashE2E(t *testing.T) {
	dsn := os.Getenv("TIDB_FTS_E2E_DSN")
	if dsn == "" {
		t.Skip("set TIDB_FTS_E2E_DSN to run the deterministic differential matrix")
	}
	f := newFixture(t, dsn)
	rng := rand.New(rand.NewPCG(70484, 70485))
	words := []string{"foo", "bar", "baz", "cafe", "CAFE", "café", "the", "The", "tidb", "TiDB", "数据", "𞤀bar", "foo_bar", "𝟙𝟚𝟛", "a", "an", "on"}
	separators := []string{" ", ".", ",", "🙃", "👁", "-", "/", "\u0301"}
	rows := []string{"VALUES (1, NULL, NULL, NULL, NULL, NULL)", "VALUES (2, '', '', '', '', '')"}
	for id := 3; id <= 66; id++ {
		var text strings.Builder
		for j, n := 0, 2+rng.IntN(20); j < n; j++ {
			if j > 0 {
				text.WriteString(separators[rng.IntN(len(separators))])
			}
			text.WriteString(words[rng.IntN(len(words))])
		}
		body := strings.ReplaceAll(text.String(), "'", "''")
		rows = append(rows, fmt.Sprintf("VALUES (%d, '%s', '%s', '%s', '%s', '%s')", id, body, body, body, body, body))
	}
	collations := []string{"utf8mb4_bin", "utf8mb4_0900_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci", "utf8mb4_0900_ai_ci"}
	queries := []string{"+foo foo.bar", "+foo -foo.bar", "+foo +bar", `+foo +"foo bar"`, "+foo -the", "+foo +caf*"}
	for len(queries) < 24 {
		query := "+foo"
		for j, n := 0, 1+rng.IntN(4); j < n; j++ {
			modifier := []string{"", "+", "-"}[rng.IntN(3)]
			term := words[rng.IntN(len(words))]
			switch rng.IntN(4) {
			case 0:
				term += "." + words[rng.IntN(len(words))]
			case 1:
				term = `"` + term + " " + words[rng.IntN(len(words))] + `"`
			case 2:
				term += "*"
			}
			query += " " + modifier + term
		}
		queries = append(queries, query)
	}
	for _, parser := range []string{"standard", "ngram"} {
		var ddl strings.Builder
		ddl.WriteString("CREATE TABLE %s (id INT PRIMARY KEY")
		for i, collation := range collations {
			fmt.Fprintf(&ddl, ", c%d TEXT COLLATE %s, FULLTEXT INDEX ft%d(c%d)", i, collation, i, i)
			if parser == "ngram" {
				ddl.WriteString(" WITH PARSER NGRAM")
			}
		}
		ddl.WriteString(")")
		family := "random_" + parser
		f.makePair(family, ddl.String(), rows)
		for i, collation := range collations {
			for _, stopword := range []string{"ON", "OFF"} {
				for _, conn := range []*sql.Conn{f.native, f.local} {
					f.exec(conn, "SET SESSION innodb_ft_enable_stopword="+stopword)
					f.exec(conn, "SET SESSION collation_server='"+collation+"'")
				}
				for j, search := range queries {
					t.Run(fmt.Sprintf("%s/%s/stopword_%s/query_%02d", parser, collation, stopword, j), func(t *testing.T) {
						predicate := fmt.Sprintf("MATCH(c%d) AGAINST('%s' IN BOOLEAN MODE)", i, search)
						nativeSQL := "SELECT id FROM " + family + "_native WHERE " + predicate
						localSQL := "SELECT id FROM " + family + "_local WHERE " + predicate
						assertPlan(t, f.native, nativeSQL, true)
						assertPlan(t, f.local, localSQL, false)
						a, b := queryIDs(t, f.native, nativeSQL), queryIDs(t, f.local, localSQL)
						if !reflect.DeepEqual(a, b) {
							t.Fatalf("seed=70484/70485 query=%q TiFlash=%v TiDB=%v", search, a, b)
						}
					})
				}
			}
		}
	}
}

func TestLocalMatchSnapshotTiFlashE2E(t *testing.T) {
	dsn := os.Getenv("TIDB_FTS_E2E_DSN")
	if dsn == "" {
		t.Skip("set TIDB_FTS_E2E_DSN to run snapshot isolation checks")
	}
	f := newFixture(t, dsn)
	f.makePair("snapshot_docs", `CREATE TABLE %s (id INT PRIMARY KEY, body TEXT, FULLTEXT INDEX ft(body))`,
		[]string{"VALUES (1, 'foo')", "VALUES (2, 'bar')"})
	query := func(table string) string {
		return "SELECT id FROM " + table + " WHERE MATCH(body) AGAINST('+foo' IN BOOLEAN MODE)"
	}
	for _, conn := range []*sql.Conn{f.native, f.local} {
		f.exec(conn, "BEGIN")
		defer func() { _, _ = conn.ExecContext(context.Background(), "ROLLBACK") }()
	}
	assertPlan(t, f.native, query("snapshot_docs_native"), true)
	assertPlan(t, f.local, query("snapshot_docs_local"), false)
	for _, path := range []struct {
		conn  *sql.Conn
		table string
	}{{f.native, "snapshot_docs_native"}, {f.local, "snapshot_docs_local"}} {
		if got := queryIDs(t, path.conn, query(path.table)); !reflect.DeepEqual(got, []int{1}) {
			t.Fatalf("initial snapshot %s: %v", path.table, got)
		}
	}
	var snapshot string
	must(t, f.native.QueryRowContext(context.Background(), "SELECT @@tidb_current_ts").Scan(&snapshot))
	latestNative, latestLocal := f.connect(), f.connect()
	for _, path := range []struct {
		conn   *sql.Conn
		engine string
	}{{latestNative, "tiflash"}, {latestLocal, "tikv"}} {
		f.exec(path.conn, "SET SESSION tidb_enable_local_match_against=ON")
		f.exec(path.conn, "SET SESSION tidb_allow_tiflash_cop=ON")
		f.exec(path.conn, "SET SESSION tidb_isolation_read_engines='"+path.engine+"'")
	}
	// Both read transactions stay open while a different connection commits
	// atomic changes to both tables. Hand-stepped commits give exact expected
	// results without racing two unrelated latest-read timestamps.
	for step := 1; step <= 7; step++ {
		f.exec(f.admin, "BEGIN")
		for _, table := range []string{"snapshot_docs_native", "snapshot_docs_local"} {
			f.exec(f.admin, "DELETE FROM "+table+" WHERE id=3")
			if step%2 == 1 {
				f.exec(f.admin, "UPDATE "+table+" SET body=IF(id=1,'bar','foo')")
				f.exec(f.admin, "INSERT INTO "+table+" VALUES (3,'foo')")
			} else {
				f.exec(f.admin, "UPDATE "+table+" SET body=IF(id=1,'foo','bar')")
			}
		}
		f.exec(f.admin, "COMMIT")
		for _, path := range []struct {
			conn  *sql.Conn
			table string
		}{{f.native, "snapshot_docs_native"}, {f.local, "snapshot_docs_local"}} {
			if got := queryIDs(t, path.conn, query(path.table)); !reflect.DeepEqual(got, []int{1}) {
				t.Fatalf("step %d repeatable read %s: %v", step, path.table, got)
			}
		}
		want := []int{1}
		if step%2 == 1 {
			want = []int{2, 3}
		}
		for _, path := range []struct {
			conn  *sql.Conn
			table string
		}{{latestNative, "snapshot_docs_native"}, {latestLocal, "snapshot_docs_local"}} {
			if got := queryIDs(t, path.conn, query(path.table)); !reflect.DeepEqual(got, want) {
				t.Fatalf("step %d latest read %s: want %v got %v", step, path.table, want, got)
			}
		}
	}
	for _, path := range []struct {
		conn  *sql.Conn
		table string
	}{{f.native, "snapshot_docs_native"}, {f.local, "snapshot_docs_local"}} {
		f.exec(path.conn, "ROLLBACK")
		f.exec(path.conn, "SET SESSION tidb_snapshot='"+snapshot+"'")
		defer func() { _, _ = path.conn.ExecContext(context.Background(), "SET SESSION tidb_snapshot=''") }()
		if got := queryIDs(t, path.conn, query(path.table)); !reflect.DeepEqual(got, []int{1}) {
			t.Fatalf("historical snapshot %s: %v", path.table, got)
		}
	}
}

func TestLocalMatchLargeDocumentsTiFlashE2E(t *testing.T) {
	dsn := os.Getenv("TIDB_FTS_E2E_DSN")
	if dsn == "" {
		t.Skip("set TIDB_FTS_E2E_DSN to run bounded large-document checks")
	}
	f := newFixture(t, dsn)
	documents := []string{strings.Repeat("foo bar 数据 café ", 8192), strings.Repeat("bar baz 科学 CAFE ", 8192),
		strings.Repeat("baz_qux ", 32768), strings.Repeat("foo_bar ", 32768), strings.Repeat("foo ", 32768) + "foo𞤀bar"}
	rows := []string{"VALUES (6, NULL)"}
	totalBytes := 0
	for i, body := range documents {
		totalBytes += len(body)
		rows = append(rows, fmt.Sprintf("VALUES (%d, '%s')", i+1, body))
	}
	f.makePair("large_docs", `CREATE TABLE %s (id INT PRIMARY KEY, body MEDIUMTEXT COLLATE utf8mb4_bin, FULLTEXT INDEX ft(body))`, rows)
	for i, tc := range []struct {
		search string
		want   []int
	}{{"+foo", []int{1, 5}}, {"+数据", []int{}}, {`+"foo bar"`, []int{1, 5}}, {"+foo*", []int{1, 4, 5}},
		{"foo.bar", []int{1, 2, 5}}, {"+foo -bar", []int{}}, {"+foo" + strings.Repeat(" bar", 256), []int{1, 5}}} {
		t.Run(fmt.Sprintf("query_%d", i), func(t *testing.T) {
			for _, path := range []struct {
				conn   *sql.Conn
				table  string
				native bool
			}{{f.native, "large_docs_native", true}, {f.local, "large_docs_local", false}} {
				query := "SELECT id FROM " + path.table + " WHERE MATCH(body) AGAINST('" + tc.search + "' IN BOOLEAN MODE)"
				assertPlan(t, path.conn, query, path.native)
				start := time.Now()
				got := queryIDs(t, path.conn, query)
				t.Logf("%s: document_bytes=%d query_bytes=%d elapsed=%s", path.table, totalBytes, len(tc.search), time.Since(start))
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("%s: want %v got %v", path.table, tc.want, got)
				}
			}
		})
	}
}

// Run this separately against a freshly bootstrapped old-collation cluster.
// Toggling a SQL session variable cannot change the cluster's collation mode.
func TestLegacyCollationLocalMatchTiFlashE2E(t *testing.T) {
	dsn := os.Getenv("TIDB_FTS_LEGACY_E2E_DSN")
	if dsn == "" {
		t.Skip("set TIDB_FTS_LEGACY_E2E_DSN to test an old-collation TiUP cluster")
	}
	f := newFixture(t, dsn)
	var mode string
	must(t, f.db.QueryRowContext(context.Background(), "SELECT variable_value FROM mysql.tidb WHERE variable_name='new_collation_enabled'").Scan(&mode))
	if mode != "False" {
		t.Fatalf("requires new_collation_enabled=False, got %q", mode)
	}
	f.makePair("legacy_docs", `CREATE TABLE %s (id INT PRIMARY KEY, body TEXT COLLATE utf8mb4_general_ci,
		FULLTEXT INDEX ft_body(body))`, []string{"VALUES (1, 'TiDB storage'), (2, 'tidb storage'), (3, 'The'), (4, 'the')"})
	for _, conn := range []*sql.Conn{f.native, f.local} {
		f.exec(conn, "SET SESSION collation_server='utf8mb4_general_ci'")
	}
	for _, tc := range []struct {
		search string
		want   []int
	}{{"+tidb", []int{2}}, {"+TiDB", []int{1}}, {"+the", []int{}}, {"+The", []int{3}}} {
		t.Run(tc.search, func(t *testing.T) {
			for _, path := range []struct {
				conn  *sql.Conn
				table string
			}{{f.native, "legacy_docs_native"}, {f.local, "legacy_docs_local"}} {
				query := "SELECT id FROM " + path.table + " WHERE MATCH(body) AGAINST(? IN BOOLEAN MODE)"
				assertRootMatchPlan(t, path.conn, query, tc.search)
				got := queryIDs(t, path.conn, query, tc.search)
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("%s: want %v, got %v", path.table, tc.want, got)
				}
			}
		})
	}
}

func openLocalDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	config, err := mysql.ParseDSN(dsn)
	must(t, err)
	if config.Net == "tcp" {
		host, _, err := net.SplitHostPort(config.Addr)
		must(t, err)
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			t.Fatalf("FTS E2E creates and drops a schema; only loopback TiUP connections are allowed, got %q", host)
		}
	} else if config.Net != "unix" {
		t.Fatalf("FTS E2E requires a local TCP or Unix-socket connection, got %q", config.Net)
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	must(t, err)
	t.Cleanup(func() { _ = db.Close() })
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	must(t, db.PingContext(pingCtx))
	return db
}

func newFixture(t *testing.T, dsn string) *fixture {
	t.Helper()
	db := openLocalDB(t, dsn)
	ctx := context.Background()
	var version string
	must(t, db.QueryRowContext(ctx, "SELECT tidb_version()").Scan(&version))
	t.Logf("TiDB: %s", version)

	// Never operate on an existing schema. The generated name is used only by
	// this test and is the only schema removed during cleanup.
	schema := fmt.Sprintf("fts_e2e_%d", time.Now().UnixNano())
	_, err := db.ExecContext(ctx, "CREATE DATABASE `"+schema+"`")
	must(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if _, err := db.ExecContext(cleanupCtx, "DROP DATABASE `"+schema+"`"); err != nil {
			t.Errorf("clean up test schema %s: %v", schema, err)
		}
	})

	f := &fixture{t: t, db: db, schema: schema}
	f.admin = f.connect()
	f.native = f.connect()
	f.local = f.connect()
	// The native path is preferred even when local fallback is enabled. Keeping
	// it enabled also makes unsupported index shapes report the matching-index
	// error instead of the fallback-disabled error.
	f.exec(f.native, "SET SESSION tidb_enable_local_match_against=ON")
	f.exec(f.native, "SET SESSION tidb_allow_tiflash_cop=ON")
	f.exec(f.native, "SET SESSION tidb_isolation_read_engines='tiflash'")
	f.exec(f.local, "SET SESSION tidb_enable_local_match_against=ON")
	f.exec(f.local, "SET SESSION tidb_isolation_read_engines='tikv'")
	return f
}

func (f *fixture) connect() *sql.Conn {
	f.t.Helper()
	conn, err := f.db.Conn(context.Background())
	must(f.t, err)
	f.t.Cleanup(func() { _ = conn.Close() })
	f.exec(conn, "USE `"+f.schema+"`")
	return conn
}

func (f *fixture) exec(conn *sql.Conn, query string, args ...any) {
	f.t.Helper()
	_, err := conn.ExecContext(context.Background(), query, args...)
	if err != nil {
		f.t.Fatalf("%s: %v", query, err)
	}
}

func (f *fixture) makePair(family, ddl string, rows []string) {
	f.t.Helper()
	for _, suffix := range []string{"native", "local"} {
		table := family + "_" + suffix
		f.exec(f.admin, fmt.Sprintf(ddl, "`"+table+"`"))
		for _, row := range rows {
			f.exec(f.admin, "INSERT INTO `"+table+"` "+row)
		}
	}
	nativeTable := family + "_native"
	f.exec(f.admin, "ALTER TABLE `"+nativeTable+"` SET TIFLASH REPLICA 1")
	f.waitForReplica(nativeTable)
	var localReplicaRows int
	must(f.t, f.db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM information_schema.tiflash_replica WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
		f.schema, family+"_local").Scan(&localReplicaRows))
	if localReplicaRows != 0 {
		f.t.Fatalf("%s_local unexpectedly has a TiFlash replica", family)
	}
}

func (f *fixture) waitForReplica(table string) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), replicaTimeout)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		var available int
		err := f.db.QueryRowContext(ctx,
			"SELECT AVAILABLE FROM information_schema.tiflash_replica WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
			f.schema, table).Scan(&available)
		if err == nil && available == 1 {
			return
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			f.t.Fatalf("check TiFlash replica for %s: %v", table, err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			f.t.Fatalf("TiFlash replica for %s did not become available within %s", table, replicaTimeout)
		}
	}
}

type matchCase struct {
	name      string
	predicate string
	want      []int
}

func (f *fixture) check(tableFamily string, tc matchCase) {
	f.t.Helper()
	f.t.Run(tc.name, func(t *testing.T) {
		nativeSQL := fmt.Sprintf("SELECT id FROM `%s_native` WHERE %s", tableFamily, tc.predicate)
		localSQL := fmt.Sprintf("SELECT id FROM `%s_local` WHERE %s", tableFamily, tc.predicate)
		assertPlan(t, f.native, nativeSQL, true)
		assertPlan(t, f.local, localSQL, false)
		gotNative := queryIDs(t, f.native, nativeSQL)
		gotLocal := queryIDs(t, f.local, localSQL)
		if !reflect.DeepEqual(gotNative, tc.want) || !reflect.DeepEqual(gotLocal, tc.want) {
			t.Fatalf("%s: want %v, TiFlash %v, TiDB fallback %v", tc.predicate, tc.want, gotNative, gotLocal)
		}
	})
}

func assertPlan(t *testing.T, conn *sql.Conn, query string, native bool, args ...any) {
	t.Helper()
	text := explainPlan(t, conn, query, args...)
	assertPlanText(t, query, text, native, false)
}

func assertPlanText(t *testing.T, query, text string, native, allowCop bool) {
	t.Helper()
	if err := validateMatchPlan(text, native, allowCop); err != nil {
		t.Fatalf("%s for %s:\n%s", err, query, text)
	}
}

// Both brief EXPLAIN and statement-summary plans put the operator and task
// before the operator info. Read those columns rather than searching predicates
// for words such as "root" or "selection". Unrelated residual filters are valid.
func matchPlanOperator(line string) (operator, task string) {
	fields := strings.Fields(strings.ToLower(line))
	if len(fields) == 0 {
		return "", ""
	}
	operator = strings.TrimLeft(fields[0], "│├└─")
	for _, field := range fields[1:min(len(fields), 4)] {
		if field == "root" || strings.HasPrefix(field, "cop[") || strings.HasPrefix(field, "mpp[") {
			task = field
			break
		}
	}
	return operator, task
}

func validateMatchPlan(text string, native, allowCop bool) error {
	text = strings.ToLower(text)
	isTiFlash := func(task string) bool {
		return task == "mpp[tiflash]" || (allowCop && task == "cop[tiflash]")
	}
	scan, match := false, false
	for _, line := range strings.Split(text, "\n") {
		operator, task := matchPlanOperator(line)
		if task == "cop[tici]" {
			return fmt.Errorf("unexpected TiCI plan")
		}
		if isTiFlash(task) && (strings.HasPrefix(operator, "tablefullscan") || strings.HasPrefix(operator, "tablerangescan")) {
			scan = true
		}
		if !strings.Contains(line, "match_against(") {
			continue
		}
		if !strings.HasPrefix(operator, "selection") {
			return fmt.Errorf("expected MATCH in a Selection, got %s", operator)
		}
		if native && !isTiFlash(task) {
			return fmt.Errorf("MATCH is not in a TiFlash Selection (task %s)", task)
		}
		if !native && task != "root" {
			return fmt.Errorf("expected TiDB root MATCH fallback, got task %s", task)
		}
		match = true
	}
	if !match {
		return fmt.Errorf("missing MATCH Selection")
	}
	if native && !scan {
		return fmt.Errorf("expected TiFlash TableFullScan or TableRangeScan")
	}
	return nil
}

func explainPlan(t *testing.T, conn *sql.Conn, query string, args ...any) string {
	t.Helper()
	return queryPlan(t, conn, "EXPLAIN FORMAT='brief' "+query, args...)
}

func queryPlan(t *testing.T, conn *sql.Conn, query string, args ...any) string {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), query, args...)
	must(t, err)
	defer rows.Close()
	cols, err := rows.Columns()
	must(t, err)
	var plan strings.Builder
	for rows.Next() {
		values := make([]sql.RawBytes, len(cols))
		dest := make([]any, len(cols))
		for i := range values {
			dest[i] = &values[i]
		}
		must(t, rows.Scan(dest...))
		for _, value := range values {
			plan.Write(value)
			plan.WriteByte(' ')
		}
		plan.WriteByte('\n')
	}
	must(t, rows.Err())
	return strings.ToLower(plan.String())
}

func assertRootMatchPlan(t *testing.T, conn *sql.Conn, query string, args ...any) {
	t.Helper()
	plan := explainPlan(t, conn, query, args...)
	found := false
	for _, line := range strings.Split(plan, "\n") {
		if strings.Contains(line, "match_against") {
			if !strings.Contains(line, "selection") || !strings.Contains(line, "root") {
				t.Fatalf("unsafe MATCH was pushed down:\n%s", plan)
			}
			found = true
		}
	}
	if !found || strings.Contains(plan, "cop[tici]") {
		t.Fatalf("expected TiDB-side Local MATCH Selection:\n%s", plan)
	}
}

func queryIDs(t *testing.T, conn *sql.Conn, query string, args ...any) []int {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), query, args...)
	must(t, err)
	return readIDs(t, rows)
}

func readIDs(t *testing.T, rows *sql.Rows) []int {
	t.Helper()
	defer rows.Close()
	ids := make([]int, 0)
	for rows.Next() {
		var id int
		must(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	must(t, rows.Err())
	sort.Ints(ids)
	return ids
}

func (f *fixture) testReviewRegressions(t *testing.T) {
	f.t = t
	f.makePair("review_docs", `CREATE TABLE %s (id INT PRIMARY KEY, body TEXT COLLATE utf8mb4_general_ci,
		FULLTEXT INDEX ft_body(body))`, []string{
		"VALUES (1, '123'), (2, '456'), (3, 'foo barista'), (4, 'foo only'), (5, 'barista only'), (6, 'baz foo'), (7, 'baz barista'), (8, 'baz other')",
	})
	nativeSQL := "SELECT id FROM review_docs_native WHERE MATCH(body) AGAINST(? IN BOOLEAN MODE)"
	localSQL := "SELECT id FROM review_docs_local WHERE MATCH(body) AGAINST(? IN BOOLEAN MODE)"
	var summaryEnabled int
	must(t, f.admin.QueryRowContext(context.Background(), "SELECT @@global.tidb_enable_stmt_summary").Scan(&summaryEnabled))
	if summaryEnabled != 1 {
		t.Fatal("prepared-statement plan verification requires tidb_enable_stmt_summary=ON in the test cluster")
	}
	nativeStmt, err := f.native.PrepareContext(context.Background(), nativeSQL)
	must(t, err)
	defer nativeStmt.Close()
	localStmt, err := f.local.PrepareContext(context.Background(), localSQL)
	must(t, err)
	defer localStmt.Close()
	for _, tc := range []struct {
		name  string
		value any
		want  []int
	}{
		{"numeric_123", int64(123), []int{1}},
		{"numeric_456", int64(456), []int{2}},
		{"string_123", "123", []int{1}},
		{"null", nil, []int{}},
		{"numeric_after_null", int64(456), []int{2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, path := range []struct {
				stmt   *sql.Stmt
				table  string
				native bool
			}{{nativeStmt, "review_docs_native", true}, {localStmt, "review_docs_local", false}} {
				rows, err := path.stmt.QueryContext(context.Background(), tc.value)
				must(t, err)
				got := readIDs(t, rows)
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("parameter %v: want %v, got %v", tc.value, tc.want, got)
				}
				if tc.value != nil {
					// This branch's EXPLAIN FOR CONNECTION can retain a stale
					// plan after binary EXECUTE. Statement summary records the
					// executed plan, keyed by SQL and plan digest. Check every
					// sampled non-NULL MATCH plan, allowing both TiFlash engines.
					plans, err := f.admin.QueryContext(context.Background(),
						"SELECT PLAN FROM information_schema.statements_summary WHERE SCHEMA_NAME=? AND DIGEST_TEXT LIKE ? AND PLAN LIKE '%match_against%'",
						f.schema, "select %from `"+path.table+"` %")
					must(t, err)
					count := 0
					for plans.Next() {
						var plan string
						must(t, plans.Scan(&plan))
						assertPlanText(t, path.table, strings.ToLower(plan), path.native, true)
						count++
					}
					must(t, plans.Err())
					must(t, plans.Close())
					if count == 0 {
						t.Fatalf("missing executed MATCH plan for %s", path.table)
					}
				}
			}
		})
	}
	for _, tc := range []struct {
		search string
		want   []int
	}{
		{"+foo.bar*", []int{3}},
		{"foo.bar*", []int{3, 4, 5, 6, 7}},
		{"baz -foo.bar*", []int{8}},
	} {
		t.Run(tc.search, func(t *testing.T) {
			// A replica may still supply the scan, but the unsafe scalar must
			// remain in a root Selection and use TiDB's evaluator.
			assertRootMatchPlan(t, f.native, nativeSQL, tc.search)
			for _, path := range []struct {
				conn *sql.Conn
				sql  string
			}{{f.native, nativeSQL}, {f.local, localSQL}} {
				got := queryIDs(t, path.conn, path.sql, tc.search)
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("%s: want %v, got %v", tc.search, tc.want, got)
				}
			}
		})
	}
}

func assertMySQLError(t *testing.T, conn *sql.Conn, query string, wantCode uint16) {
	t.Helper()
	var id int
	err := conn.QueryRowContext(context.Background(), query).Scan(&id)
	var sqlErr *mysql.MySQLError
	if !errors.As(err, &sqlErr) || sqlErr.Number != wantCode {
		t.Fatalf("%s: expected MySQL error %d, got %v", query, wantCode, err)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) testTokenSemantics(t *testing.T) {
	f.t = t
	rows := []string{
		"VALUES (1, 'foo only')", "VALUES (2, 'bar only')", "VALUES (3, 'foo bar')",
		"VALUES (4, 'baz foo')", "VALUES (5, 'baz bar')", "VALUES (6, 'baz qux')",
		"VALUES (7, 'foo🙃bar')", "VALUES (8, 'foo👁bar')", "VALUES (9, 'foo𞤀bar')",
		"VALUES (10, 'foo𝟙bar')", "VALUES (11, '𞤀bar')", "VALUES (12, 'foobar')",
		"VALUES (13, 'foo\U0002EBF0bar')",
	}
	f.makePair("token_standard", `CREATE TABLE %s (id INT PRIMARY KEY, body TEXT COLLATE utf8mb4_bin,
		FULLTEXT INDEX ft_body(body))`, rows)
	for _, tc := range []matchCase{
		{"split_optional", "MATCH(body) AGAINST('foo.bar' IN BOOLEAN MODE)", []int{1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 13}},
		{"split_required", "MATCH(body) AGAINST('+foo.bar' IN BOOLEAN MODE)", []int{3, 7, 8, 9, 10, 13}},
		{"split_excluded", "MATCH(body) AGAINST('baz -foo.bar' IN BOOLEAN MODE)", []int{6}},
		{"emoji_delimiter", "MATCH(body) AGAINST('+foo' IN BOOLEAN MODE)", []int{1, 3, 4, 7, 8, 9, 10, 13}},
		{"emoji_in_query", "MATCH(body) AGAINST('+foo🙃bar' IN BOOLEAN MODE)", []int{3, 7, 8, 9, 10, 13}},
		{"phrase_delimiters", `MATCH(body) AGAINST('+"foo bar"' IN BOOLEAN MODE)`, []int{3, 7, 8, 9, 10, 13}},
		{"supplementary_letter_delimiter", "MATCH(body) AGAINST('+𞤀bar' IN BOOLEAN MODE)", []int{2, 3, 5, 7, 8, 9, 10, 11, 13}},
		{"supplementary_number_delimiter", "MATCH(body) AGAINST('+foo𝟙bar' IN BOOLEAN MODE)", []int{3, 7, 8, 9, 10, 13}},
		{"newer_unicode_is_delimiter", "MATCH(body) AGAINST('+foo\U0002EBF0bar' IN BOOLEAN MODE)", []int{3, 7, 8, 9, 10, 13}},
	} {
		f.check("token_standard", tc)
	}
	var size int
	must(t, f.db.QueryRowContext(context.Background(), "SELECT @@global.ngram_token_size").Scan(&size))
	if size != 2 && size != 3 {
		t.Logf("skip Unicode NGRAM cases: token size %d, require 2 or 3", size)
		return
	}
	f.makePair("token_ngram", `CREATE TABLE %s (id INT PRIMARY KEY, body TEXT COLLATE utf8mb4_bin,
		FULLTEXT INDEX ft_body(body) WITH PARSER NGRAM)`, rows)
	for _, conn := range []*sql.Conn{f.native, f.local} {
		f.exec(conn, "SET SESSION innodb_ft_enable_stopword=OFF")
	}
	defer func() {
		for _, conn := range []*sql.Conn{f.native, f.local} {
			f.exec(conn, "SET SESSION innodb_ft_enable_stopword=ON")
		}
	}()
	for _, tc := range []matchCase{
		{"ngram_supplementary_query_delimiter", "MATCH(body) AGAINST('+𞤀bar' IN BOOLEAN MODE)", []int{2, 3, 5, 7, 8, 9, 10, 11, 12, 13}},
		{"ngram_supplementary_number_query_delimiter", "MATCH(body) AGAINST('+foo𝟙bar' IN BOOLEAN MODE)", []int{1, 3, 4, 7, 8, 9, 10, 12, 13}},
		{"ngram_no_cross_delimiter", "MATCH(body) AGAINST('+foob' IN BOOLEAN MODE)", []int{12}},
		{"ngram_phrase_boundaries", `MATCH(body) AGAINST('+"foo bar"' IN BOOLEAN MODE)`, []int{3}},
	} {
		f.check("token_ngram", tc)
	}
}

func (f *fixture) testStandard(t *testing.T) {
	f.t = t
	f.makePair("docs", `CREATE TABLE %s (id INT PRIMARY KEY, title VARCHAR(200), body TEXT,
		FULLTEXT INDEX ft_body(body), FULLTEXT INDEX ft_title_body(title, body))`, []string{
		"VALUES (1, 'MySQL Tutorial', 'apple banana')",
		"VALUES (2, 'TiDB guide', 'apple carrot')",
		"VALUES (3, 'PostgreSQL', 'banana carrot')",
		"VALUES (4, 'TiDB tutorial', 'apple banana carrot')",
		"VALUES (5, 'Null payload', NULL)",
		"VALUES (6, NULL, 'apple banana')",
	})
	for _, tc := range []matchCase{
		{"required_excluded", "MATCH(body) AGAINST('+apple -carrot' IN BOOLEAN MODE)", []int{1, 6}},
		{"phrase", `MATCH(body) AGAINST('+"apple banana"' IN BOOLEAN MODE)`, []int{1, 4, 6}},
		{"prefix", "MATCH(body) AGAINST('+app*' IN BOOLEAN MODE)", []int{1, 2, 4, 6}},
		{"nullable_body", "MATCH(body) AGAINST('+apple' IN BOOLEAN MODE)", []int{1, 2, 4, 6}},
		{"composite_columns", "MATCH(title, body) AGAINST('+TiDB +apple' IN BOOLEAN MODE)", []int{2, 4}},
		{"composite_reordered_columns", "MATCH(body, title) AGAINST('+TiDB +apple' IN BOOLEAN MODE)", []int{2, 4}},
		{"cross_column_phrase", `MATCH(title, body) AGAINST('+"TiDB guide"' IN BOOLEAN MODE)`, []int{2}},
	} {
		f.check("docs", tc)
	}
	for _, path := range []struct {
		conn  *sql.Conn
		table string
	}{{f.native, "docs_native"}, {f.local, "docs_local"}} {
		assertMySQLError(t, path.conn, "SELECT id FROM `"+path.table+"` WHERE MATCH(title, title) AGAINST('+apple' IN BOOLEAN MODE)", 1191)
	}

	// QA function case 50 and mysql-test2/subexpr combine multiple MATCH
	// predicates; keep these in the default matrix to guard expression-level
	// TiFlash pushdown as well as single scan-level MATCH pushdown.
	for _, tc := range []matchCase{
		{"or_two_matches", "MATCH(body) AGAINST('+apple' IN BOOLEAN MODE) OR MATCH(body) AGAINST('+banana' IN BOOLEAN MODE)", []int{1, 2, 3, 4, 6}},
		{"and_two_matches", "MATCH(body) AGAINST('+apple' IN BOOLEAN MODE) AND MATCH(body) AGAINST('+banana' IN BOOLEAN MODE)", []int{1, 4, 6}},
		{"and_not_match", "MATCH(body) AGAINST('+apple' IN BOOLEAN MODE) AND NOT MATCH(body) AGAINST('+banana' IN BOOLEAN MODE)", []int{2}},
		{"or_composite_matches", "MATCH(title, body) AGAINST('+TiDB' IN BOOLEAN MODE) OR MATCH(title, body) AGAINST('+PostgreSQL' IN BOOLEAN MODE)", []int{2, 3, 4}},
	} {
		f.check("docs", tc)
	}

	// The same committed DML must be reflected by both real execution paths.
	for _, table := range []string{"docs_native", "docs_local"} {
		f.exec(f.admin, "INSERT INTO `"+table+"` VALUES (7, 'New TiDB', 'kiwi mango')")
		f.exec(f.admin, "UPDATE `"+table+"` SET body='kiwi carrot' WHERE id=2")
		f.exec(f.admin, "DELETE FROM `"+table+"` WHERE id=3")
	}
	f.check("docs", matchCase{"committed_dml", "MATCH(body) AGAINST('+kiwi' IN BOOLEAN MODE)", []int{2, 7}})
}

func (f *fixture) testNgram(t *testing.T) {
	f.t = t
	var size int
	must(t, f.db.QueryRowContext(context.Background(), "SELECT @@global.ngram_token_size").Scan(&size))
	if size != 2 {
		t.Skipf("QA NGRAM fixtures require ngram_token_size=2; cluster has %d (test does not mutate GLOBAL variables)", size)
	}
	f.makePair("ngram_docs", `CREATE TABLE %s (id INT PRIMARY KEY, body TEXT,
		FULLTEXT INDEX ft_body_ngram(body) WITH PARSER NGRAM)`, []string{
		"VALUES (1, 'apple')",
		"VALUES (2, 'banana')",
		"VALUES (3, '数据库')",
		"VALUES (4, '数据科学')",
		"VALUES (5, NULL)",
	})
	for _, tc := range []matchCase{
		{"chinese_ngram", "MATCH(body) AGAINST('+数据' IN BOOLEAN MODE)", []int{3, 4}},
		{"chinese_inner_ngram", "MATCH(body) AGAINST('+据库' IN BOOLEAN MODE)", []int{3}},
		{"short_prefix", "MATCH(body) AGAINST('+p*' IN BOOLEAN MODE)", []int{1}},
		{"long_prefix", "MATCH(body) AGAINST('+app*' IN BOOLEAN MODE)", []int{1}},
		{"multiple_match_ngram", "MATCH(body) AGAINST('+数据' IN BOOLEAN MODE) AND MATCH(body) AGAINST('+科学' IN BOOLEAN MODE)", []int{4}},
	} {
		f.check("ngram_docs", tc)
	}
}

func (f *fixture) testCollations(t *testing.T) {
	f.t = t
	columns := []struct {
		name      string
		collation string
		want      []int
	}{
		{"c_bin", "utf8mb4_bin", []int{3}},
		{"c_0900_bin", "utf8mb4_0900_bin", []int{3}},
		{"c_general", "utf8mb4_general_ci", []int{1, 2, 3}},
		{"c_unicode", "utf8mb4_unicode_ci", []int{1, 2, 3}},
		{"c_0900_ai", "utf8mb4_0900_ai_ci", []int{1, 2, 3}},
	}
	var ddl strings.Builder
	ddl.WriteString("CREATE TABLE %s (id INT PRIMARY KEY")
	for _, col := range columns {
		fmt.Fprintf(&ddl, ", %s TEXT CHARACTER SET utf8mb4 COLLATE %s, FULLTEXT INDEX ft_%s(%s)", col.name, col.collation, col.name, col.name)
	}
	ddl.WriteString(")")
	rows := []string{
		"VALUES (1, 'café', 'café', 'café', 'café', 'café')",
		"VALUES (2, 'CAFE', 'CAFE', 'CAFE', 'CAFE', 'CAFE')",
		"VALUES (3, 'cafe', 'cafe', 'cafe', 'cafe', 'cafe')",
		"VALUES (4, NULL, NULL, NULL, NULL, NULL)",
	}
	f.makePair("collation_docs", ddl.String(), rows)
	for _, col := range columns {
		f.check("collation_docs", matchCase{col.collation, "MATCH(" + col.name + ") AGAINST('+cafe' IN BOOLEAN MODE)", col.want})
	}
	f.check("collation_docs", matchCase{
		"multiple_match_mixed_collations",
		"MATCH(c_bin) AGAINST('+CAFE' IN BOOLEAN MODE) AND MATCH(c_general) AGAINST('+cafe' IN BOOLEAN MODE)",
		[]int{2},
	})
}
