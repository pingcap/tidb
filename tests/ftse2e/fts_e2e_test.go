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
}

func newFixture(t *testing.T, dsn string) *fixture {
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
	var version string
	must(t, db.QueryRowContext(ctx, "SELECT tidb_version()").Scan(&version))
	t.Logf("TiDB: %s", version)

	// Never operate on an existing schema. The generated name is used only by
	// this test and is the only schema removed during cleanup.
	schema := fmt.Sprintf("fts_e2e_%d", time.Now().UnixNano())
	_, err = db.ExecContext(ctx, "CREATE DATABASE `"+schema+"`")
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

func assertPlan(t *testing.T, conn *sql.Conn, query string, native bool) {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), "EXPLAIN FORMAT='brief' "+query)
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
	text := strings.ToLower(plan.String())
	if strings.Contains(text, "cop[tici]") {
		t.Fatalf("unexpected TiCI plan for %s:\n%s", query, text)
	}
	if native {
		if !strings.Contains(text, "mpp[tiflash]") || !strings.Contains(text, "tablefullscan") {
			t.Fatalf("expected TiFlash TableFullScan for %s:\n%s", query, text)
		}
		mppSelection := false
		for _, line := range strings.Split(text, "\n") {
			if !strings.Contains(line, "selection") {
				continue
			}
			if !strings.Contains(line, "mpp[tiflash]") {
				t.Fatalf("MATCH remains in a TiDB-side Selection for %s:\n%s", query, text)
			}
			mppSelection = true
		}
		if strings.Count(strings.ToLower(query), "match(") > 1 && !mppSelection {
			t.Fatalf("expected compound MATCH predicates to execute in a TiFlash Selection for %s:\n%s", query, text)
		}
	} else if strings.Contains(text, "mpp[tiflash]") || !strings.Contains(text, "match_against") {
		t.Fatalf("expected TiDB local MATCH fallback for %s:\n%s", query, text)
	}
}

func queryIDs(t *testing.T, conn *sql.Conn, query string) []int {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), query)
	must(t, err)
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
		{"cross_column_phrase", `MATCH(title, body) AGAINST('+"TiDB guide"' IN BOOLEAN MODE)`, []int{2}},
	} {
		f.check("docs", tc)
	}
	for _, path := range []struct {
		conn  *sql.Conn
		table string
	}{{f.native, "docs_native"}, {f.local, "docs_local"}} {
		assertMySQLError(t, path.conn, "SELECT id FROM `"+path.table+"` WHERE MATCH(body, title) AGAINST('+apple' IN BOOLEAN MODE)", 1191)
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
		{"short_prefix", "MATCH(body) AGAINST('+ap*' IN BOOLEAN MODE)", []int{1}},
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
