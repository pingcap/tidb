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

package ftse2e

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestLocalMatchPlanCacheTiFlashE2E(t *testing.T) {
	dsn := os.Getenv("TIDB_FTS_E2E_DSN")
	if dsn == "" {
		t.Skip("set TIDB_FTS_E2E_DSN to verify Local MATCH never reuses plan-cache analyzer settings")
	}
	f := newFixture(t, dsn)
	executions := 0
	for _, parser := range []string{"standard", "ngram"} {
		parserDDL, search, upperSearch, id, upperID := "", "+the", "+The", 1, 2
		if parser == "ngram" {
			parserDDL, search, upperSearch, id, upperID = " WITH PARSER NGRAM", "+aaa", "+AAA", 4, 5
		}
		family := "plan_cache_" + parser
		f.makePair(family, "CREATE TABLE %s (id INT PRIMARY KEY, body TEXT COLLATE utf8mb4_bin, FULLTEXT INDEX ft(body)"+parserDDL+")", []string{"VALUES (1,'the'),(2,'The'),(3,'foo'),(4,'aaa'),(5,'AAA')"})
		for _, path := range []struct {
			conn   *sql.Conn
			table  string
			native bool
		}{{f.native, family + "_native", true}, {f.local, family + "_local", false}} {
			t.Run(path.table, func(t *testing.T) {
				f.exec(path.conn, "SET SESSION tidb_enable_prepared_plan_cache=ON,tidb_enable_non_prepared_plan_cache=ON,innodb_ft_enable_stopword=OFF,collation_server='utf8mb4_bin'")
				check := func(stmt *sql.Stmt, want []int, arg any) {
					t.Helper()
					for range 2 {
						rows, err := stmt.QueryContext(context.Background(), arg)
						must(t, err)
						got := readIDs(t, rows)
						var hit int
						must(t, path.conn.QueryRowContext(context.Background(), "SELECT @@last_plan_from_cache").Scan(&hit))
						if !reflect.DeepEqual(got, want) || hit != 0 {
							t.Fatalf("%s parameter=%v: want %v, got %v, last_plan_from_cache=%d", path.table, arg, want, got, hit)
						}
						executions++
					}
				}
				query := "SELECT id FROM " + path.table + " WHERE MATCH(body) AGAINST(? IN BOOLEAN MODE)"
				stmt, err := path.conn.PrepareContext(context.Background(), query)
				must(t, err)
				defer stmt.Close()
				for _, tc := range []struct {
					stopword, collation string
					search              any
					want                []int
				}{
					{"OFF", "utf8mb4_bin", search, []int{id}},
					{"ON", "utf8mb4_bin", search, []int{}},
					{"OFF", "utf8mb4_bin", search, []int{id}},
					{"ON", "utf8mb4_bin", upperSearch, []int{upperID}},
					{"ON", "utf8mb4_general_ci", upperSearch, []int{}},
					{"ON", "utf8mb4_bin", upperSearch, []int{upperID}},
					{"ON", "utf8mb4_bin", nil, []int{}},
					{"ON", "utf8mb4_bin", "+foo", []int{3}},
				} {
					f.exec(path.conn, "SET SESSION innodb_ft_enable_stopword="+tc.stopword+",collation_server='"+tc.collation+"'")
					check(stmt, tc.want, tc.search)
				}
				assertPlan(t, path.conn, "SELECT id FROM "+path.table+" WHERE MATCH(body) AGAINST('+foo' IN BOOLEAN MODE)", path.native)
				// Bind outside AGAINST and exercise a primary-key range as well.
				query = "SELECT id FROM " + path.table + " WHERE id>? AND MATCH(body) AGAINST('" + search + "' IN BOOLEAN MODE)"
				literalStmt, err := path.conn.PrepareContext(context.Background(), query)
				must(t, err)
				defer literalStmt.Close()
				for _, stopword := range []string{"OFF", "ON", "OFF"} {
					f.exec(path.conn, "SET SESSION innodb_ft_enable_stopword="+stopword)
					want := []int{id}
					if stopword == "ON" {
						want = []int{}
					}
					check(literalStmt, want, 0)
				}
				// Statement summary is sampled from binary EXECUTE, not a fresh
				// EXPLAIN of another statement. Inspect all retained MATCH plans.
				plans, err := f.admin.QueryContext(context.Background(), "SELECT PLAN FROM information_schema.statements_summary WHERE SCHEMA_NAME=? AND DIGEST_TEXT LIKE ? AND PLAN LIKE '%match_against%'", f.schema, "select %from `"+path.table+"` %")
				must(t, err)
				defer plans.Close()
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
			})
		}
	}
	t.Logf("verified %d binary prepared executions with last_plan_from_cache=0", executions)
}

func TestLocalMatchReviewFixesTiFlashE2E(t *testing.T) {
	dsn := os.Getenv("TIDB_FTS_E2E_DSN")
	if dsn == "" {
		t.Skip("set TIDB_FTS_E2E_DSN to verify collation rejection and filtered-phrase cache scope")
	}
	f := newFixture(t, dsn)
	for _, parser := range []string{"standard", "ngram"} {
		parserDDL := ""
		if parser == "ngram" {
			parserDDL = " WITH PARSER NGRAM"
		}
		family := "mixed_" + parser
		f.makePair(family, "CREATE TABLE %s (id INT PRIMARY KEY, title TEXT COLLATE utf8mb4_bin, body TEXT COLLATE utf8mb4_general_ci, FULLTEXT INDEX ft(title,body)"+parserDDL+", FULLTEXT INDEX ft_title(title)"+parserDDL+", FULLTEXT INDEX ft_body(body)"+parserDDL+")", []string{"VALUES (1,'FOO','BAR')"})
		for _, path := range []struct {
			conn  *sql.Conn
			table string
		}{{f.native, family + "_native"}, {f.local, family + "_local"}} {
			for _, columns := range []string{"title,body", "body,title"} {
				for _, search := range []string{"'+foo'", "NULL", "''"} {
					query := fmt.Sprintf("SELECT id FROM %s WHERE MATCH(%s) AGAINST(%s IN BOOLEAN MODE)", path.table, columns, search)
					assertMySQLError(t, path.conn, query, 1235)
					rows, err := path.conn.QueryContext(context.Background(), query)
					if rows != nil {
						_ = rows.Close()
					}
					if err == nil || !strings.Contains(err.Error(), "different MATCH column collations") {
						t.Fatalf("%s: expected the mixed-collation diagnostic, got %v", query, err)
					}
				}
			}
		}
		f.check(family, matchCase{"separate_" + parser + "_collations", "MATCH(title) AGAINST('+FOO' IN BOOLEAN MODE) OR MATCH(body) AGAINST('+bar' IN BOOLEAN MODE)", []int{1}})
	}
	for i, collation := range []string{"utf8mb4_bin", "utf8mb4_0900_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci", "utf8mb4_0900_ai_ci"} {
		family := fmt.Sprintf("same_collation_%d", i)
		f.makePair(family, "CREATE TABLE %s (id INT PRIMARY KEY, title TEXT COLLATE "+collation+", body TEXT COLLATE "+collation+", FULLTEXT INDEX ft(title,body))", []string{"VALUES (1,'FOO','BAR')"})
		want := []int{}
		if !strings.HasSuffix(collation, "_bin") {
			want = []int{1}
		}
		for j, columns := range []string{"title,body", "body,title"} {
			f.check(family, matchCase{fmt.Sprintf("same_%s_order_%d", collation, j), "MATCH(" + columns + ") AGAINST('+foo' IN BOOLEAN MODE)", want})
		}
	}
	f.makePair("phrase_cache", `CREATE TABLE %s (id INT PRIMARY KEY, title MEDIUMTEXT COLLATE utf8mb4_bin, body MEDIUMTEXT COLLATE utf8mb4_bin, FULLTEXT INDEX ft(title,body))`, []string{
		"VALUES (1,'foo x bar','foo a bar')",
		"VALUES (2,'foo a bar','foo x bar')",
		"VALUES (3,'foo x bar',NULL)",
		"VALUES (4,NULL,'foo a bar')",
		"VALUES (5,'" + strings.Repeat("foo x bar ", 8192) + "','foo x bar')",
	})
	for _, tc := range []matchCase{
		{"phrase_cache_must", `MATCH(title,body) AGAINST('+"foo a bar"' IN BOOLEAN MODE)`, []int{1, 2, 4}},
		{"phrase_cache_should", `MATCH(title,body) AGAINST('"foo a bar"' IN BOOLEAN MODE)`, []int{1, 2, 4}},
		{"phrase_cache_must_not", `MATCH(title,body) AGAINST('+foo -"foo a bar"' IN BOOLEAN MODE)`, []int{3, 5}},
		{"phrase_cache_independent_clauses", `MATCH(title,body) AGAINST('+"foo x bar" -"foo a bar"' IN BOOLEAN MODE)`, []int{3, 5}},
	} {
		f.check("phrase_cache", tc)
	}
}
