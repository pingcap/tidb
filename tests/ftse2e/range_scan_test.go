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

func TestMatchPlanValidation(t *testing.T) {
	for _, tc := range []struct {
		name, plan             string
		native, decodedSummary bool
		wantErr                bool
	}{
		{"full_scan", "Selection_1 1 mpp[tiflash] match_against(\"+foo\",body)\n└─TableFullScan_2 10 mpp[tiflash]", true, false, false},
		{"range_scan_with_root_filter", "Selection_1 1 root unrelated(body)\n└─Selection_2 1 mpp[tiflash] match_against(\"+foo\",body)\n  └─TableRangeScan_3 10 mpp[tiflash] range:[-1,10)", true, false, false},
		{"executed_cop_range_without_mpp", "Selection_1\tcop[tiflash]\t1\tmatch_against(\"+foo\",body)\n└─TableRangeScan_2\tcop[tiflash]\t10\trange:[-1,10)", true, true, true},
		{"decoded_mpp_range", "TableReader_1\troot\t1\tMppVersion: 2, data:ExchangeSender_2\n└─ExchangeSender_2\tcop[tiflash]\t1\tExchangeType: PassThrough\n  └─Selection_3\tcop[tiflash]\t1\tmatch_against(\"+foo\",body)\n    └─TableRangeScan_4\tcop[tiflash]\t10\trange:[-1,10)", true, true, false},
		{"summary_missing_sender", "TableReader_1 root 1 MppVersion: 2, data:ExchangeSender_2\n└─Selection_3 cop[tiflash] 1 match_against(\"+foo\",body)\n  └─TableRangeScan_4 cop[tiflash] 10 range:[-1,10)", true, true, true},
		{"summary_missing_reader", "ExchangeSender_2 cop[tiflash] 1 ExchangeType: PassThrough\n└─Selection_3 cop[tiflash] 1 match_against(\"+foo\",body)\n  └─TableRangeScan_4 cop[tiflash] 10 range:[-1,10)", true, true, true},
		{"batch_cop_rejected", "Selection_1 1 batchCop[tiflash] match_against(\"+foo\",body)\n└─TableFullScan_2 10 batchCop[tiflash]", true, false, true},
		{"cop_not_allowed", "Selection_1 1 cop[tiflash] match_against(\"+foo\",body)\n└─TableRangeScan_2 10 cop[tiflash]", true, false, true},
		{"no_match", "Selection_1 1 mpp[tiflash] gt(id,0)\n└─TableFullScan_2 10 mpp[tiflash]", true, false, true},
		{"root_match", "Selection_1 1 root match_against(\"+foo\",body)\n└─Selection_2 1 mpp[tiflash] gt(id,0)\n  └─TableRangeScan_3 10 mpp[tiflash]", true, false, true},
		{"mixed_match_placement", "Selection_1 1 root match_against(\"+bar\",body)\n└─Selection_2 1 mpp[tiflash] match_against(\"+foo\",body)\n  └─TableFullScan_3 10 mpp[tiflash]", true, false, true},
		{"projection_match", "Projection_1 1 mpp[tiflash] match_against(\"+foo\",body)\n└─TableFullScan_2 10 mpp[tiflash]", true, false, true},
		{"wrong_scan_task", "Selection_1 1 mpp[tiflash] match_against(\"+foo\",body)\n└─TableRangeScan_2 10 cop[tikv]", true, false, true},
		{"dual_not_pushdown_evidence", "TableDual_1 0 root rows:0", true, false, true},
		{"tici", "Selection_1 1 root match_against(\"+foo\",body)\n└─IndexRangeScan_2 10 cop[tici]", false, false, true},
		{"fallback", "Selection_1 1 root match_against(\"+foo\",body)\n└─TableRangeScan_2 10 cop[tikv]", false, false, false},
		{"fallback_with_replica_scan", "Selection_1 1 root match_against(\"+foo\",body)\n└─TableRangeScan_2 10 mpp[tiflash]", false, false, false},
		{"fallback_match_pushed", "Selection_1 1 cop[tiflash] match_against(\"+foo\",body)\n└─TableRangeScan_2 10 cop[tiflash]", false, true, true},
		{"task_word_in_search", "Selection_1 1 root match_against(\"+mpp[tiflash] selection\",body)\n└─TableFullScan_2 10 mpp[tiflash]", true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateMatchPlan(tc.plan, tc.native, tc.decodedSummary); (err != nil) != tc.wantErr {
				t.Fatalf("want error=%v, got %v for:\n%s", tc.wantErr, err, tc.plan)
			}
		})
	}
}

func TestLocalMatchRangesTiFlashE2E(t *testing.T) {
	dsn := os.Getenv("TIDB_FTS_E2E_DSN")
	if dsn == "" {
		t.Skip("set TIDB_FTS_E2E_DSN to verify primary-key ranges with Local MATCH")
	}
	f := newFixture(t, dsn)
	for _, conn := range []*sql.Conn{f.native, f.local} {
		f.exec(conn, "SET SESSION innodb_ft_enable_stopword=OFF,tidb_enable_prepared_plan_cache=ON")
	}
	var summaryEnabled int
	must(t, f.admin.QueryRowContext(context.Background(), "SELECT @@global.tidb_enable_stmt_summary").Scan(&summaryEnabled))
	if summaryEnabled != 1 {
		t.Fatal("prepared range verification requires tidb_enable_stmt_summary=ON")
	}
	executions := 0
	for _, parser := range []string{"standard", "ngram"} {
		parserDDL := ""
		if parser == "ngram" {
			parserDDL = " WITH PARSER NGRAM"
		}
		family := "ranges_" + parser
		f.makePair(family, "CREATE TABLE %s (id INT PRIMARY KEY, body TEXT COLLATE utf8mb4_bin, FULLTEXT INDEX ft(body)"+parserDDL+")", []string{
			"VALUES (-2147483648,'foo'),(-10,'foo'),(-1,'foo mysql'),(0,'foo'),(1,'foo'),(2,'bar'),(3,NULL),(10,'foo'),(2147483647,'foo')",
		})
		for _, path := range []struct {
			conn   *sql.Conn
			table  string
			native bool
		}{{f.native, family + "_native", true}, {f.local, family + "_local", false}} {
			t.Run(path.table, func(t *testing.T) {
				for _, tc := range []struct {
					name, bounds string
					want         []int
				}{
					{"inclusive", "id>=-10 AND id<=10", []int{-10, 0, 1, 10}},
					{"exclusive", "id>-10 AND id<10", []int{0, 1}},
					{"between", "id BETWEEN -10 AND 0", []int{-10, 0}},
					{"disjoint", "(id BETWEEN -10 AND -1 OR id BETWEEN 1 AND 10)", []int{-10, 1, 10}},
					{"signed_min", "id<=-2147483648", []int{-2147483648}},
					{"signed_max", "id>=2147483647", []int{2147483647}},
					{"no_stored_keys", "id BETWEEN 11 AND 20", []int{}},
					{"only_nonmatches", "id BETWEEN 2 AND 3", []int{}},
				} {
					t.Run(tc.name, func(t *testing.T) {
						query := "SELECT id FROM " + path.table + " WHERE " + tc.bounds + " AND MATCH(body) AGAINST('+foo -mysql' IN BOOLEAN MODE)"
						plan := explainPlan(t, path.conn, query)
						assertPlanText(t, query, plan, path.native, false)
						if path.native && !strings.Contains(plan, "tablerangescan") {
							t.Fatalf("expected primary-key TableRangeScan:\n%s", plan)
						}
						if got := queryIDs(t, path.conn, query); !reflect.DeepEqual(got, tc.want) {
							t.Fatalf("%s: want %v, got %v", query, tc.want, got)
						}
						t.Logf("%s plan:\n%s", tc.name, plan)
					})
				}
				// A contradictory range has no scan. TiDB fallback may retain a
				// Selection above TableDual, but no input row reaches its matcher.
				// This proves an empty result, not TiFlash scalar execution.
				query := "SELECT id FROM " + path.table + " WHERE id>10 AND id<0 AND MATCH(body) AGAINST('+foo' IN BOOLEAN MODE)"
				plan := explainPlan(t, path.conn, query)
				if !strings.Contains(plan, "tabledual") || strings.Contains(plan, "scan") || strings.Contains(plan, "cop[tici]") {
					t.Fatalf("expected contradictory range to be eliminated:\n%s", plan)
				}
				if got := queryIDs(t, path.conn, query); len(got) != 0 {
					t.Fatalf("contradictory range returned %v", got)
				}
				query = "SELECT id FROM " + path.table + " WHERE id>=? AND id<? AND MATCH(body) AGAINST(? IN BOOLEAN MODE)"
				stmt, err := path.conn.PrepareContext(context.Background(), query)
				must(t, err)
				defer stmt.Close()
				for _, tc := range []struct {
					low, high int
					search    string
					want      []int
				}{
					{-10, 11, "+foo -mysql", []int{-10, 0, 1, 10}},
					{0, 2, "+foo", []int{0, 1}},
					{-10, 0, "+mysql", []int{-1}},
					{11, 20, "+foo", []int{}},
					{10, 0, "+foo", []int{}},
					{-10, 11, "+foo -mysql", []int{-10, 0, 1, 10}},
				} {
					for range 2 {
						rows, err := stmt.QueryContext(context.Background(), tc.low, tc.high, tc.search)
						must(t, err)
						got := readIDs(t, rows)
						var hit int
						must(t, path.conn.QueryRowContext(context.Background(), "SELECT @@last_plan_from_cache").Scan(&hit))
						if !reflect.DeepEqual(got, tc.want) || hit != 0 {
							t.Fatalf("range [%d,%d) search=%s: want %v, got %v, cache=%d", tc.low, tc.high, tc.search, tc.want, got, hit)
						}
						executions++
					}
				}
				// Inspect executed prepared plans, not EXPLAIN with independently
				// rewritten placeholders. One summary plan cannot prove every bind.
				plans, err := f.admin.QueryContext(context.Background(), "SELECT PLAN FROM information_schema.statements_summary WHERE SCHEMA_NAME=? AND DIGEST_TEXT LIKE ? AND PLAN LIKE '%match_against%'", f.schema, fmt.Sprintf("select %%from `%s` where `id` >= ? and `id` < ? and match%%", path.table))
				must(t, err)
				defer plans.Close()
				count := 0
				for plans.Next() {
					var executedPlan string
					must(t, plans.Scan(&executedPlan))
					assertPlanText(t, query, executedPlan, path.native, true)
					if path.native && !strings.Contains(strings.ToLower(executedPlan), "tablerangescan") {
						t.Fatalf("expected executed primary-key range:\n%s", executedPlan)
					}
					t.Logf("executed prepared range plan:\n%s", executedPlan)
					count++
				}
				must(t, plans.Err())
				must(t, plans.Close())
				if count == 0 {
					t.Fatal("missing executed prepared range MATCH plan")
				}
			})
		}
	}
	t.Logf("verified 32 bounded result/plan cases, 4 eliminated ranges and %d binary prepared executions", executions)
}
