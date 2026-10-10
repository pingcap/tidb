// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"database/sql"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPercentile(t *testing.T) {
	samples := []float64{100, 1, 20, 2, 3}
	sort.Float64s(samples)
	if percentile(samples, .5) != 3 || percentile(samples, .95) != 100 || percentile(nil, .99) != 0 {
		t.Fatal("incorrect nearest-rank percentiles")
	}
}

func TestMeasureCountsAndMismatch(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		mock.MatchExpectationsInOrder(false)
		count := 30
		if mismatch {
			count = 1
		}
		for range count {
			got := 7
			if mismatch {
				got = 8
			}
			mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(got))
		}
		workers := 3
		if mismatch {
			workers = 1
		}
		conns := make([]*sql.Conn, workers)
		for i := range conns {
			conns[i], err = db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
		}
		m, err := measure(context.Background(), conns, "SELECT COUNT(*)", 7, options{iterations: 30, timeout: time.Second})
		if mismatch {
			if err == nil || m.Errors != 1 || m.Completed != 0 {
				t.Fatalf("mismatch was not rejected: %+v %v", m, err)
			}
		} else if err != nil || m.Completed != 30 || m.Errors != 0 || m.QPS <= 0 {
			t.Fatalf("incorrect completed query accounting: %+v %v", m, err)
		}
		for _, c := range conns {
			c.Close()
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}

func TestRejectNonlocalDSN(t *testing.T) {
	for _, dsn := range []string{"root@tcp(10.0.0.1:4000)/", "root@tcp(127.0.0.1:4000)/existing_db"} {
		if db, err := openDB(dsn); err == nil {
			db.Close()
			t.Fatalf("accepted %s", dsn)
		}
	}
}

func TestPlanPlacement(t *testing.T) {
	for _, tc := range []struct {
		name, path, matchTask, scanTask, scanOperator string
		wantError                                     bool
	}{
		{"mpp", "native", "mpp[tiflash]", "mpp[tiflash]", "TableFullScan", false},
		{"mpp_range", "native", "mpp[tiflash]", "mpp[tiflash]", "TableRangeScan", false},
		{"cop", "native", "cop[tiflash]", "cop[tiflash]", "TableFullScan", true},
		{"batch_cop", "native", "batchCop[tiflash]", "batchCop[tiflash]", "TableFullScan", true},
		{"batch_cop_range", "native", "batchCop[tiflash]", "batchCop[tiflash]", "TableRangeScan", true},
		{"fallback", "local", "root", "cop[tikv]", "TableFullScan", false},
		{"native_root_match", "native", "root", "mpp[tiflash]", "TableFullScan", true},
		{"native_tikv_scan", "native", "mpp[tiflash]", "cop[tikv]", "TableFullScan", true},
		{"fallback_pushed", "local", "batchCop[tiflash]", "batchCop[tiflash]", "TableFullScan", true},
		{"tici", "native", "batchCop[tici]", "batchCop[tici]", "TableFullScan", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("EXPLAIN").WillReturnRows(sqlmock.NewRows([]string{"id", "estRows", "task", "access object", "operator info"}).
				AddRow("Selection", "1", tc.matchTask, "", `match_against("+foo", body)`).
				AddRow(tc.scanOperator, "1", tc.scanTask, "table:docs", "keep order:false"))
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_, err = plan(context.Background(), conn, "SELECT COUNT(*) FROM docs", tc.path)
			if (err != nil) != tc.wantError {
				t.Fatalf("unexpected plan verdict: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRequireDefaultMPPSettings(t *testing.T) {
	for _, settings := range [][3]int{{1, 0, 0}, {0, 0, 0}, {1, 1, 0}, {1, 0, 1}} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("SELECT @@tidb_allow_mpp,@@tidb_allow_tiflash_cop,@@tidb_enforce_mpp").
			WillReturnRows(sqlmock.NewRows([]string{"allow", "cop", "enforce"}).AddRow(settings[0], settings[1], settings[2]))
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		err = requireDefaultMPPSettings(context.Background(), conn)
		if (err == nil) != (settings == [3]int{1, 0, 0}) {
			t.Fatalf("unexpected settings verdict for %v: %v", settings, err)
		}
		conn.Close()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}

func TestVerifyAnalyzerSettings(t *testing.T) {
	for _, tc := range []struct {
		name, stored, current string
		gram                  int
		wantError             bool
	}{
		{"same", "utf8mb4_bin", "utf8mb4_bin", 2, false},
		{"changed_stopword_collation", "utf8mb4_bin", "utf8mb4_general_ci", 2, true},
		{"changed_token_size", "utf8mb4_bin", "utf8mb4_bin", 3, true},
		{"missing_stopword_collation", "", "utf8mb4_bin", 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if tc.stored != "" {
				mock.ExpectQuery(regexp.QuoteMeta(analyzerSettingsSQL)).WillReturnRows(sqlmock.NewRows([]string{"ngram", "min", "max", "collation"}).AddRow(tc.gram, 3, 84, tc.current))
			}
			err = verifyAnalyzerSettings(context.Background(), db, manifest{NgramSize: 2, MinSize: 3, MaxSize: 84, StopwordCollation: tc.stored})
			if (err != nil) != tc.wantError {
				t.Fatalf("unexpected analyzer verdict: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConnectPinsStopwordCollation(t *testing.T) {
	for _, effective := range []string{"utf8mb4_bin", "utf8mb4_general_ci"} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range []string{"USE `fts_bench_1`", "SET SESSION tidb_enable_local_match_against=ON", "SET SESSION tidb_isolation_read_engines='tikv'", "SET SESSION innodb_ft_enable_stopword=ON", "SET SESSION tidb_max_tiflash_threads=1"} {
			mock.ExpectExec(regexp.QuoteMeta(q)).WillReturnResult(sqlmock.NewResult(0, 0))
		}
		mock.ExpectExec(regexp.QuoteMeta("SET SESSION collation_server=?")).WithArgs("utf8mb4_bin").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta("SELECT @@session.collation_server")).WillReturnRows(sqlmock.NewRows([]string{"collation"}).AddRow(effective))
		conn, err := connect(context.Background(), db, manifest{Schema: "fts_bench_1", StopwordCollation: "utf8mb4_bin"}, options{stopwords: true, threads: 1}, "local")
		if (err == nil) != (effective == "utf8mb4_bin") {
			t.Fatalf("unexpected session verdict for %s: %v", effective, err)
		}
		if conn != nil {
			conn.Close()
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}
