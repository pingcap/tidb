// Copyright 2026 PingCAP, Inc. Licensed under Apache License 2.0.

package ftse2e

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestLocalMatchTokenMemoryTiFlashE2E is deliberately opt-in: NGRAM size 1
// materializes many tokens, and concurrent long-document queries consume much
// more memory than the default correctness suite. It never changes GLOBALs.
// Sample the owned TiFlash process externally; SQL timing is not an allocation
// measurement, and process RSS includes storage, proxy and background work.
func TestLocalMatchTokenMemoryTiFlashE2E(t *testing.T) {
	dsn := os.Getenv("TIDB_FTS_E2E_DSN")
	if dsn == "" || os.Getenv("TIDB_FTS_MEMORY_STRESS") != "1" {
		t.Skip("set TIDB_FTS_E2E_DSN and TIDB_FTS_MEMORY_STRESS=1 for bounded server memory stress")
	}
	bytes := 1 << 20
	if value := os.Getenv("TIDB_FTS_MEMORY_BYTES"); value != "" {
		var err error
		bytes, err = strconv.Atoi(value)
		if err != nil || bytes < 4096 || bytes > 1<<20 {
			t.Fatal("TIDB_FTS_MEMORY_BYTES must be 4096..1048576")
		}
	}
	f := newFixture(t, dsn)
	var gramSize int
	must(t, f.db.QueryRowContext(context.Background(), "SELECT @@global.ngram_token_size").Scan(&gramSize))
	unit := "quick the fox 数据库 "
	long := strings.Repeat(unit, bytes/len(unit)) + "endingmark"
	t.Logf("memory workload: ngram=%d document_bytes=%d native_connections=4 query_threads=1 repeats=3", gramSize, len(long))
	for _, parser := range []string{"standard", "ngram"} {
		for _, collation := range []string{"utf8mb4_bin", "utf8mb4_general_ci"} {
			family := "memory_" + parser + "_" + collation
			parserSQL := ""
			if parser == "ngram" {
				parserSQL = " WITH PARSER NGRAM"
			}
			ddl := "CREATE TABLE %s (id INT PRIMARY KEY, body MEDIUMTEXT COLLATE " + collation + ", extra MEDIUMTEXT COLLATE " + collation + ", FULLTEXT INDEX ft(body,extra)" + parserSQL + ")"
			// Large/short/NULL/empty/large rows and two columns exercise reusable
			// token buffers and borrowed views across changing input lifetimes.
			for _, suffix := range []string{"native", "local"} {
				table := family + "_" + suffix
				f.exec(f.admin, fmt.Sprintf(ddl, table))
				for i, pair := range [][2]any{{long, "quick the fox"}, {"quick fox", long}, {nil, nil}, {"", ""}, {long, long}, {"quick the fox", "quick x fox"}} {
					f.exec(f.admin, "INSERT INTO "+table+" VALUES (?,?,?)", i+1, pair[0], pair[1])
				}
			}
			f.exec(f.admin, "ALTER TABLE "+family+"_native SET TIFLASH REPLICA 1")
			f.waitForReplica(family + "_native")
			for _, stop := range []string{"OFF", "ON"} {
				t.Run(family+"/stopword_"+stop, func(t *testing.T) {
					for _, c := range []*sql.Conn{f.native, f.local} {
						f.exec(c, "SET SESSION innodb_ft_enable_stopword="+stop)
						f.exec(c, "SET SESSION collation_server='"+collation+"'")
						f.exec(c, "SET SESSION tidb_max_tiflash_threads=1")
					}
					searches := []string{"+quick -absentterm", `+"quick the fox"`, "+endingmark*", "+absentterm"}
					queries := make([]string, len(searches))
					want := make([][]int, len(searches))
					for i, search := range searches {
						predicate := "MATCH(body,extra) AGAINST('" + search + "' IN BOOLEAN MODE)"
						queries[i] = "SELECT id FROM " + family + "_native WHERE " + predicate
						localQuery := "SELECT id FROM " + family + "_local WHERE " + predicate
						assertPlan(t, f.native, queries[i], true)
						assertPlan(t, f.local, localQuery, false)
						want[i] = queryIDs(t, f.local, localQuery)
						if got := queryIDs(t, f.native, queries[i]); !reflect.DeepEqual(got, want[i]) {
							t.Fatalf("initial %q: TiDB=%v TiFlash=%v", search, want[i], got)
						}
					}
					connections := make([]*sql.Conn, 4)
					for i := range connections {
						connections[i] = f.connect()
						for _, setting := range []string{"tidb_enable_local_match_against=ON", "tidb_isolation_read_engines='tiflash'", "tidb_max_tiflash_threads=1", "innodb_ft_enable_stopword=" + stop, "collation_server='" + collation + "'"} {
							f.exec(connections[i], "SET SESSION "+setting)
						}
					}
					var workers sync.WaitGroup
					errors := make(chan error, len(connections))
					start := time.Now()
					for _, c := range connections {
						workers.Add(1)
						go func() {
							defer workers.Done()
							for range 3 {
								for i, q := range queries {
									// Do not call Fatal in worker goroutines: collect the
									// first error per worker and join before reporting.
									if err := checkMemoryQuery(c, q, want[i]); err != nil {
										errors <- err
										return
									}
								}
							}
						}()
					}
					workers.Wait()
					close(errors)
					for _, c := range connections {
						must(t, c.Close())
					}
					for err := range errors {
						t.Error(err)
					}
					t.Logf("48 concurrent native queries elapsed=%s", time.Since(start))
				})
			}
		}
	}
}

func checkMemoryQuery(c *sql.Conn, query string, want []int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	rows, err := c.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	got := make([]int, 0)
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return err
		}
		got = append(got, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// No SQL ORDER BY: sorting in the runner preserves the validated plan.
	slices.Sort(got)
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("%s: want %v got %v", query, want, got)
	}
	return nil
}
