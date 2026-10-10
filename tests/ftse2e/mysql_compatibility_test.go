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
	"time"

	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/collate"
)

// TestLocalMatchMySQLCompatibility uses a real InnoDB index as the oracle.
// With both DSNs it also verifies real TiDB fallback and TiFlash Selection.
// MySQL owns no pre-existing schema here; only this test's unique schema is dropped.
func TestLocalMatchMySQLCompatibility(t *testing.T) {
	dsn := os.Getenv("MYSQL_FTS_E2E_DSN")
	if dsn == "" {
		t.Skip("set MYSQL_FTS_E2E_DSN for the real MySQL 8.0.44 compatibility oracle")
	}
	db := openLocalDB(t, dsn)
	conn, err := db.Conn(context.Background())
	must(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	exec := func(query string) {
		_, err := conn.ExecContext(context.Background(), query)
		must(t, err)
	}
	var version string
	var minSize, maxSize, gramSize int
	must(t, conn.QueryRowContext(context.Background(), "SELECT @@version,@@innodb_ft_min_token_size,@@innodb_ft_max_token_size,@@ngram_token_size").Scan(&version, &minSize, &maxSize, &gramSize))
	if !strings.HasPrefix(version, "8.0.44") {
		t.Fatalf("oracle requires MySQL 8.0.44, got %s", version)
	}
	t.Logf("MySQL oracle: %s min=%d max=%d ngram=%d", version, minSize, maxSize, gramSize)
	schema := fmt.Sprintf("fts_oracle_%d", time.Now().UnixNano())
	exec("CREATE DATABASE `" + schema + "`")
	t.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP DATABASE `"+schema+"`")
		if err != nil {
			t.Errorf("oracle cleanup: %v", err)
		}
	})
	exec("USE `" + schema + "`")
	exec("SET NAMES utf8mb4")
	previous := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(previous)
	var f *fixture
	if tidbDSN := os.Getenv("TIDB_FTS_E2E_DSN"); tidbDSN != "" {
		f = newFixture(t, tidbDSN)
		var a, b, c int
		must(t, f.db.QueryRowContext(context.Background(), "SELECT @@innodb_ft_min_token_size,@@innodb_ft_max_token_size,@@ngram_token_size").Scan(&a, &b, &c))
		if a != minSize || b != maxSize || c != gramSize {
			t.Fatalf("analyzer configuration differs: TiDB=%d/%d/%d MySQL=%d/%d/%d", a, b, c, minSize, maxSize, gramSize)
		}
	}
	documents := []*string{nil}
	for _, text := range []string{"", "quick fox", "quick the fox", "quick x fox", "quick theory fox", "slow turtle", "foobar", "foo bar", "foo,bar", "foo，bar", "foo🙃bar", "foo👁bar", "foo𞤀bar", "foo𝟙bar", "foo\U0002EBF0bar", "abc", "ab bc", "a,b", "a，b", "quick brown fox", "quick a fox", "quick xx fox", "quick café", "QUICK CAFÉ", "foo zoo", "foo a zoo", "foo x zoo", "foo xx zoo", "zoo foo", "fox quick", "fox the quick"} {
		documents = append(documents, &text)
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	searches := []string{"+quick", "+quick +the", "+quick +x", "+the quick", "+x quick", "+quick -the", "+quick -x", "+quick +the*", "+the", "+x", `+quick +"the"`, `+quick +"x"`, `"quick the fox"`, `"quick x fox"`, `+"quick brown fox"`, "+foo", "+foob", "+foo，bar", "+foo🙃bar", "+foo👁bar", "+foo𞤀bar", "+foo𝟙bar", `+"foo bar"`, `+"foo🙃bar"`, `+"abc"`, "a*", "abc*", "+cafe", `"quick a fox"`, `"foo a zoo"`, `"foo zoo"`, `"the quick"`, `"quick the"`, `"fox quick"`}
	for _, parser := range []model.FullTextParserType{model.FullTextParserTypeStandardV1, model.FullTextParserTypeNgramV1} {
		for _, columnCollation := range []string{"utf8mb4_bin", "utf8mb4_0900_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci", "utf8mb4_0900_ai_ci"} {
			for _, stopwords := range []bool{false, true} {
				family := fmt.Sprintf("oracle_%s_%s_%t", strings.ToLower(string(parser)), columnCollation, stopwords)
				setting := "OFF"
				if stopwords {
					setting = "ON"
				}
				exec("SET SESSION innodb_ft_enable_stopword=" + setting)
				exec("SET SESSION collation_server='" + columnCollation + "'")
				ddl := "CREATE TABLE %s (id INT PRIMARY KEY, body TEXT COLLATE " + columnCollation + ", FULLTEXT INDEX ft(body)"
				if parser == model.FullTextParserTypeNgramV1 {
					ddl += " WITH PARSER NGRAM"
				}
				ddl += ")"
				exec(fmt.Sprintf(ddl, family) + " ENGINE=InnoDB")
				var inserts []string
				for i, document := range documents {
					value := "NULL"
					if document != nil {
						value = quote(*document)
					}
					row := fmt.Sprintf("VALUES (%d,%s)", i+1, value)
					inserts = append(inserts, row)
					exec("INSERT INTO " + family + " " + row)
				}
				if f != nil {
					for _, path := range []*sql.Conn{f.native, f.local} {
						f.exec(path, "SET SESSION innodb_ft_enable_stopword="+setting)
						f.exec(path, "SET SESSION collation_server='"+columnCollation+"'")
					}
					f.makePair(family, ddl, inserts)
				}
				config := fulltext.AnalyzerConfig{ParserType: parser, Collation: columnCollation, StopwordCollation: columnCollation, InnodbFtMinTokenSize: minSize, InnodbFtMaxTokenSize: maxSize, NgramTokenSize: gramSize, InnodbFtEnableStopword: stopwords}
				analyzer, err := fulltext.GetAnalyzer(config)
				must(t, err)
				for _, search := range searches {
					t.Run(family+"/"+search, func(t *testing.T) {
						predicate := "MATCH(body) AGAINST(" + quote(search) + " IN BOOLEAN MODE)"
						want := queryIDs(t, conn, "SELECT id FROM "+family+" WHERE "+predicate)
						query, err := fulltext.CompileBooleanQuery(search, config)
						must(t, err)
						got := make([]int, 0)
						for i, text := range documents {
							input := fulltext.ColumnInput{IsNull: text == nil}
							if text != nil {
								input.Text = *text
							}
							document, err := fulltext.BuildDocument([]fulltext.ColumnInput{input}, analyzer)
							must(t, err)
							if query.Match(document) {
								got = append(got, i+1)
							}
						}
						if !reflect.DeepEqual(got, want) {
							t.Errorf("MySQL=%v Go matcher=%v", want, got)
						}
						if f != nil {
							previousTest := f.t
							f.t = t
							f.check(family, matchCase{"three_way", predicate, want})
							f.t = previousTest
						}
					})
				}
				for _, prefix := range []string{"", "NOT "} {
					predicate := prefix + "MATCH(body) AGAINST(NULL IN BOOLEAN MODE)"
					want := queryIDs(t, conn, "SELECT id FROM "+family+" WHERE "+predicate)
					if f != nil {
						for _, path := range []struct {
							conn   *sql.Conn
							suffix string
						}{{f.native, "native"}, {f.local, "local"}} {
							query := "SELECT id FROM " + family + "_" + path.suffix + " WHERE " + predicate
							if got := queryIDs(t, path.conn, query); !reflect.DeepEqual(got, want) {
								t.Fatalf("%s: MySQL=%v TiDB/TiFlash=%v", query, want, got)
							}
							// A constant-empty search may be folded before execution.
							t.Logf("NULL plan (%s): %s", path.suffix, explainPlan(t, path.conn, query))
						}
					}
				}
			}
		}
	}
}
