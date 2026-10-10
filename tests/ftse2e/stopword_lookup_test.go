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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/collate"
)

// Stopword lookup must use the server collation, independently of MATCH's
// column collation. Compare the optimized native path with both the unchanged
// Go matcher and TiDB's SQL fallback, including filtered-phrase verification.
func TestLocalMatchStopwordLookupTiFlashE2E(t *testing.T) {
	dsn := os.Getenv("TIDB_FTS_E2E_DSN")
	if dsn == "" {
		t.Skip("set TIDB_FTS_E2E_DSN to verify collation-aware stopword lookup")
	}
	f := newFixture(t, dsn)
	previous := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(previous)
	var minSize, maxSize, gramSize int
	must(t, f.admin.QueryRowContext(context.Background(), "SELECT @@innodb_ft_min_token_size,@@innodb_ft_max_token_size,@@ngram_token_size").Scan(&minSize, &maxSize, &gramSize))
	documents := []string{"", "the", "The", "THE", "thé", "with", "WITH", "ｗｉｔｈ", "xá", "áx", "xＡ", "Ａx", "aaa", "AAA", "foo", "foo the zoo", "foo thé zoo", "foo xyz zoo", "foo a zoo", "数据库"}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	rows := []string{"VALUES (0,NULL)"}
	for i, text := range documents {
		rows = append(rows, fmt.Sprintf("VALUES (%d,%s)", i+1, quote(text)))
	}
	searches := []string{"+the", "+The", "+thé", "+with", "+ｗｉｔｈ", "+xá", "+aaa", `+"foo thé zoo"`}
	cases := 0
	for _, parser := range []model.FullTextParserType{model.FullTextParserTypeStandardV1, model.FullTextParserTypeNgramV1} {
		parserDDL := ""
		if parser == model.FullTextParserTypeNgramV1 {
			parserDDL = " WITH PARSER NGRAM"
		}
		for _, columnCollation := range []string{"utf8mb4_bin", "utf8mb4_general_ci"} {
			family := "stopword_" + strings.ToLower(string(parser)) + "_" + columnCollation
			f.makePair(family, "CREATE TABLE %s (id INT PRIMARY KEY,body TEXT COLLATE "+columnCollation+",FULLTEXT INDEX ft(body)"+parserDDL+")", rows)
			for _, serverCollation := range []string{"utf8mb4_bin", "utf8mb4_0900_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci", "utf8mb4_0900_ai_ci"} {
				for _, enabled := range []bool{false, true} {
					setting := "OFF"
					if enabled {
						setting = "ON"
					}
					for _, conn := range []*sql.Conn{f.local, f.native} {
						f.exec(conn, "SET SESSION innodb_ft_enable_stopword="+setting+",collation_server='"+serverCollation+"'")
					}
					config := fulltext.AnalyzerConfig{ParserType: parser, Collation: columnCollation, StopwordCollation: serverCollation, InnodbFtMinTokenSize: minSize, InnodbFtMaxTokenSize: maxSize, NgramTokenSize: gramSize, InnodbFtEnableStopword: enabled}
					analyzer, err := fulltext.GetAnalyzer(config)
					must(t, err)
					for _, search := range searches {
						query, err := fulltext.CompileBooleanQuery(search, config)
						must(t, err)
						want := make([]int, 0)
						for i, text := range documents {
							doc, err := fulltext.BuildDocument([]fulltext.ColumnInput{{Text: text}}, analyzer)
							must(t, err)
							if query.Match(doc) {
								want = append(want, i+1)
							}
						}
						f.check(family, matchCase{fmt.Sprintf("%s_%s_stopwords_%s/%s", family, serverCollation, setting, search), "MATCH(body) AGAINST(" + quote(search) + " IN BOOLEAN MODE)", want})
						cases++
					}
				}
			}
		}
	}
	t.Logf("verified %d stopword result/plan cases at NGRAM size %d", cases, gramSize)
}
