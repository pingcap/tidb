// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ftse2e

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/tests/ftse2e/benchdata"
)

// These microbenchmarks do not start a cluster. They measure analyzer+matcher
// work (no SQL planning, scan, network or MPP), not end-to-end acceleration.
func BenchmarkLocalMatchRows(b *testing.B) {
	previous := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(previous)
	for _, parser := range []struct {
		name string
		kind model.FullTextParserType
		size int
	}{{"standard", model.FullTextParserTypeStandardV1, 2}, {"ngram2", model.FullTextParserTypeNgramV1, 2}, {"ngram3", model.FullTextParserTypeNgramV1, 3}} {
		for _, collation := range []string{"utf8mb4_bin", "utf8mb4_general_ci"} {
			for _, stopwords := range []bool{false, true} {
				config := fulltext.AnalyzerConfig{ParserType: parser.kind, NgramTokenSize: parser.size,
					InnodbFtMinTokenSize: 3, InnodbFtMaxTokenSize: 84, InnodbFtEnableStopword: stopwords,
					Collation: collation, StopwordCollation: collation}
				for _, search := range []string{`+quick -slow`, `+"quick brown fox"`, `+pre*`, `+the quick`} {
					for _, size := range []int{128, 4096, 262144} {
						name := fmt.Sprintf("%s/%s/stopword=%t/%s/bytes=%d", parser.name, collation, stopwords, search, size)
						b.Run(name, func(b *testing.B) {
							analyzer, err := fulltext.GetAnalyzer(config)
							if err != nil {
								b.Fatal(err)
							}
							query, err := fulltext.CompileBooleanQuery(search, config)
							if err != nil {
								b.Fatal(err)
							}
							var inputs [4][]fulltext.ColumnInput
							for row := range inputs {
								inputs[row] = []fulltext.ColumnInput{{Text: benchdata.Document(row, size)}}
							}
							b.ReportAllocs()
							b.SetBytes(int64(size))
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								if query.MatchesNothing() {
									continue
								}
								doc, err := fulltext.BuildDocument(inputs[i%4], analyzer)
								if err != nil {
									b.Fatal(err)
								}
								_ = query.Match(doc)
							}
							b.StopTimer()
						})
					}
				}
			}
		}
	}
}

func BenchmarkLocalMatchPlanning(b *testing.B) {
	config := fulltext.AnalyzerConfig{ParserType: model.FullTextParserTypeStandardV1,
		InnodbFtMinTokenSize: 3, InnodbFtMaxTokenSize: 84, Collation: "utf8mb4_bin"}
	for _, terms := range []int{1, 64} {
		search := strings.Repeat("+quick -slow pre* ", terms)
		for _, shared := range []bool{false, true} {
			b.Run(fmt.Sprintf("terms=%d/shared=%t", terms, shared), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if shared {
						group, err := fulltext.ParseBooleanQuery(search, config.ParserType)
						if err != nil {
							b.Fatal(err)
						}
						if _, err = fulltext.CompileParsedBooleanQuery(group, config); err != nil {
							b.Fatal(err)
						}
						if _, err = fulltext.BuildParsedLocalMatchAgainstBooleanQuery(group, config); err != nil {
							b.Fatal(err)
						}
					} else {
						if _, err := fulltext.CompileBooleanQuery(search, config); err != nil {
							b.Fatal(err)
						}
						if _, err := fulltext.BuildLocalMatchAgainstBooleanQueryWithAnalyzerConfig(search, config); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}
