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

package fulltext

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestBuildLocalMatchAgainstBooleanQuery(t *testing.T) {
	query, err := BuildLocalMatchAgainstBooleanQuery(`+cat -dog "red fox" pre*`, model.FullTextParserTypeStandardV1)
	require.NoError(t, err)
	require.Equal(t, LocalMatchAgainstProtocolVersion, query.GetVersion())
	require.Equal(t, tipb.LocalMatchAgainstParser_LocalMatchAgainstParserStandard, query.GetParser())
	require.Len(t, query.GetNodes(), 4)

	require.Equal(t, tipb.LocalMatchAgainstBooleanOccur_LocalMatchAgainstBooleanOccurMust, query.GetNodes()[0].GetOccur())
	require.Equal(t, tipb.LocalMatchAgainstBooleanTermType_LocalMatchAgainstBooleanTermWord, query.GetNodes()[0].GetTermType())
	require.Equal(t, "cat", query.GetNodes()[0].GetText())

	require.Equal(t, tipb.LocalMatchAgainstBooleanOccur_LocalMatchAgainstBooleanOccurShould, query.GetNodes()[1].GetOccur())
	require.Equal(t, tipb.LocalMatchAgainstBooleanTermType_LocalMatchAgainstBooleanTermPhrase, query.GetNodes()[1].GetTermType())
	require.Equal(t, "red fox", query.GetNodes()[1].GetText())

	require.Equal(t, tipb.LocalMatchAgainstBooleanOccur_LocalMatchAgainstBooleanOccurShould, query.GetNodes()[2].GetOccur())
	require.Equal(t, tipb.LocalMatchAgainstBooleanTermType_LocalMatchAgainstBooleanTermPrefix, query.GetNodes()[2].GetTermType())
	require.Equal(t, "pre", query.GetNodes()[2].GetText())

	require.Equal(t, tipb.LocalMatchAgainstBooleanOccur_LocalMatchAgainstBooleanOccurMustNot, query.GetNodes()[3].GetOccur())
	require.Equal(t, "dog", query.GetNodes()[3].GetText())
}

func TestBuildLocalMatchAgainstBooleanQueryWithAnalyzerConfig(t *testing.T) {
	query, err := BuildLocalMatchAgainstBooleanQueryWithAnalyzerConfig("+cat -dog", AnalyzerConfig{
		ParserType:             model.FullTextParserTypeStandardV1,
		StopwordCollation:      "utf8mb4_general_ci",
		InnodbFtMinTokenSize:   1,
		InnodbFtMaxTokenSize:   16,
		InnodbFtEnableStopword: false,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(1), query.GetInnodbFtMinTokenSize())
	require.Equal(t, uint32(16), query.GetInnodbFtMaxTokenSize())
	require.Equal(t, tipb.LocalMatchAgainstStopwordMode_LocalMatchAgainstStopwordModeDisabled, query.GetStopwordMode())
	require.Equal(t, tipb.LocalMatchAgainstParser_LocalMatchAgainstParserStandard, query.GetParser())
	require.Equal(t, "utf8mb4_general_ci", query.GetStopwordCollation())

	_, err = BuildLocalMatchAgainstBooleanQueryWithAnalyzerConfig("cat", AnalyzerConfig{
		ParserType:           model.FullTextParserTypeStandardV1,
		InnodbFtMinTokenSize: 5,
		InnodbFtMaxTokenSize: 0,
	})
	require.Error(t, err)

	query, err = BuildLocalMatchAgainstBooleanQueryWithAnalyzerConfig("cat", AnalyzerConfig{
		ParserType:           model.FullTextParserTypeStandardV1,
		InnodbFtMinTokenSize: 16,
		InnodbFtMaxTokenSize: 10,
	})
	require.NoError(t, err, "min > max is a valid TiDB configuration that analyzes no standard tokens")

	query, err = BuildLocalMatchAgainstBooleanQueryWithAnalyzerConfig("+the", AnalyzerConfig{
		ParserType:             model.FullTextParserTypeNgramV1,
		NgramTokenSize:         3,
		InnodbFtEnableStopword: true,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(3), query.GetNgramTokenSize())
	require.Equal(t, tipb.LocalMatchAgainstStopwordMode_LocalMatchAgainstStopwordModeBuiltin, query.GetStopwordMode(), "NGRAM must receive the stopword setting too")

	query, err = BuildLocalMatchAgainstBooleanQueryWithAnalyzerConfig("+the", AnalyzerConfig{
		ParserType:             model.FullTextParserTypeNgramV1,
		NgramTokenSize:         3,
		InnodbFtEnableStopword: false,
	})
	require.NoError(t, err)
	require.Equal(t, tipb.LocalMatchAgainstStopwordMode_LocalMatchAgainstStopwordModeDisabled, query.GetStopwordMode())
}

func TestBuildLocalMatchAgainstBooleanQuerySupportsNgramAndRejectsUnsupportedSyntax(t *testing.T) {
	query, err := BuildLocalMatchAgainstBooleanQueryWithNgramTokenSize("+数据库 -mysql", model.FullTextParserTypeNgramV1, 2)
	require.NoError(t, err)
	require.Equal(t, tipb.LocalMatchAgainstParser_LocalMatchAgainstParserNgram, query.GetParser())
	require.Equal(t, uint32(2), query.GetNgramTokenSize())
	require.Len(t, query.GetNodes(), 2)
	require.Equal(t, tipb.LocalMatchAgainstBooleanOccur_LocalMatchAgainstBooleanOccurMust, query.GetNodes()[0].GetOccur())
	require.Equal(t, "数据库", query.GetNodes()[0].GetText())
	require.Equal(t, tipb.LocalMatchAgainstBooleanOccur_LocalMatchAgainstBooleanOccurMustNot, query.GetNodes()[1].GetOccur())
	require.Equal(t, "mysql", query.GetNodes()[1].GetText())

	_, err = BuildLocalMatchAgainstBooleanQuery("(cat)", model.FullTextParserTypeStandardV1)
	require.Error(t, err)
	for _, unsupported := range []string{">cat", "<cat", "~cat", `"cat dog"@2`} {
		_, err = BuildLocalMatchAgainstBooleanQuery(unsupported, model.FullTextParserTypeStandardV1)
		require.Error(t, err, "unsupported Boolean extensions must not enter the scalar wire protocol: %s", unsupported)
	}
}
