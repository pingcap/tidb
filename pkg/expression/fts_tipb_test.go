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

package expression

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestBuildFTSBooleanQuery(t *testing.T) {
	query, err := BuildFTSBooleanQuery(`+cat -dog "red fox" pre*`, model.FullTextParserTypeStandardV1)
	require.NoError(t, err)
	require.Len(t, query.GetNodes(), 4)

	require.Equal(t, tipb.FTSBooleanOccur_FTSBooleanOccurMust, query.GetNodes()[0].GetOccur())
	require.Equal(t, tipb.FTSBooleanTermType_FTSBooleanTermWord, query.GetNodes()[0].GetTerm().GetTermType())
	require.Equal(t, "cat", query.GetNodes()[0].GetTerm().GetText())

	require.Equal(t, tipb.FTSBooleanOccur_FTSBooleanOccurShould, query.GetNodes()[1].GetOccur())
	require.Equal(t, tipb.FTSBooleanTermType_FTSBooleanTermPhrase, query.GetNodes()[1].GetTerm().GetTermType())
	require.Equal(t, "red fox", query.GetNodes()[1].GetTerm().GetText())

	require.Equal(t, tipb.FTSBooleanOccur_FTSBooleanOccurShould, query.GetNodes()[2].GetOccur())
	require.Equal(t, tipb.FTSBooleanTermType_FTSBooleanTermPrefix, query.GetNodes()[2].GetTerm().GetTermType())
	require.Equal(t, "pre", query.GetNodes()[2].GetTerm().GetText())

	require.Equal(t, tipb.FTSBooleanOccur_FTSBooleanOccurMustNot, query.GetNodes()[3].GetOccur())
	require.Equal(t, "dog", query.GetNodes()[3].GetTerm().GetText())
}

func TestBuildFTSBooleanQuerySupportsNgramAndRejectsUnsupportedSyntax(t *testing.T) {
	query, err := BuildFTSBooleanQueryWithNgramTokenSize("+数据库 -mysql", model.FullTextParserTypeNgramV1, 2)
	require.NoError(t, err)
	require.Equal(t, "NGRAM_V1", query.GetQueryTokenizer())
	require.Equal(t, uint32(2), query.GetNgramTokenSize())
	require.Len(t, query.GetNodes(), 2)
	require.Equal(t, tipb.FTSBooleanOccur_FTSBooleanOccurMust, query.GetNodes()[0].GetOccur())
	require.Equal(t, "数据库", query.GetNodes()[0].GetTerm().GetText())
	require.Equal(t, tipb.FTSBooleanOccur_FTSBooleanOccurMustNot, query.GetNodes()[1].GetOccur())
	require.Equal(t, "mysql", query.GetNodes()[1].GetTerm().GetText())

	_, err = BuildFTSBooleanQuery("(cat)", model.FullTextParserTypeStandardV1)
	require.Error(t, err)
}
