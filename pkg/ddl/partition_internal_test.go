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

package ddl

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/tici"
	"github.com/stretchr/testify/require"
)

func TestMarshalTiCIParserInfoKeyCanonicalizesStopwords(t *testing.T) {
	parserInfo1 := &tici.ParserInfo{
		ParserType: tici.ParserType_OTHER_PARSER,
		ParserParams: map[string]string{
			"parser_name":      "ngram",
			"ngram_token_size": "2",
		},
		StopWords: []string{"banana", "apple"},
	}
	parserInfo2 := &tici.ParserInfo{
		ParserType: tici.ParserType_OTHER_PARSER,
		ParserParams: map[string]string{
			"ngram_token_size": "2",
			"parser_name":      "ngram",
		},
		StopWords: []string{"apple", "banana"},
	}

	key1, err := marshalTiCIParserInfoKey(parserInfo1)
	require.NoError(t, err)
	key2, err := marshalTiCIParserInfoKey(parserInfo2)
	require.NoError(t, err)

	require.Equal(t, key1, key2)
	require.Len(t, key1, 64)
	require.NotContains(t, key1, "banana")
}

func TestMarshalTiCIParserInfoKeyNil(t *testing.T) {
	key, err := marshalTiCIParserInfoKey(nil)
	require.NoError(t, err)
	require.Equal(t, "nil", key)
}

func TestValidateTiCIAddPartitionParserConfigs(t *testing.T) {
	legacy := &model.TableInfo{Indices: []*model.IndexInfo{{
		Name: ast.NewCIStr("ft_legacy"),
		FullTextInfo: &model.FullTextIndexInfo{
			ParserType: model.FullTextParserTypeStandardV1,
		},
	}}}
	err := validateTiCIAddPartitionParserConfigs(legacy)
	require.ErrorContains(t, err, "analyzer snapshot is missing")
	require.ErrorContains(t, err, "ft_legacy")

	legacy.Indices[0].FullTextInfo.ParserConfig = &model.FullTextIndexParserConfig{
		ParserParams: map[string]string{
			"parser_name":                    "standard",
			vardef.InnodbFtMinTokenSize:      "3",
			vardef.InnodbFtMaxTokenSize:      "84",
			vardef.InnodbFtEnableStopword:    vardef.Off,
			vardef.InnodbFtUserStopwordTable: "",
		},
	}
	require.NoError(t, validateTiCIAddPartitionParserConfigs(legacy))
}

func TestLegacyTiCIAddPartitionRollbackDoesNotDependOnStopwordContents(t *testing.T) {
	job := &model.Job{State: model.JobStateRollingback, SessionVars: make(map[string]string)}
	job.AddSystemVars(vardef.InnodbFtMinTokenSize, "3")
	job.AddSystemVars(vardef.InnodbFtMaxTokenSize, "84")
	job.AddSystemVars(vardef.InnodbFtEnableStopword, vardef.On)
	job.AddSystemVars(vardef.InnodbFtUserStopwordTable, "test/stopwords")
	tblInfo := &model.TableInfo{Indices: []*model.IndexInfo{{
		ID:    42,
		Name:  ast.NewCIStr("ft_legacy"),
		State: model.StatePublic,
		FullTextInfo: &model.FullTextIndexInfo{
			ParserType: model.FullTextParserTypeStandardV1,
		},
	}}}

	// A nil worker/job context is intentional: rollback must not try to read
	// the external stopword table merely to reconstruct cleanup index IDs.
	groups, err := (*worker)(nil).buildTiCIAddPartitionGroups(nil, job, tblInfo)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.True(t, groups[0].mutableLegacyAnalyzer)
	groups = append(groups, ticiAddPartitionGroup{key: "stable-snapshot-hash", indexIDs: []int64{7}})
	require.Equal(t, []int64{7}, getTiCIAddedPartitionIndexIDs(
		&model.DDLReorgMeta{TiCIPartitionAddedGroups: []string{"stable-snapshot-hash"}}, groups,
	))

	// Simulate the hash persisted before the stopword table changed. It no
	// longer equals the synthetic rollback key, but still identifies the only
	// mutable legacy STANDARD group that may have completed its side effect.
	reorgMeta := &model.DDLReorgMeta{TiCIPartitionAddedGroups: []string{"old-stopword-snapshot-hash"}}
	require.Equal(t, []int64{42}, getTiCIAddedPartitionIndexIDs(reorgMeta, groups))
	reorgMeta.TiCIPartitionAddedGroups = append(reorgMeta.TiCIPartitionAddedGroups, "stable-snapshot-hash")
	require.Equal(t, []int64{7, 42}, getTiCIAddedPartitionIndexIDs(reorgMeta, groups))
}
