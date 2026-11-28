// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestT(t *testing.T) {
	abc := NewCIStr("aBC")
	require.Equal(t, "aBC", abc.O)
	require.Equal(t, "abc", abc.L)
	require.Equal(t, "aBC", abc.String())
}

func TestUnmarshalCIStr(t *testing.T) {
	var ci CIStr

	// Test unmarshal CIStr from a single string.
	str := "aaBB"
	buf, err := json.Marshal(str)
	require.NoError(t, err)
	require.NoError(t, ci.UnmarshalJSON(buf))
	require.Equal(t, str, ci.O)
	require.Equal(t, "aabb", ci.L)

	buf, err = json.Marshal(ci)
	require.NoError(t, err)
	require.Equal(t, `{"O":"aaBB","L":"aabb"}`, string(buf))
	require.NoError(t, ci.UnmarshalJSON(buf))
	require.Equal(t, str, ci.O)
	require.Equal(t, "aabb", ci.L)
}

func TestFullTextIndexInfoFlattenedJSON(t *testing.T) {
	expectedJSON := `{
		"parser_type":"NGRAM_V1",
		"index_fields":[{
			"column_id":1,
			"index_field_option":{
				"analyzer_type":"NGRAM_V1",
				"enable_bm25":true,
				"ngram":{
					"min_gram":2,
					"max_gram":3,
					"granularity":"CHAR",
					"lower_case":false
				}
			}
		}]
	}`
	info := FullTextIndexInfo{
		ParserType: FullTextParserTypeNgramV1,
		IndexFields: []FullTextIndexFieldInfo{
			{
				ColumnID: 1,
				IndexFieldOption: FullTextIndexFieldOption{
					AnalyzerType: FullTextParserTypeNgramV1,
					EnableBM25:   true,
					Ngram: &FullTextNgramOption{
						MinGram:     2,
						MaxGram:     3,
						Granularity: FullTextNgramGranularityChar,
						LowerCase:   false,
					},
				},
			},
		},
	}

	data, err := json.Marshal(&info)
	require.NoError(t, err)
	require.JSONEq(t, expectedJSON, string(data))

	var decoded FullTextIndexInfo
	require.NoError(t, json.Unmarshal([]byte(expectedJSON), &decoded))
	require.Equal(t, info, decoded)
	field, ok := decoded.PrimaryIndexFieldForColumn(1)
	require.True(t, ok)
	require.Equal(t, FullTextParserTypeNgramV1, field.IndexFieldOption.AnalyzerType)
}

func TestFullTextIndexInfoLegacyParserFallback(t *testing.T) {
	info := FullTextIndexInfo{ParserType: FullTextParserTypeStandardV1}

	field, ok := info.PrimaryIndexFieldForColumn(42)

	require.True(t, ok)
	require.Equal(t, int64(42), field.ColumnID)
	require.Equal(t, FullTextParserTypeStandardV1, field.IndexFieldOption.AnalyzerType)
	require.True(t, field.IndexFieldOption.EnableBM25)
}

func TestFullTextIndexInfoPrimaryFieldPreservesOrder(t *testing.T) {
	info := FullTextIndexInfo{
		IndexFields: []FullTextIndexFieldInfo{
			{
				ColumnID: 1,
				IndexFieldOption: FullTextIndexFieldOption{
					AnalyzerType: FullTextParserTypeMultilingualV1,
				},
			},
			{
				ColumnID: 1,
				IndexFieldOption: FullTextIndexFieldOption{
					AnalyzerType: FullTextParserTypeStandardV1,
				},
			},
		},
	}

	field, ok := info.PrimaryIndexFieldForColumn(1)

	require.True(t, ok)
	require.Same(t, &info.IndexFields[0], field)
	require.Equal(t, FullTextParserTypeMultilingualV1, info.AnalyzerTypeForColumn(1))
}

func TestFullTextParserSQLNames(t *testing.T) {
	testCases := []struct {
		parserType FullTextParserType
		sqlName    string
	}{
		{FullTextParserTypeStandardV1, "STANDARD"},
		{FullTextParserTypeMultilingualV1, "MULTILINGUAL"},
		{FullTextParserTypeNgramV1, "NGRAM"},
		{FullTextParserTypeExactV1, "EXACT"},
		{FullTextParserTypePathHierarchyV1, "PATH_HIERARCHY"},
	}
	for _, testCase := range testCases {
		require.Equal(t, testCase.sqlName, testCase.parserType.SQLName())
		require.Equal(t, testCase.parserType, GetFullTextParserTypeBySQLName(testCase.sqlName))
		require.Equal(t, testCase.parserType, GetFullTextParserTypeBySQLName(strings.ToLower(testCase.sqlName)))
		require.Equal(t, testCase.parserType, GetFullTextParserTypeByName(string(testCase.parserType)))
		require.Equal(t, testCase.parserType, GetFullTextParserTypeByName(strings.ToLower(string(testCase.parserType))))
	}
	require.Equal(t, FullTextParserTypeInvalid, GetFullTextParserTypeBySQLName("unknown"))
	require.Equal(t, FullTextParserTypeInvalid, GetFullTextParserTypeBySQLName("PREFIX"))
}

func TestFullTextIndexInfoMultipleFieldsForColumn(t *testing.T) {
	info := FullTextIndexInfo{
		IndexFields: []FullTextIndexFieldInfo{
			{
				ColumnID: 1,
				IndexFieldOption: FullTextIndexFieldOption{
					AnalyzerType: FullTextParserTypeExactV1,
					EnableBM25:   true,
				},
			},
			{
				ColumnID: 1,
				IndexFieldOption: FullTextIndexFieldOption{
					AnalyzerType: FullTextParserTypePathHierarchyV1,
					EnableBM25:   true,
					PathHierarchy: &FullTextPathHierarchyOption{
						Delimiter: "/",
					},
				},
			},
			{
				ColumnID: 2,
				IndexFieldOption: FullTextIndexFieldOption{
					AnalyzerType: FullTextParserTypeStandardV1,
				},
			},
		},
	}

	data, err := json.Marshal(&info)
	require.NoError(t, err)
	var decoded FullTextIndexInfo
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, info, decoded)

	fields := decoded.IndexFieldsForColumn(1)
	require.Len(t, fields, 2)
	require.Equal(t, FullTextParserTypeExactV1, fields[0].IndexFieldOption.AnalyzerType)
	require.Equal(t, FullTextParserTypePathHierarchyV1, fields[1].IndexFieldOption.AnalyzerType)
	require.Equal(t, "/", fields[1].IndexFieldOption.PathHierarchy.Delimiter)
	require.True(t, decoded.HasAnalyzerTypeForColumn(1, FullTextParserTypePathHierarchyV1))
	require.False(t, decoded.HasAnalyzerTypeForColumn(2, FullTextParserTypePathHierarchyV1))
}
