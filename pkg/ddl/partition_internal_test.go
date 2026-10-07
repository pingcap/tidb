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
