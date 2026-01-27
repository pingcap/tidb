// Copyright 2025 PingCAP, Inc.
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

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseBooleanSearchString(t *testing.T) {
	tests := []struct {
		input    string
		expected []searchTerm
	}{
		{
			input: "+apple +pie",
			expected: []searchTerm{
				{word: "apple", isRequired: true},
				{word: "pie", isRequired: true},
			},
		},
		{
			input: "+apple -cherry",
			expected: []searchTerm{
				{word: "apple", isRequired: true},
				{word: "cherry", isExcluded: true},
			},
		},
		{
			input: "apple*",
			expected: []searchTerm{
				{word: "apple"},
			},
		},
		{
			input: `"exact phrase"`,
			expected: []searchTerm{
				{word: "exact phrase"},
			},
		},
		{
			input: `+database +mysql -oracle "full text"`,
			expected: []searchTerm{
				{word: "database", isRequired: true},
				{word: "mysql", isRequired: true},
				{word: "oracle", isExcluded: true},
				{word: "full text"},
			},
		},
		{
			input: "word1 word2 word3",
			expected: []searchTerm{
				{word: "word1"},
				{word: "word2"},
				{word: "word3"},
			},
		},
		{
			input: "+word1* -word2",
			expected: []searchTerm{
				{word: "word1", isRequired: true},
				{word: "word2", isExcluded: true},
			},
		},
		{
			input: `"unclosed quote`,
			expected: []searchTerm{
				{word: "unclosed quote"},
			},
		},
		{
			input: "word1\t\nword2",
			expected: []searchTerm{
				{word: "word1"},
				{word: "word2"},
			},
		},
		{
			input: `+"required phrase"`,
			expected: []searchTerm{
				{word: "required phrase", isRequired: true},
			},
		},
		{
			input: `-"excluded phrase"`,
			expected: []searchTerm{
				{word: "excluded phrase", isExcluded: true},
			},
		},
		{
			input: `+"required phrase" optional -"excluded phrase"`,
			expected: []searchTerm{
				{word: "required phrase", isRequired: true},
				{word: "optional"},
				{word: "excluded phrase", isExcluded: true},
			},
		},
		{
			input: `+word1 +"required phrase" -word2 -"excluded phrase"`,
			expected: []searchTerm{
				{word: "word1", isRequired: true},
				{word: "required phrase", isRequired: true},
				{word: "word2", isExcluded: true},
				{word: "excluded phrase", isExcluded: true},
			},
		},
		{
			input: `abc"phrase"`,
			expected: []searchTerm{
				{word: "abc"},
				{word: "phrase"},
			},
		},
		{
			input: `word1 abc"phrase" word2`,
			expected: []searchTerm{
				{word: "word1"},
				{word: "abc"},
				{word: "phrase"},
				{word: "word2"},
			},
		},
		{
			input: `+"unclosed`,
			expected: []searchTerm{
				{word: "unclosed", isRequired: true},
			},
		},
		{
			input: `-"unclosed phrase`,
			expected: []searchTerm{
				{word: "unclosed phrase", isExcluded: true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseBooleanSearchString(tt.input)
			require.Equal(t, len(tt.expected), len(result), "Number of terms should match")
			for i, expected := range tt.expected {
				require.Equal(t, expected.word, result[i].word, "Word should match")
				require.Equal(t, expected.isRequired, result[i].isRequired, "isRequired should match")
				require.Equal(t, expected.isExcluded, result[i].isExcluded, "isExcluded should match")
			}
		})
	}
}

func TestParseSearchTerm(t *testing.T) {
	tests := []struct {
		input    string
		expected searchTerm
	}{
		{
			input:    "+word",
			expected: searchTerm{word: "word", isRequired: true},
		},
		{
			input:    "-word",
			expected: searchTerm{word: "word", isExcluded: true},
		},
		{
			input:    "word*",
			expected: searchTerm{word: "word"},
		},
		{
			input:    "+word*",
			expected: searchTerm{word: "word", isRequired: true},
		},
		{
			input:    "word",
			expected: searchTerm{word: "word"},
		},
		{
			input:    "",
			expected: searchTerm{word: ""},
		},
		{
			input:    "+*",
			expected: searchTerm{word: "", isRequired: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseSearchTerm(tt.input)
			require.Equal(t, tt.expected.word, result.word, "Word should match")
			require.Equal(t, tt.expected.isRequired, result.isRequired, "isRequired should match")
			require.Equal(t, tt.expected.isExcluded, result.isExcluded, "isExcluded should match")
		})
	}
}

func TestEscapeLikePattern(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "normal text",
			expected: "normal text",
		},
		{
			input:    "100%",
			expected: "100\\%",
		},
		{
			input:    "test_file",
			expected: "test\\_file",
		},
		{
			input:    "path\\to\\file",
			expected: "path\\\\to\\\\file",
		},
		{
			input:    "mix_%_all",
			expected: "mix\\_\\%\\_all",
		},
		{
			input:    "\\%_",
			expected: "\\\\\\%\\_",
		},
		{
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := escapeLikePattern(tt.input)
			require.Equal(t, tt.expected, result, "Escaped pattern should match")
		})
	}
}
