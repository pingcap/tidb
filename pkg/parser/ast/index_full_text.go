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

package ast

import "strings"

// FullTextParserType is the tokenizer kind.
// Note: Must use UPPER_UNDER_SCORE naming convension.
type FullTextParserType string

// FullTextAnalyzerType is the analyzer kind stored in flattened full-text
// index-field metadata.
type FullTextAnalyzerType = FullTextParserType

const (
	// DefaultFullTextNgramMinGram is the default min gram for NGRAM parser.
	DefaultFullTextNgramMinGram uint64 = 3
	// DefaultFullTextNgramMaxGram is the default max gram for NGRAM parser.
	DefaultFullTextNgramMaxGram uint64 = 3
	// DefaultFullTextNgramLowerCase controls whether the NGRAM parser lowercases text by default.
	DefaultFullTextNgramLowerCase = true
	// DefaultFullTextPathHierarchyDelimiter is the default delimiter for PATH_HIERARCHY parser.
	DefaultFullTextPathHierarchyDelimiter = "/"

	// FullTextParserTypeInvalid is the invalid tokenizer
	FullTextParserTypeInvalid FullTextParserType = "INVALID"
	// FullTextParserTypeStandard is the standard parser, for English texts
	// The value matches with the supported tokenizer in Libclara.
	FullTextParserTypeStandardV1 FullTextParserType = "STANDARD_V1"
	// FullTextParserTypeMultilingual is a parser for multilingual texts
	// The value matches with the supported tokenizer in Libclara.
	FullTextParserTypeMultilingualV1 FullTextParserType = "MULTILINGUAL_V1"
	// FullTextParserTypeNgramV1 is an Ngram parser.
	// The value matches the parser type stored in CSE metadata.
	FullTextParserTypeNgramV1 FullTextParserType = "NGRAM_V1"
	// FullTextParserTypeExactV1 indexes the whole input as one exact token.
	FullTextParserTypeExactV1 FullTextParserType = "EXACT_VALUE_V1"
	// FullTextParserTypePathHierarchyV1 indexes delimiter-separated path prefixes.
	FullTextParserTypePathHierarchyV1 FullTextParserType = "PATH_HIERARCHY_V1"
)

// FullTextNgramGranularity is the token generation mode for NGRAM parser.
type FullTextNgramGranularity string

const (
	// FullTextNgramGranularityWord tokenizes by word before generating ngrams.
	FullTextNgramGranularityWord FullTextNgramGranularity = "WORD"
	// FullTextNgramGranularityChar generates ngrams on character windows.
	FullTextNgramGranularityChar FullTextNgramGranularity = "CHAR"
)

// DefaultFullTextNgramGranularity is the default granularity for NGRAM parser.
const DefaultFullTextNgramGranularity = FullTextNgramGranularityWord

// GetFullTextNgramGranularityBySQLName returns the NGRAM granularity by a SQL name.
func GetFullTextNgramGranularityBySQLName(name string) (FullTextNgramGranularity, bool) {
	switch strings.ToUpper(name) {
	case string(FullTextNgramGranularityWord):
		return FullTextNgramGranularityWord, true
	case string(FullTextNgramGranularityChar):
		return FullTextNgramGranularityChar, true
	default:
		return "", false
	}
}

func (t FullTextParserType) SQLName() string {
	switch t {
	case FullTextParserTypeStandardV1:
		return "STANDARD"
	case FullTextParserTypeMultilingualV1:
		return "MULTILINGUAL"
	case FullTextParserTypeNgramV1:
		return "NGRAM"
	case FullTextParserTypeExactV1:
		return "EXACT"
	case FullTextParserTypePathHierarchyV1:
		return "PATH_HIERARCHY"
	default:
		return "INVALID"
	}
}

// GetFullTextParserTypeBySQLName returns the FullTextParserType by a SQL name.
func GetFullTextParserTypeBySQLName(name string) FullTextParserType {
	switch strings.ToUpper(name) {
	case "STANDARD":
		return FullTextParserTypeStandardV1
	case "MULTILINGUAL":
		return FullTextParserTypeMultilingualV1
	case "NGRAM":
		return FullTextParserTypeNgramV1
	case "EXACT":
		return FullTextParserTypeExactV1
	case "PATH_HIERARCHY":
		return FullTextParserTypePathHierarchyV1
	default:
		return FullTextParserTypeInvalid
	}
}

// GetFullTextParserTypeByName resolves either a SQL parser name such as NGRAM
// or a persisted parser type such as NGRAM_V1.
func GetFullTextParserTypeByName(name string) FullTextParserType {
	if parserType := GetFullTextParserTypeBySQLName(name); parserType != FullTextParserTypeInvalid {
		return parserType
	}
	switch FullTextParserType(strings.ToUpper(name)) {
	case FullTextParserTypeStandardV1:
		return FullTextParserTypeStandardV1
	case FullTextParserTypeMultilingualV1:
		return FullTextParserTypeMultilingualV1
	case FullTextParserTypeNgramV1:
		return FullTextParserTypeNgramV1
	case FullTextParserTypeExactV1:
		return FullTextParserTypeExactV1
	case FullTextParserTypePathHierarchyV1:
		return FullTextParserTypePathHierarchyV1
	default:
		return FullTextParserTypeInvalid
	}
}

// FullTextIndexInfo is the information of a FULLTEXT index.
type FullTextIndexInfo struct {
	// ParserType is the legacy parser metadata for indexes that have one parser
	// shared by all indexed columns.
	ParserType FullTextParserType `json:"parser_type,omitempty"`
	// IndexFields is the canonical analyzer metadata. Entries preserve the SQL
	// column declaration order and, within each column, the parser declaration order.
	// Each array position is a Tantivy field ID.
	IndexFields []FullTextIndexFieldInfo `json:"index_fields,omitempty"`
}

// FullTextIndexFieldInfo is one field in a full-text index.
type FullTextIndexFieldInfo struct {
	ColumnID         int64                    `json:"column_id"`
	IndexFieldOption FullTextIndexFieldOption `json:"index_field_option"`
}

// FullTextIndexFieldOption controls how one full-text index field is built.
type FullTextIndexFieldOption struct {
	AnalyzerType  FullTextAnalyzerType         `json:"analyzer_type"`
	EnableBM25    bool                         `json:"enable_bm25"`
	Ngram         *FullTextNgramOption         `json:"ngram,omitempty"`
	PathHierarchy *FullTextPathHierarchyOption `json:"path_hierarchy,omitempty"`
}

// FullTextNgramOption is the typed parameter set for an NGRAM analyzer.
type FullTextNgramOption struct {
	MinGram     uint64                   `json:"min_gram"`
	MaxGram     uint64                   `json:"max_gram"`
	Granularity FullTextNgramGranularity `json:"granularity"`
	LowerCase   bool                     `json:"lower_case"`
}

// FullTextPathHierarchyOption is the parameter set for a path-hierarchy analyzer.
type FullTextPathHierarchyOption struct {
	Delimiter string `json:"delimiter"`
}

// PrimaryIndexFieldForColumn returns the first index field for a column.
func (i *FullTextIndexInfo) PrimaryIndexFieldForColumn(columnID int64) (*FullTextIndexFieldInfo, bool) {
	if i == nil {
		return nil, false
	}
	if len(i.IndexFields) > 0 {
		for fieldIdx := range i.IndexFields {
			indexField := &i.IndexFields[fieldIdx]
			if indexField.ColumnID == columnID {
				return indexField, true
			}
		}
		return nil, false
	}
	if i.ParserType == "" || i.ParserType == FullTextParserTypeInvalid {
		return nil, false
	}
	return &FullTextIndexFieldInfo{
		ColumnID: columnID,
		IndexFieldOption: FullTextIndexFieldOption{
			AnalyzerType: i.ParserType,
			EnableBM25:   true,
		},
	}, true
}

// IndexFieldsForColumn returns all index fields for a column in analyzer order.
// Legacy metadata is expanded into one synthetic field.
func (i *FullTextIndexInfo) IndexFieldsForColumn(columnID int64) []FullTextIndexFieldInfo {
	if i == nil {
		return nil
	}
	if len(i.IndexFields) == 0 {
		field, ok := i.PrimaryIndexFieldForColumn(columnID)
		if !ok {
			return nil
		}
		return []FullTextIndexFieldInfo{*field}
	}
	fields := make([]FullTextIndexFieldInfo, 0, 1)
	for _, field := range i.IndexFields {
		if field.ColumnID == columnID {
			fields = append(fields, field)
		}
	}
	return fields
}

// HasAnalyzerTypeForColumn reports whether a column has the specified analyzer.
func (i *FullTextIndexInfo) HasAnalyzerTypeForColumn(columnID int64, analyzer FullTextAnalyzerType) bool {
	for _, field := range i.IndexFieldsForColumn(columnID) {
		if field.IndexFieldOption.AnalyzerType == analyzer {
			return true
		}
	}
	return false
}

// AnalyzerTypeForColumn returns the primary analyzer type for a column.
func (i *FullTextIndexInfo) AnalyzerTypeForColumn(columnID int64) FullTextAnalyzerType {
	indexField, ok := i.PrimaryIndexFieldForColumn(columnID)
	if !ok {
		return FullTextParserTypeInvalid
	}
	return indexField.IndexFieldOption.AnalyzerType
}
