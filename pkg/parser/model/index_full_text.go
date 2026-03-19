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

package model

import "strings"

// FullTextParserType is the tokenizer kind.
// Note: Must use UPPER_UNDER_SCORE naming convension.
type FullTextParserType string

const (
	// FullTextParserTypeInvalid is the invalid tokenizer
	FullTextParserTypeInvalid FullTextParserType = "INVALID"
	// FullTextParserTypeStandard is the standard parser, for English texts
	// The value matches with the supported tokenizer in Libclara.
	FullTextParserTypeStandardV1 FullTextParserType = "STANDARD_V1"
	// FullTextParserTypeMultilingual is a parser for multilingual texts
	// The value matches with the supported tokenizer in Libclara.
	FullTextParserTypeMultilingualV1 FullTextParserType = "MULTILINGUAL_V1"
)

func (t FullTextParserType) SQLName() string {
	switch t {
	case FullTextParserTypeStandardV1:
		return "STANDARD"
	case FullTextParserTypeMultilingualV1:
		return "MULTILINGUAL"
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
	default:
		return FullTextParserTypeInvalid
	}
}

// FullTextIndexInfo is the information of FULLTEXT index of a column.
type FullTextIndexInfo struct {
	ParserType FullTextParserType `json:"parser_type"`
	// TODO: Add other options
}
