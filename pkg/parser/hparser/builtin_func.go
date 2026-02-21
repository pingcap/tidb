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
// See the License for the specific language governing permissions and
// limitations under the License.

package hparser

import "strings"

// Builtin function token constants.
// These MUST match the values in parser_tokens.go (the generated token file).
const (
	builtinCast        = 58130
	builtinCount       = 58131
	builtinCurDate     = 58132
	builtinCurTime     = 58133
	builtinDateAdd     = 58134
	builtinDateSub     = 58135
	builtinGroupConcat = 58137
	builtinMax         = 58138
	builtinMin         = 58139
	builtinNow         = 58140
	builtinPosition    = 58141
	builtinSubstring   = 58145
	builtinSum         = 58146
	builtinTrim        = 58149

	// Additional common function tokens.
	builtinExtract    = 58136
	builtinStddevPop  = 58143
	builtinStddevSamp = 58144
	builtinVarPop     = 58151
	builtinVarSamp    = 58152
)

// builtinFuncName maps builtin function token types to function names.
// Returns empty string if the token is not a builtin function.
func builtinFuncName(tp int) string {
	switch tp {
	case builtinCount:
		return "count"
	case builtinSum:
		return "sum"
	case builtinMax:
		return "max"
	case builtinMin:
		return "min"
	case builtinGroupConcat:
		return "group_concat"
	default:
		return ""
	}
}

// isAggregateFunc returns true if the function name is an aggregate function.
func isAggregateFunc(name string) bool {
	switch strings.ToLower(name) {
	case "count", "sum", "avg", "max", "min", "group_concat",
		"bit_and", "bit_or", "bit_xor", "stddev_pop", "stddev_samp",
		"var_pop", "var_samp", "json_arrayagg", "json_objectagg",
		"std", "stddev", "variance",
		"approx_count_distinct", "approx_percentile":
		return true
	}
	return false
}
