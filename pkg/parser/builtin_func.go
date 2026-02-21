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

package parser

import "strings"

// Builtin function token constants.
// These MUST match the values in parser_tokens.go (the generated token file).
const (
	builtinFnCast        = builtinCast
	builtinFnCount       = builtinCount
	builtinFnCurDate     = builtinCurDate
	builtinFnCurTime     = builtinCurTime
	builtinFnDateAdd     = builtinDateAdd
	builtinFnDateSub     = builtinDateSub
	builtinFnGroupConcat = builtinGroupConcat
	builtinFnMax         = builtinMax
	builtinFnMin         = builtinMin
	builtinFnNow         = builtinNow
	builtinFnPosition    = builtinPosition
	builtinFnSubstring   = builtinSubstring
	builtinFnSum         = builtinSum
	builtinFnTrim        = builtinTrim

	// Additional common function tokens.
	builtinFnExtract    = builtinExtract
	builtinFnStddevPop  = builtinStddevPop
	builtinFnStddevSamp = builtinStddevSamp
	builtinFnVarPop     = builtinVarPop
	builtinFnVarSamp    = builtinVarSamp
)

// builtinFuncName maps builtin function token types to function names.
// Returns empty string if the token is not a builtin function.
func builtinFuncName(tp int) string {
	switch tp {
	case builtinFnCount:
		return "count"
	case builtinFnSum:
		return "sum"
	case builtinFnMax:
		return "max"
	case builtinFnMin:
		return "min"
	case builtinFnGroupConcat:
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
