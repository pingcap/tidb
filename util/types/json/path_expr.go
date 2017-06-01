// Copyright 2017 PingCAP, Inc.
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

package json

import "regexp"

/*
	From MySQL 5.7, JSON path expression grammar:
		pathExpression ::= scope pathLeg (pathLeg)*
		scope ::= [ columnReference ] '$'
		columnReference ::= // omit...
		pathLeg ::= member | arrayLocation | '**'
		member ::= '.' (keyName | '*')
		arrayLocation ::= '[' (non-negative-integer | '*') ']'
		keyName ::= ECMAScript-identifier | ECMAScript-string-literal

	And some implemetion limits in MySQL 5.7:
		1) columnReference in scope must be empty now;
		2) double asterisk(**) could not be last leg;

	Examples:
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.a') -> "b"
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.c') -> [1, "2"]
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.a', '$.c') -> ["b", [1, "2"]]
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[0]') -> 1
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[2]') -> NULL
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[*]') -> [1, "2"]
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.*') -> ["b", [1, "2"]]
	TODO:
		1) add double asterisk support;
*/
var jsonPathExprLegRe = regexp.MustCompile(`(\.([a-zA-Z_][a-zA-Z0-9_]*|\*)|(\[([0-9]+|\*)\]))`)

func validateJSONPathExpr(pathExpr string) (indices [][]int, err error) {
	if pathExpr[0] != '$' {
		err = ErrInvalidJSONPath.GenByArgs(pathExpr)
		return
	}

	indices = jsonPathExprLegRe.FindAllStringIndex(pathExpr, -1)
	lastEnd := -1
	currentStart := -1
	for _, indice := range indices {
		currentStart = indice[0]
		if lastEnd > 0 {
			for idx := lastEnd; idx < currentStart; idx++ {
				c := pathExpr[idx]
				if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
					err = ErrInvalidJSONPath.GenByArgs(pathExpr)
					return
				}
			}
		}
		lastEnd = indice[1]
	}
	return
}
