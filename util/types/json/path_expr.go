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

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/juju/errors"
)

/*
	From MySQL 5.7, JSON path expression grammar:
		pathExpression ::= scope (pathLeg)*
		scope ::= [ columnReference ] '$'
		columnReference ::= // omit...
		pathLeg ::= member | arrayLocation | '**'
		member ::= '.' (keyName | '*')
		arrayLocation ::= '[' (non-negative-integer | '*') ']'
		keyName ::= ECMAScript-identifier | ECMAScript-string-literal

	And some implementation limits in MySQL 5.7:
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
		2) support '\"' in string literal;
*/
var jsonPathExprLegRe = regexp.MustCompile(`(\.\s*([a-zA-Z_][a-zA-Z0-9_]*|\*|"[^"]*")|(\[\s*([0-9]+|\*)\s*\]))`)

// pathLeg is only used by PathExpression.
type pathLeg struct {
	isArrayIndex bool   // the leg is an array index or not.
	arrayIndex   int    // if isArrayIndex is true, the value should be parsed into here.
	dotKey       string // if isArrayIndex is false, the key should be parsed into here.
}

// arrayIndexAsterisk is for parsing `*` into a number.
// we need this number represent "all".
const arrayIndexAsterisk int = -1

// pathExpressionFlag holds attributes of PathExpression
type pathExpressionFlag byte

const (
	pathExpressionContainsAsterisk       pathExpressionFlag = 0x01
	pathExpressionContainsDoubleAsterisk pathExpressionFlag = 0x02
)

func (pef pathExpressionFlag) containsAnyAsterisk() bool {
	pef &= pathExpressionContainsAsterisk
	pef &= pathExpressionContainsDoubleAsterisk
	return byte(pef) != 0
}

// PathExpression is for JSON path expression.
type PathExpression struct {
	legs  []pathLeg
	flags pathExpressionFlag
}

// ParseJSONPathExpr parses a JSON path expression. Returns a PathExpression
// object which can be used in JSON_EXTRACT, JSON_SET and so on.
func ParseJSONPathExpr(pathExpr string) (pe PathExpression, err error) {
	dollarIndex := strings.Index(pathExpr, "$")
	if dollarIndex < 0 {
		err = ErrInvalidJSONPath.GenByArgs(pathExpr)
		return
	}
	for i := 0; i < dollarIndex; i++ {
		if !isBlank(rune(pathExpr[i])) {
			err = ErrInvalidJSONPath.GenByArgs(pathExpr)
			return
		}
	}

	pathExprSuffix := pathExpr[dollarIndex+1:]
	indices := jsonPathExprLegRe.FindAllStringIndex(pathExprSuffix, -1)

	pe.legs = make([]pathLeg, 0, len(indices))
	pe.flags = pathExpressionFlag(0)

	lastEnd, currentStart := 0, 0
	for _, indice := range indices {
		currentStart = indice[0]
		for i := lastEnd; i < currentStart; i++ {
			if !isBlank(rune(pathExprSuffix[i])) {
				err = ErrInvalidJSONPath.GenByArgs(pathExpr)
				return
			}
		}
		lastEnd = indice[1]

		if pathExprSuffix[indice[0]] == '[' {
			var leg = strings.TrimFunc(pathExprSuffix[indice[0]+1:indice[1]], isBlank)
			var indexStr = strings.TrimFunc(leg[0:len(leg)-1], isBlank)
			var index int
			if len(indexStr) == 1 && indexStr[0] == '*' {
				pe.flags |= pathExpressionContainsAsterisk
				index = arrayIndexAsterisk
			} else {
				if index, err = strconv.Atoi(indexStr); err != nil {
					err = errors.Trace(err)
					return
				}
			}
			pe.legs = append(pe.legs, pathLeg{isArrayIndex: true, arrayIndex: index})
		} else {
			var key = strings.TrimFunc(pathExprSuffix[indice[0]+1:indice[1]], isBlank)
			if len(key) == 1 && key[0] == '*' {
				pe.flags |= pathExpressionContainsAsterisk
			} else if key[0] == '"' {
				key = key[1 : len(key)-1]
			}
			pe.legs = append(pe.legs, pathLeg{isArrayIndex: false, dotKey: key})
		}
	}
	return
}

func isBlank(c rune) bool {
	if c == '\n' || c == '\r' || c == '\t' || c == ' ' {
		return true
	}
	return false
}
