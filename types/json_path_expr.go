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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
)

/*
	From MySQL 5.7, JSON path expression grammar:
		pathExpression ::= scope (jsonPathLeg)*
		scope ::= [ columnReference ] '$'
		columnReference ::= // omit...
		jsonPathLeg ::= member | arrayLocation | '**'
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
*/

type jsonPathLegType byte

const (
	// jsonPathLegKey indicates the path leg with '.key'.
	jsonPathLegKey jsonPathLegType = 0x01
	// jsonPathLegIndex indicates the path leg with form '[number]'.
	jsonPathLegIndex jsonPathLegType = 0x02
	// jsonPathLegDoubleAsterisk indicates the path leg with form '**'.
	jsonPathLegDoubleAsterisk jsonPathLegType = 0x03
)

// jsonPathLeg is only used by JSONPathExpression.
type jsonPathLeg struct {
	typ        jsonPathLegType
	arrayIndex int    // if typ is jsonPathLegIndex, the value should be parsed into here.
	dotKey     string // if typ is jsonPathLegKey, the key should be parsed into here.
}

// arrayIndexAsterisk is for parsing `*` into a number.
// we need this number represent "all".
const arrayIndexAsterisk = -1

// jsonPathExpressionFlag holds attributes of JSONPathExpression
type jsonPathExpressionFlag byte

const (
	jsonPathExpressionContainsAsterisk       jsonPathExpressionFlag = 0x01
	jsonPathExpressionContainsDoubleAsterisk jsonPathExpressionFlag = 0x02
)

// containsAnyAsterisk returns true if pef contains any asterisk.
func (pef jsonPathExpressionFlag) containsAnyAsterisk() bool {
	pef &= jsonPathExpressionContainsAsterisk | jsonPathExpressionContainsDoubleAsterisk
	return byte(pef) != 0
}

// JSONPathExpression is for JSON path expression.
type JSONPathExpression struct {
	legs  []jsonPathLeg
	flags jsonPathExpressionFlag
}

var peCache JSONPathExpressionCache

type jsonPathExpressionKey string

func (key jsonPathExpressionKey) Hash() []byte {
	return hack.Slice(string(key))
}

// JSONPathExpressionCache is a cache for JSONPathExpression.
type JSONPathExpressionCache struct {
	mu    sync.Mutex
	cache *kvcache.SimpleLRUCache
}

// popOneLeg returns a jsonPathLeg, and a child JSONPathExpression without that leg.
func (pe JSONPathExpression) popOneLeg() (jsonPathLeg, JSONPathExpression) {
	newPe := JSONPathExpression{
		legs:  pe.legs[1:],
		flags: 0,
	}
	for _, leg := range newPe.legs {
		if leg.typ == jsonPathLegIndex && leg.arrayIndex == -1 {
			newPe.flags |= jsonPathExpressionContainsAsterisk
		} else if leg.typ == jsonPathLegKey && leg.dotKey == "*" {
			newPe.flags |= jsonPathExpressionContainsAsterisk
		} else if leg.typ == jsonPathLegDoubleAsterisk {
			newPe.flags |= jsonPathExpressionContainsDoubleAsterisk
		}
	}
	return pe.legs[0], newPe
}

// popOneLastLeg returns the parent JSONPathExpression and the last jsonPathLeg
func (pe JSONPathExpression) popOneLastLeg() (JSONPathExpression, jsonPathLeg) {
	lastLegIdx := len(pe.legs) - 1
	lastLeg := pe.legs[lastLegIdx]
	// It is used only in modification, it has been checked that there is no asterisks.
	return JSONPathExpression{legs: pe.legs[:lastLegIdx]}, lastLeg
}

// pushBackOneIndexLeg pushback one leg of INDEX type
func (pe JSONPathExpression) pushBackOneIndexLeg(index int) JSONPathExpression {
	newPe := JSONPathExpression{
		legs:  append(pe.legs, jsonPathLeg{typ: jsonPathLegIndex, arrayIndex: index}),
		flags: pe.flags,
	}
	if index == -1 {
		newPe.flags |= jsonPathExpressionContainsAsterisk
	}
	return newPe
}

// pushBackOneKeyLeg pushback one leg of KEY type
func (pe JSONPathExpression) pushBackOneKeyLeg(key string) JSONPathExpression {
	newPe := JSONPathExpression{
		legs:  append(pe.legs, jsonPathLeg{typ: jsonPathLegKey, dotKey: key}),
		flags: pe.flags,
	}
	if key == "*" {
		newPe.flags |= jsonPathExpressionContainsAsterisk
	}
	return newPe
}

// ContainsAnyAsterisk returns true if pe contains any asterisk.
func (pe JSONPathExpression) ContainsAnyAsterisk() bool {
	return pe.flags.containsAnyAsterisk()
}

type jsonPathStream struct {
	pathExpr string
	pos      int
}

func (s *jsonPathStream) skipWhiteSpace() {
	for ; s.pos < len(s.pathExpr); s.pos++ {
		if !unicode.IsSpace(rune(s.pathExpr[s.pos])) {
			break
		}
	}
}

func (s *jsonPathStream) read() byte {
	b := s.pathExpr[s.pos]
	s.pos++
	return b
}

func (s *jsonPathStream) peek() byte {
	return s.pathExpr[s.pos]
}

func (s *jsonPathStream) skip(i int) {
	s.pos += i
}

func (s *jsonPathStream) exhausted() bool {
	return s.pos >= len(s.pathExpr)
}

func (s *jsonPathStream) readWhile(f func(byte) bool) (str string, metEnd bool) {
	start := s.pos
	for ; !s.exhausted(); s.skip(1) {
		if !f(s.peek()) {
			return s.pathExpr[start:s.pos], false
		}
	}
	return s.pathExpr[start:s.pos], true
}

func parseJSONPathExpr(pathExpr string) (pe JSONPathExpression, err error) {
	s := &jsonPathStream{pathExpr: pathExpr, pos: 0}
	s.skipWhiteSpace()
	if s.exhausted() || s.read() != '$' {
		return JSONPathExpression{}, ErrInvalidJSONPath.GenWithStackByArgs(1)
	}
	s.skipWhiteSpace()

	pe.legs = make([]jsonPathLeg, 0, 16)
	pe.flags = jsonPathExpressionFlag(0)

	var ok bool

	for !s.exhausted() {
		switch s.peek() {
		case '.':
			ok = parseJSONPathMember(s, &pe)
		case '[':
			ok = parseJSONPathArray(s, &pe)
		case '*':
			ok = parseJSONPathWildcard(s, &pe)
		default:
			ok = false
		}

		if !ok {
			return JSONPathExpression{}, ErrInvalidJSONPath.GenWithStackByArgs(s.pos)
		}

		s.skipWhiteSpace()
	}

	if len(pe.legs) > 0 && pe.legs[len(pe.legs)-1].typ == jsonPathLegDoubleAsterisk {
		return JSONPathExpression{}, ErrInvalidJSONPath.GenWithStackByArgs(s.pos)
	}

	return
}

func parseJSONPathWildcard(s *jsonPathStream, p *JSONPathExpression) bool {
	s.skip(1)
	if s.exhausted() || s.read() != '*' {
		return false
	}
	if s.exhausted() || s.peek() == '*' {
		return false
	}

	p.flags |= jsonPathExpressionContainsDoubleAsterisk
	p.legs = append(p.legs, jsonPathLeg{typ: jsonPathLegDoubleAsterisk})
	return true
}

func parseJSONPathArray(s *jsonPathStream, p *JSONPathExpression) bool {
	s.skip(1)
	s.skipWhiteSpace()
	if s.exhausted() {
		return false
	}

	if s.peek() == '*' {
		s.skip(1)
		p.flags |= jsonPathExpressionContainsAsterisk
		p.legs = append(p.legs, jsonPathLeg{typ: jsonPathLegIndex, arrayIndex: arrayIndexAsterisk})
	} else {
		// FIXME: only support an integer index for now. Need to support [last], [1 to 2]... in the future.
		str, meetEnd := s.readWhile(func(b byte) bool {
			return b >= '0' && b <= '9'
		})
		if meetEnd {
			return false
		}
		index, err := strconv.Atoi(str)
		if err != nil || index > math.MaxUint32 {
			return false
		}
		p.legs = append(p.legs, jsonPathLeg{typ: jsonPathLegIndex, arrayIndex: index})
	}

	s.skipWhiteSpace()
	if s.exhausted() || s.read() != ']' {
		return false
	}

	return true
}

func parseJSONPathMember(s *jsonPathStream, p *JSONPathExpression) bool {
	var err error
	s.skip(1)
	s.skipWhiteSpace()
	if s.exhausted() {
		return false
	}

	if s.peek() == '*' {
		s.skip(1)
		p.flags |= jsonPathExpressionContainsAsterisk
		p.legs = append(p.legs, jsonPathLeg{typ: jsonPathLegKey, dotKey: "*"})
	} else {
		var dotKey string
		var wasQuoted bool
		if s.peek() == '"' {
			s.skip(1)
			str, meetEnd := s.readWhile(func(b byte) bool {
				if b == '\\' {
					s.skip(1)
					return true
				}
				return b != '"'
			})
			if meetEnd {
				return false
			}
			s.skip(1)
			dotKey = str
			wasQuoted = true
		} else {
			dotKey, _ = s.readWhile(func(b byte) bool {
				return !(unicode.IsSpace(rune(b)) || b == '.' || b == '[' || b == '*')
			})
		}
		dotKey = "\"" + dotKey + "\""

		if !json.Valid(hack.Slice(dotKey)) {
			return false
		}
		dotKey, err = unquoteJSONString(dotKey[1 : len(dotKey)-1])
		if err != nil || (!wasQuoted && !isEcmascriptIdentifier(dotKey)) {
			return false
		}

		p.legs = append(p.legs, jsonPathLeg{typ: jsonPathLegKey, dotKey: dotKey})
	}
	return true
}

func isEcmascriptIdentifier(s string) bool {
	if s == "" {
		return false
	}

	for i := 0; i < len(s); i++ {
		if (i != 0 && s[i] >= '0' && s[i] <= '9') ||
			(s[i] >= 'a' && s[i] <= 'z') || (s[i] >= 'A' && s[i] <= 'Z') ||
			s[i] == '_' || s[i] == '$' || s[i] >= 0x80 {
			continue
		}
		return false
	}
	return true
}

// ParseJSONPathExpr parses a JSON path expression. Returns a JSONPathExpression
// object which can be used in JSON_EXTRACT, JSON_SET and so on.
func ParseJSONPathExpr(pathExpr string) (JSONPathExpression, error) {
	peCache.mu.Lock()
	val, ok := peCache.cache.Get(jsonPathExpressionKey(pathExpr))
	if ok {
		peCache.mu.Unlock()
		return val.(JSONPathExpression), nil
	}
	peCache.mu.Unlock()

	pathExpression, err := parseJSONPathExpr(pathExpr)
	if err == nil {
		peCache.mu.Lock()
		peCache.cache.Put(jsonPathExpressionKey(pathExpr), kvcache.Value(pathExpression))
		peCache.mu.Unlock()
	}
	return pathExpression, err
}

func (pe JSONPathExpression) String() string {
	var s strings.Builder

	s.WriteString("$")
	for _, leg := range pe.legs {
		switch leg.typ {
		case jsonPathLegIndex:
			if leg.arrayIndex == -1 {
				s.WriteString("[*]")
			} else {
				s.WriteString("[")
				s.WriteString(strconv.Itoa(leg.arrayIndex))
				s.WriteString("]")
			}
		case jsonPathLegKey:
			s.WriteString(".")
			if leg.dotKey == "*" {
				s.WriteString(leg.dotKey)
			} else {
				s.WriteString(quoteJSONString(leg.dotKey))
			}
		case jsonPathLegDoubleAsterisk:
			s.WriteString("**")
		}
	}
	return s.String()
}

func init() {
	peCache.cache = kvcache.NewSimpleLRUCache(1000, 0.1, math.MaxUint64)
}
