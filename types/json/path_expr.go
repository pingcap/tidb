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

package json

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
*/

type pathLegType byte

const (
	// pathLegKey indicates the path leg with '.key'.
	pathLegKey pathLegType = 0x01
	// pathLegIndex indicates the path leg with form '[number]'.
	pathLegIndex pathLegType = 0x02
	// pathLegDoubleAsterisk indicates the path leg with form '**'.
	pathLegDoubleAsterisk pathLegType = 0x03
)

// pathLeg is only used by PathExpression.
type pathLeg struct {
	typ        pathLegType
	arrayIndex int    // if typ is pathLegIndex, the value should be parsed into here.
	dotKey     string // if typ is pathLegKey, the key should be parsed into here.
}

// arrayIndexAsterisk is for parsing `*` into a number.
// we need this number represent "all".
const arrayIndexAsterisk = -1

// pathExpressionFlag holds attributes of PathExpression
type pathExpressionFlag byte

const (
	pathExpressionContainsAsterisk       pathExpressionFlag = 0x01
	pathExpressionContainsDoubleAsterisk pathExpressionFlag = 0x02
)

// containsAnyAsterisk returns true if pef contains any asterisk.
func (pef pathExpressionFlag) containsAnyAsterisk() bool {
	pef &= pathExpressionContainsAsterisk | pathExpressionContainsDoubleAsterisk
	return byte(pef) != 0
}

// PathExpression is for JSON path expression.
type PathExpression struct {
	legs  []pathLeg
	flags pathExpressionFlag
}

var peCache PathExpressionCache

type pathExpressionKey string

func (key pathExpressionKey) Hash() []byte {
	return hack.Slice(string(key))
}

// PathExpressionCache is a cache for PathExpression.
type PathExpressionCache struct {
	mu    sync.Mutex
	cache *kvcache.SimpleLRUCache
}

// popOneLeg returns a pathLeg, and a child PathExpression without that leg.
func (pe PathExpression) popOneLeg() (pathLeg, PathExpression) {
	newPe := PathExpression{
		legs:  pe.legs[1:],
		flags: 0,
	}
	for _, leg := range newPe.legs {
		if leg.typ == pathLegIndex && leg.arrayIndex == -1 {
			newPe.flags |= pathExpressionContainsAsterisk
		} else if leg.typ == pathLegKey && leg.dotKey == "*" {
			newPe.flags |= pathExpressionContainsAsterisk
		} else if leg.typ == pathLegDoubleAsterisk {
			newPe.flags |= pathExpressionContainsDoubleAsterisk
		}
	}
	return pe.legs[0], newPe
}

// popOneLastLeg returns the parent PathExpression and the last pathLeg
func (pe PathExpression) popOneLastLeg() (PathExpression, pathLeg) {
	lastLegIdx := len(pe.legs) - 1
	lastLeg := pe.legs[lastLegIdx]
	// It is used only in modification, it has been checked that there is no asterisks.
	return PathExpression{legs: pe.legs[:lastLegIdx]}, lastLeg
}

// pushBackOneIndexLeg pushback one leg of INDEX type
func (pe PathExpression) pushBackOneIndexLeg(index int) PathExpression {
	newPe := PathExpression{
		legs:  append(pe.legs, pathLeg{typ: pathLegIndex, arrayIndex: index}),
		flags: pe.flags,
	}
	if index == -1 {
		newPe.flags |= pathExpressionContainsAsterisk
	}
	return newPe
}

// pushBackOneKeyLeg pushback one leg of KEY type
func (pe PathExpression) pushBackOneKeyLeg(key string) PathExpression {
	newPe := PathExpression{
		legs:  append(pe.legs, pathLeg{typ: pathLegKey, dotKey: key}),
		flags: pe.flags,
	}
	if key == "*" {
		newPe.flags |= pathExpressionContainsAsterisk
	}
	return newPe
}

// ContainsAnyAsterisk returns true if pe contains any asterisk.
func (pe PathExpression) ContainsAnyAsterisk() bool {
	return pe.flags.containsAnyAsterisk()
}

type stream struct {
	pathExpr string
	pos      int
}

func (s *stream) skipWhiteSpace() {
	for ; s.pos < len(s.pathExpr); s.pos++ {
		if !unicode.IsSpace(rune(s.pathExpr[s.pos])) {
			break
		}
	}
}

func (s *stream) read() byte {
	b := s.pathExpr[s.pos]
	s.pos++
	return b
}

func (s *stream) peek() byte {
	return s.pathExpr[s.pos]
}

func (s *stream) skip(i int) {
	s.pos += i
}

func (s *stream) exhausted() bool {
	return s.pos >= len(s.pathExpr)
}

func (s *stream) readWhile(f func(byte) bool) (str string, metEnd bool) {
	start := s.pos
	for ; !s.exhausted(); s.skip(1) {
		if !f(s.peek()) {
			return s.pathExpr[start:s.pos], false
		}
	}
	return s.pathExpr[start:s.pos], true
}

func parseJSONPathExpr(pathExpr string) (pe PathExpression, err error) {
	s := &stream{pathExpr: pathExpr, pos: 0}
	s.skipWhiteSpace()
	if s.exhausted() || s.read() != '$' {
		return PathExpression{}, ErrInvalidJSONPath.GenWithStackByArgs(1)
	}
	s.skipWhiteSpace()

	pe.legs = make([]pathLeg, 0, 16)
	pe.flags = pathExpressionFlag(0)

	var ok bool

	for !s.exhausted() {
		switch s.peek() {
		case '.':
			ok = parseMember(s, &pe)
		case '[':
			ok = parseArray(s, &pe)
		case '*':
			ok = parseWildcard(s, &pe)
		default:
			ok = false
		}

		if !ok {
			return PathExpression{}, ErrInvalidJSONPath.GenWithStackByArgs(s.pos)
		}

		s.skipWhiteSpace()
	}

	if len(pe.legs) > 0 && pe.legs[len(pe.legs)-1].typ == pathLegDoubleAsterisk {
		return PathExpression{}, ErrInvalidJSONPath.GenWithStackByArgs(s.pos)
	}

	return
}

func parseWildcard(s *stream, p *PathExpression) bool {
	s.skip(1)
	if s.exhausted() || s.read() != '*' {
		return false
	}
	if s.exhausted() || s.peek() == '*' {
		return false
	}

	p.flags |= pathExpressionContainsDoubleAsterisk
	p.legs = append(p.legs, pathLeg{typ: pathLegDoubleAsterisk})
	return true
}

func parseArray(s *stream, p *PathExpression) bool {
	s.skip(1)
	s.skipWhiteSpace()
	if s.exhausted() {
		return false
	}

	if s.peek() == '*' {
		s.skip(1)
		p.flags |= pathExpressionContainsAsterisk
		p.legs = append(p.legs, pathLeg{typ: pathLegIndex, arrayIndex: arrayIndexAsterisk})
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
		p.legs = append(p.legs, pathLeg{typ: pathLegIndex, arrayIndex: index})
	}

	s.skipWhiteSpace()
	if s.exhausted() || s.read() != ']' {
		return false
	}

	return true
}

func parseMember(s *stream, p *PathExpression) bool {
	var err error
	s.skip(1)
	s.skipWhiteSpace()
	if s.exhausted() {
		return false
	}

	if s.peek() == '*' {
		s.skip(1)
		p.flags |= pathExpressionContainsAsterisk
		p.legs = append(p.legs, pathLeg{typ: pathLegKey, dotKey: "*"})
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
		dotKey, err = unquoteString(dotKey[1 : len(dotKey)-1])
		if err != nil || (!wasQuoted && !isEcmascriptIdentifier(dotKey)) {
			return false
		}

		p.legs = append(p.legs, pathLeg{typ: pathLegKey, dotKey: dotKey})
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

// ParseJSONPathExpr parses a JSON path expression. Returns a PathExpression
// object which can be used in JSON_EXTRACT, JSON_SET and so on.
func ParseJSONPathExpr(pathExpr string) (PathExpression, error) {
	peCache.mu.Lock()
	val, ok := peCache.cache.Get(pathExpressionKey(pathExpr))
	if ok {
		peCache.mu.Unlock()
		return val.(PathExpression), nil
	}
	peCache.mu.Unlock()

	pathExpression, err := parseJSONPathExpr(pathExpr)
	if err == nil {
		peCache.mu.Lock()
		peCache.cache.Put(pathExpressionKey(pathExpr), kvcache.Value(pathExpression))
		peCache.mu.Unlock()
	}
	return pathExpression, err
}

func (pe PathExpression) String() string {
	var s strings.Builder

	s.WriteString("$")
	for _, leg := range pe.legs {
		switch leg.typ {
		case pathLegIndex:
			if leg.arrayIndex == -1 {
				s.WriteString("[*]")
			} else {
				s.WriteString("[")
				s.WriteString(strconv.Itoa(leg.arrayIndex))
				s.WriteString("]")
			}
		case pathLegKey:
			s.WriteString(".")
			if leg.dotKey == "*" {
				s.WriteString(leg.dotKey)
			} else {
				s.WriteString(quoteString(leg.dotKey))
			}
		case pathLegDoubleAsterisk:
			s.WriteString("**")
		}
	}
	return s.String()
}

func init() {
	peCache.cache = kvcache.NewSimpleLRUCache(1000, 0.1, math.MaxUint64)
}
