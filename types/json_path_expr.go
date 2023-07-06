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

// if index is positive, it represents the [index]
// if index is negative, it represents the [len() + index]
// a normal index "5" will be parsed into "5", and the "last - 5" will be "-6"
type jsonPathArrayIndex int

func (index jsonPathArrayIndex) getIndexFromStart(obj BinaryJSON) int {
	if index < 0 {
		return obj.GetElemCount() + int(index)
	}

	return int(index)
}

// validateIndexRange returns whether `a` could be less or equal than `b`
// if two indexes are all non-negative, or all negative, the comparison follows the number order
// if the sign of them differs, this function will still return true
func validateIndexRange(a jsonPathArrayIndex, b jsonPathArrayIndex) bool {
	if (a >= 0 && b >= 0) || (a < 0 && b < 0) {
		return a <= b
	}

	return true
}

func jsonPathArrayIndexFromStart(index int) jsonPathArrayIndex {
	return jsonPathArrayIndex(index)
}

func jsonPathArrayIndexFromLast(index int) jsonPathArrayIndex {
	return jsonPathArrayIndex(-1 - index)
}

type jsonPathArraySelection interface {
	// returns the closed interval of the range it represents
	// it ensures the `end` is less or equal than element count - 1
	// the caller should validate the return value through checking start <= end
	getIndexRange(bj BinaryJSON) (int, int)
}

var _ jsonPathArraySelection = jsonPathArraySelectionAsterisk{}
var _ jsonPathArraySelection = jsonPathArraySelectionIndex{}
var _ jsonPathArraySelection = jsonPathArraySelectionRange{}

type jsonPathArraySelectionAsterisk struct{}

func (jsonPathArraySelectionAsterisk) getIndexRange(bj BinaryJSON) (int, int) {
	return 0, bj.GetElemCount() - 1
}

type jsonPathArraySelectionIndex struct {
	index jsonPathArrayIndex
}

func (i jsonPathArraySelectionIndex) getIndexRange(bj BinaryJSON) (int, int) {
	end := i.index.getIndexFromStart(bj)

	// returns index, min(index, count - 1)
	// so that the caller could only check the start <= end
	elemCount := bj.GetElemCount()
	if end >= elemCount {
		end = elemCount - 1
	}
	return i.index.getIndexFromStart(bj), end
}

// jsonPathArraySelectionRange represents a closed interval
type jsonPathArraySelectionRange struct {
	start jsonPathArrayIndex
	end   jsonPathArrayIndex
}

func (i jsonPathArraySelectionRange) getIndexRange(bj BinaryJSON) (int, int) {
	start := i.start.getIndexFromStart(bj)
	end := i.end.getIndexFromStart(bj)

	elemCount := bj.GetElemCount()
	if end >= elemCount {
		end = elemCount - 1
	}
	return start, end
}

type jsonPathLegType byte

const (
	// jsonPathLegKey indicates the path leg with '.key'.
	jsonPathLegKey jsonPathLegType = 0x01
	// jsonPathLegArraySelection indicates the path leg with form '[index]', '[index to index]'.
	jsonPathLegArraySelection jsonPathLegType = 0x02
	// jsonPathLegDoubleAsterisk indicates the path leg with form '**'.
	jsonPathLegDoubleAsterisk jsonPathLegType = 0x03
)

// jsonPathLeg is only used by JSONPathExpression.
type jsonPathLeg struct {
	typ            jsonPathLegType
	arraySelection jsonPathArraySelection // if typ is jsonPathLegArraySelection, the value should be parsed into here.
	dotKey         string                 // if typ is jsonPathLegKey, the key should be parsed into here.
}

// jsonPathExpressionFlag holds attributes of JSONPathExpression
type jsonPathExpressionFlag byte

const (
	jsonPathExpressionContainsAsterisk       jsonPathExpressionFlag = 0x01
	jsonPathExpressionContainsDoubleAsterisk jsonPathExpressionFlag = 0x02
	jsonPathExpressionContainsRange          jsonPathExpressionFlag = 0x04
)

// containsAnyAsterisk returns true if pef contains any asterisk.
func (pef jsonPathExpressionFlag) containsAnyAsterisk() bool {
	pef &= jsonPathExpressionContainsAsterisk | jsonPathExpressionContainsDoubleAsterisk
	return byte(pef) != 0
}

// containsAnyRange returns true if pef contains any range.
func (pef jsonPathExpressionFlag) containsAnyRange() bool {
	pef &= jsonPathExpressionContainsRange
	return byte(pef) != 0
}

// JSONPathExpression is for JSON path expression.
type JSONPathExpression struct {
	legs  []jsonPathLeg
	flags jsonPathExpressionFlag
}

func (pe JSONPathExpression) clone() JSONPathExpression {
	legs := make([]jsonPathLeg, len(pe.legs))
	copy(legs, pe.legs)
	return JSONPathExpression{legs: legs, flags: pe.flags}
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
		if leg.typ == jsonPathLegArraySelection {
			switch leg.arraySelection.(type) {
			case jsonPathArraySelectionAsterisk:
				newPe.flags |= jsonPathExpressionContainsAsterisk
			case jsonPathArraySelectionRange:
				newPe.flags |= jsonPathExpressionContainsRange
			}
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

// pushBackOneArraySelectionLeg pushback one leg of INDEX type
func (pe JSONPathExpression) pushBackOneArraySelectionLeg(arraySelection jsonPathArraySelection) JSONPathExpression {
	newPe := JSONPathExpression{
		legs:  append(pe.legs, jsonPathLeg{typ: jsonPathLegArraySelection, arraySelection: arraySelection}),
		flags: pe.flags,
	}
	switch arraySelection.(type) {
	case jsonPathArraySelectionAsterisk:
		newPe.flags |= jsonPathExpressionContainsAsterisk
	case jsonPathArraySelectionRange:
		newPe.flags |= jsonPathExpressionContainsRange
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

// CouldMatchMultipleValues returns true if pe contains any asterisk or range selection.
func (pe JSONPathExpression) CouldMatchMultipleValues() bool {
	return pe.flags.containsAnyAsterisk() || pe.flags.containsAnyRange()
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

func (s *jsonPathStream) tryReadString(expected string) bool {
	recordPos := s.pos

	i := 0
	str, meetEnd := s.readWhile(func(b byte) bool {
		i += 1
		return i <= len(expected)
	})

	if meetEnd || str != expected {
		s.pos = recordPos
		return false
	}
	return true
}

func (s *jsonPathStream) tryReadIndexNumber() (int, bool) {
	recordPos := s.pos

	str, meetEnd := s.readWhile(func(b byte) bool {
		return b >= '0' && b <= '9'
	})
	if meetEnd {
		s.pos = recordPos
		return 0, false
	}

	index, err := strconv.Atoi(str)
	if err != nil || index > math.MaxUint32 {
		s.pos = recordPos
		return 0, false
	}

	return index, true
}

// tryParseArrayIndex tries to read an arrayIndex, which is 'number', 'last' or 'last - number'
// if failed, the stream will not be pushed forward
func (s *jsonPathStream) tryParseArrayIndex() (jsonPathArrayIndex, bool) {
	recordPos := s.pos

	s.skipWhiteSpace()
	if s.exhausted() {
		return 0, false
	}
	switch c := s.peek(); {
	case c >= '0' && c <= '9':
		index, ok := s.tryReadIndexNumber()
		if !ok {
			s.pos = recordPos
			return 0, false
		}
		return jsonPathArrayIndexFromStart(index), true
	case c == 'l':
		if !s.tryReadString("last") {
			s.pos = recordPos
			return 0, false
		}
		s.skipWhiteSpace()
		if s.exhausted() {
			return jsonPathArrayIndexFromLast(0), true
		}

		if s.peek() != '-' {
			return jsonPathArrayIndexFromLast(0), true
		}
		s.skip(1)
		s.skipWhiteSpace()

		index, ok := s.tryReadIndexNumber()
		if !ok {
			s.pos = recordPos
			return 0, false
		}
		return jsonPathArrayIndexFromLast(index), true
	}
	return 0, false
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
		p.legs = append(p.legs, jsonPathLeg{typ: jsonPathLegArraySelection, arraySelection: jsonPathArraySelectionAsterisk{}})
	} else {
		start, ok := s.tryParseArrayIndex()
		if !ok {
			return false
		}

		var selection jsonPathArraySelection
		selection = jsonPathArraySelectionIndex{start}
		// try to read " to " and the end
		if unicode.IsSpace(rune(s.peek())) {
			s.skipWhiteSpace()
			if s.tryReadString("to") && unicode.IsSpace(rune(s.peek())) {
				s.skipWhiteSpace()
				if s.exhausted() {
					return false
				}
				end, ok := s.tryParseArrayIndex()
				if !ok {
					return false
				}

				if !validateIndexRange(start, end) {
					return false
				}
				p.flags |= jsonPathExpressionContainsRange
				selection = jsonPathArraySelectionRange{start, end}
			}
		}

		p.legs = append(p.legs, jsonPathLeg{typ: jsonPathLegArraySelection, arraySelection: selection})
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
		return val.(JSONPathExpression).clone(), nil
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

func (index jsonPathArrayIndex) String() string {
	if index < 0 {
		indexStr := strconv.Itoa(int(math.Abs(float64(index + 1))))
		return "last-" + indexStr
	}

	indexStr := strconv.Itoa(int(index))
	return indexStr
}

func (pe JSONPathExpression) String() string {
	var s strings.Builder

	s.WriteString("$")
	for _, leg := range pe.legs {
		switch leg.typ {
		case jsonPathLegArraySelection:
			switch selection := leg.arraySelection.(type) {
			case jsonPathArraySelectionAsterisk:
				s.WriteString("[*]")
			case jsonPathArraySelectionIndex:
				s.WriteString("[")
				s.WriteString(selection.index.String())
				s.WriteString("]")
			case jsonPathArraySelectionRange:
				s.WriteString("[")
				s.WriteString(selection.start.String())
				s.WriteString(" to ")
				s.WriteString(selection.end.String())
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
