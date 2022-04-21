// Copyright 2022 PingCAP, Inc.
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

package selector

import (
	"fmt"
	"sync"

	"github.com/pingcap/errors"
)

// 1. asterisk character (*, also called "star") matches zero or more characters,
//    for example, doc* matches doc and document but not dodo;
//    asterisk character must be the last character of wildcard word.
// 2. the question mark ? matches exactly one character
const (
	// asterisk [ * ]
	asterisk = '*'
	// question mark [ ? ]
	question = '?'
	// rangeOpen mark [ [ ]
	rangeOpen = '['
	// rangeClose mark [ ] ]
	rangeClose = ']'
	// rangeNot mark [ ! ]
	rangeNot = '!'
	// rangeBetween mark [ - ]
	rangeBetween = '-'
)

const maxCacheNum = 1024

// Selector stores rules of schema/table for easy retrieval
type Selector interface {
	// Insert will insert one rule into trie
	// if table is empty, insert rule into schema level
	// otherwise insert rule into table level
	Insert(schema, table string, rule interface{}, insertType int) error
	// Match will return all matched rules
	Match(schema, table string) RuleSet
	// Remove will remove one rule
	Remove(schema, table string) error
	// AllRules will returns all rules
	AllRules() (map[string][]interface{}, map[string]map[string][]interface{})
}

// RuleSet is a set of rules that selected
type RuleSet []interface{}

func (r RuleSet) clone() RuleSet {
	if r == nil {
		return nil
	}

	c := make(RuleSet, 0, len(r))
	return append(c, r...)
}

type matchedResult struct {
	nodes []*node
	rules RuleSet
}

func (r *matchedResult) empty() bool {
	return r == nil || (len(r.nodes) == 0 && len(r.rules) == 0)
}

type trieSelector struct {
	sync.RWMutex

	cache map[string]RuleSet
	root  *node
}

type node struct {
	characters map[byte]item
	asterisk   item
	question   item
	rItems     []item
}

type item interface {
	child() *node
	setChild(*node)
	getRule() []interface{}
	setRule(...interface{})
	resetRule()
	appendRule(interface{})
	getNextLevel() *node
	setNextLevel(*node)
}

type baseItem struct {
	ch *node

	rule []interface{}
	// schema level ->(to) table level
	nextLevel *node
}

func (i *baseItem) child() *node {
	return i.ch
}

func (i *baseItem) setChild(c *node) {
	i.ch = c
}

func (i *baseItem) getRule() []interface{} {
	return i.rule
}

func (i *baseItem) setRule(rules ...interface{}) {
	i.rule = rules
}

func (i *baseItem) resetRule() {
	i.rule = nil
}

func (i *baseItem) appendRule(rule interface{}) {
	i.rule = append(i.rule, rule)
}

func (i *baseItem) getNextLevel() *node {
	return i.nextLevel
}

func (i *baseItem) setNextLevel(c *node) {
	i.nextLevel = c
}

func newNode() *node {
	return &node{characters: make(map[byte]item)}
}

type ran struct {
	start, end byte
	hasBetween bool
}

type rangeItem struct {
	*baseItem

	hasNot bool
	ranges []ran
}

func (i *rangeItem) equal(i2 *rangeItem) bool {
	return i.match(i2) && i2.match(i)
}

func (i *rangeItem) match(i2 *rangeItem) bool {
	if i.hasNot != i2.hasNot {
		return false
	}
	for _, r := range i.ranges {
		matched := false
		for _, r2 := range i2.ranges {
			if r2.start <= r.start && r.end <= r2.end {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func (i *rangeItem) matchChar(c byte) bool {
	for _, r := range i.ranges {
		if r.start <= c && c <= r.end {
			return !i.hasNot
		}
	}
	return i.hasNot
}

func (i *rangeItem) str() string {
	ret := "["
	if i.hasNot {
		ret += string([]byte{rangeNot})
	}
	for _, r := range i.ranges {
		if r.hasBetween {
			ret += string([]byte{r.start}) + string([]byte{rangeBetween}) + string([]byte{r.end})
		} else {
			ret += string([]byte{r.start})
		}
	}
	ret += "]"
	return ret
}

// NewTrieSelector returns a trie Selector
func NewTrieSelector() Selector {
	return &trieSelector{cache: make(map[string]RuleSet), root: newNode()}
}

// Insert implements Selector's interface.
func (t *trieSelector) Insert(schema, table string, rule interface{}, insertType int) error {
	if len(schema) == 0 || rule == nil {
		return errors.Errorf("schema pattern %s or rule %v can't be empty", schema, rule)
	}

	var err error
	t.Lock()
	if len(table) == 0 {
		err = t.insertSchema(schema, rule, insertType)
	} else {
		err = t.insertTable(schema, table, rule, insertType)
	}
	t.Unlock()

	return errors.Trace(err)
}

const (
	// Insert means insert a new rule
	Insert int = iota
	// Replace means update an old rule
	Replace
	// Append means delete an old rule
	Append
)

func (t *trieSelector) insertSchema(schema string, rule interface{}, insertType int) error {
	_, err := t.insert(t.root, schema, rule, insertType)
	if err != nil {
		return errors.Annotate(err, "insert into schema selector")
	}

	return nil
}

func (t *trieSelector) insertTable(schema, table string, rule interface{}, insertType int) error {
	schemaEntity, err := t.insert(t.root, schema, nil, Insert)
	if err != nil {
		return errors.Annotate(err, "insert into schema selector")
	}

	if schemaEntity.getNextLevel() == nil {
		schemaEntity.setNextLevel(newNode())
	}

	_, err = t.insert(schemaEntity.getNextLevel(), table, rule, insertType)
	if err != nil {
		return errors.Annotate(err, "insert into table selector")
	}

	return nil
}

func (t *trieSelector) getRangeItem(pattern string) (*rangeItem, int) {
	nextI := -1
	for i := range pattern {
		if pattern[i] == rangeClose {
			nextI = i
			break
		}
	}
	if nextI == -1 {
		return nil, nextI
	}
	item := &rangeItem{}
	startI := 1
	if pattern[startI] == rangeNot {
		startI++
		item.hasNot = true
	}
	for i := startI; i < nextI; i++ {
		if pattern[i+1] == rangeBetween && i+2 < nextI {
			item.ranges = append(item.ranges, ran{pattern[i], pattern[i+2], true})
			i += 2
		} else {
			item.ranges = append(item.ranges, ran{pattern[i], pattern[i], false})
		}
	}
	// Change the `[!]` to `[\!-\!]`.
	if len(item.ranges) == 0 && item.hasNot {
		item.hasNot = false
		item.ranges = append(item.ranges, ran{'!', '!', false})
	}
	return item, nextI
}

// if rule is nil, just extract nodes
func (t *trieSelector) insert(root *node, pattern string, rule interface{}, insertType int) (item, error) {
	var (
		n           = root
		hadAsterisk = false
		entity      item
	)

	for i := 0; i < len(pattern); i++ {
		if hadAsterisk {
			return nil, errors.NotValidf("pattern %s", pattern)
		}

		var rItem *rangeItem
		var nextI int
		switch pattern[i] {
		case asterisk:
			entity = n.asterisk
			hadAsterisk = true
		case question:
			entity = n.question
		case rangeOpen:
			rItem, nextI = t.getRangeItem(pattern[i:])
			if nextI == -1 {
				entity = n.characters[pattern[i]]
			} else {
				entity = nil
				for _, nrItem := range n.rItems {
					if rItem.equal(nrItem.(*rangeItem)) {
						entity = nrItem
						break
					}
				}
			}
		default:
			entity = n.characters[pattern[i]]
		}
		if entity == nil {
			entity = &baseItem{}
			switch pattern[i] {
			case asterisk:
				n.asterisk = entity
			case question:
				n.question = entity
			case rangeOpen:
				if nextI == -1 {
					n.characters[pattern[i]] = entity
				} else {
					n.rItems = append(n.rItems, rItem)
					rItem.baseItem = entity.(*baseItem)
					entity = rItem
				}
			default:
				n.characters[pattern[i]] = entity
			}
		}
		if entity.child() == nil {
			entity.setChild(newNode())
		}
		n = entity.child()
		if nextI != -1 {
			i += nextI
		}
	}

	if rule != nil {
		if insertType == Insert && entity.getRule() != nil {
			return nil, errors.AlreadyExistsf("pattern %s", pattern)
		}
		if insertType == Replace {
			entity.setRule(rule)
		} else {
			entity.appendRule(rule)
		}
		t.clearCache()
	}

	return entity, nil
}

// Match implements Selector's interface.
func (t *trieSelector) Match(schema, table string) RuleSet {
	// try to find schema/table in cache
	t.RLock()
	cacheKey := quoteSchemaTable(schema, table)
	rules, ok := t.cache[cacheKey]
	t.RUnlock()
	if ok {
		return rules.clone()
	}

	matchedSchemaResult := &matchedResult{
		nodes: make([]*node, 0, 4),
		rules: make(RuleSet, 0, 4),
	}
	rules = nil

	// find matched rules
	t.Lock()
	defer t.Unlock()
	t.matchNode(t.root, schema, matchedSchemaResult)

	// not found matched rules in schema level
	if matchedSchemaResult.empty() {
		t.addToCache(cacheKey, nil)
		return nil
	}

	rules = append(rules, matchedSchemaResult.rules...)

	for _, si := range matchedSchemaResult.nodes {
		matchedTableResult := &matchedResult{
			rules: make(RuleSet, 0, 4),
		}
		// find matched rules in table level
		t.matchNode(si, table, matchedTableResult)
		rules = append(rules, matchedTableResult.rules...)
	}

	// not found matched rule in table level, return mathed rule in schema level
	t.addToCache(cacheKey, rules)
	return rules.clone()
}

// Remove implements Selector interface.
// TODO: remove useless nodes and lazy deletion
func (t *trieSelector) Remove(schema, table string) error {
	t.Lock()
	defer t.Unlock()

	if len(schema) == 0 {
		return errors.NotValidf("schema/table %s/%s", schema, table)
	}

	schemaItems, err := t.track(t.root, schema)
	if err != nil {
		return errors.Annotatef(err, "track schema/table %s/%s in schema level", schema, table)
	}

	schemaLeafItem := schemaItems[len(schemaItems)-1]
	if len(table) > 0 {
		if schemaLeafItem.getNextLevel() == nil {
			return errors.NotFoundf("table level while we track chema/table %s/%s", schema, table)
		}

		tableItems, err := t.track(schemaLeafItem.getNextLevel(), table)
		if err != nil {
			return errors.Annotatef(err, "track schema/table %s/%s in table level", schema, table)
		}

		if tableItems[len(tableItems)-1].getRule() == nil {
			return errors.NotFoundf("schema/table %s/%s in table level", schema, table)
		}

		// remove table level nodes
		tableItems[len(tableItems)-1].resetRule()
		t.clearCache()
		return nil
	}

	if schemaLeafItem.getRule() == nil {
		return errors.NotFoundf("schema/table %s/%s in schema level", schema, table)
	}

	schemaLeafItem.resetRule()
	t.clearCache()
	return nil
}

func (t *trieSelector) track(n *node, pattern string) ([]item, error) {
	items := make([]item, 0, len(pattern))
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case asterisk:
			if n.asterisk == nil {
				return nil, errors.NotFoundf("pattern %v", pattern)
			}

			if i != len(pattern)-1 {
				return nil, errors.NotValidf("pattern %v ", pattern)
			}

			items = append(items, n.asterisk)
		case question:
			if n.question == nil {
				return nil, errors.NotFoundf("pattern %v", pattern)
			}

			items = append(items, n.question)
			n = n.question.child()
		case rangeOpen:
			rItem, nextI := t.getRangeItem(pattern[i:])
			if nextI == -1 {
				item, ok := n.characters[pattern[i]]
				if !ok {
					return nil, errors.NotFoundf("pattern %v", pattern)
				}
				items = append(items, item)
				n = item.child()
			} else {
				matchIdx := -1
				for idx := range n.rItems {
					if n.rItems[idx].(*rangeItem).equal(rItem) {
						matchIdx = idx
						break
					}
				}
				if matchIdx == -1 {
					return nil, errors.NotFoundf("pattern %v", pattern)
				}
				items = append(items, n.rItems[matchIdx])
				n = n.rItems[matchIdx].child()
				i += nextI
			}
		default:
			item, ok := n.characters[pattern[i]]
			if !ok {
				return nil, errors.NotFoundf("pattern %v", pattern)
			}
			items = append(items, item)
			n = item.child()
		}
	}

	return items, nil
}

// AllRules implements Selector's AllRules
func (t *trieSelector) AllRules() (map[string][]interface{}, map[string]map[string][]interface{}) {
	var (
		tableRules  = make(map[string]map[string][]interface{})
		schemaNodes = make(map[string]*node)
		schemaRules = make(map[string][]interface{})
		word        []byte
	)
	t.RLock()
	t.travel(t.root, word, schemaRules, schemaNodes)

	for schema, n := range schemaNodes {
		rules, ok := tableRules[schema]
		if !ok {
			rules = make(map[string][]interface{})
		}

		word = word[:0]
		t.travel(n, word, rules, nil)
		if len(rules) > 0 {
			tableRules[schema] = rules
		}
	}
	t.RUnlock()
	return schemaRules, tableRules
}

func (t *trieSelector) travel(n *node, word []byte, rules map[string][]interface{}, nodes map[string]*node) {
	if n == nil {
		return
	}

	if n.asterisk != nil {
		pattern := append(word, asterisk)
		insertMatchedItemIntoMap(string(pattern), n.asterisk, rules, nodes)
	}

	if n.question != nil {
		pattern := append(word, question)
		insertMatchedItemIntoMap(string(pattern), n.question, rules, nodes)
		t.travel(n.question.child(), pattern, rules, nodes)
	}

	for i := range n.rItems {
		pattern := append(word, []byte(n.rItems[i].(*rangeItem).str())...)
		insertMatchedItemIntoMap(string(pattern), n.rItems[i], rules, nodes)
		t.travel(n.rItems[i].child(), pattern, rules, nodes)
	}

	for char, baseItem := range n.characters {
		pattern := append(word, char)
		if baseItem != nil {
			insertMatchedItemIntoMap(string(pattern), baseItem, rules, nodes)
			t.travel(baseItem.child(), pattern, rules, nodes)
		}
	}
}

func (t *trieSelector) matchNode(n *node, s string, mr *matchedResult) {
	if n == nil {
		return
	}

	var (
		ok     bool
		entity item
	)
	for i := range s {
		if n.asterisk != nil {
			appendMatchedItem(n.asterisk, mr)
		}

		if n.question != nil {
			if i == len(s)-1 {
				appendMatchedItem(n.question, mr)
			}

			t.matchNode(n.question.child(), s[i+1:], mr)
		}

		for _, rItem := range n.rItems {
			if rItem.(*rangeItem).matchChar(s[i]) {
				if i == len(s)-1 {
					appendMatchedItem(rItem, mr)
				}

				t.matchNode(rItem.child(), s[i+1:], mr)
			}
		}

		entity, ok = n.characters[s[i]]
		if !ok {
			return
		}
		n = entity.child()
	}

	if entity != nil {
		appendMatchedItem(entity, mr)
	}

	if n.asterisk != nil {
		appendMatchedItem(n.asterisk, mr)
	}
}

func appendMatchedItem(entity item, mr *matchedResult) {
	if entity.getRule() != nil {
		mr.rules = append(mr.rules, entity.getRule()...)
	}

	if entity.getNextLevel() != nil {
		mr.nodes = append(mr.nodes, entity.getNextLevel())
	}
}

func insertMatchedItemIntoMap(pattern string, entity item, rules map[string][]interface{}, nodes map[string]*node) {
	if rules != nil && entity.getRule() != nil {
		rules[pattern] = entity.getRule()
	}

	if nodes != nil && entity.getNextLevel() != nil {
		nodes[pattern] = entity.getNextLevel()
	}
}

func (t *trieSelector) addToCache(key string, rules RuleSet) {
	t.cache[key] = rules
	if len(t.cache) > maxCacheNum {
		for literal := range t.cache {
			delete(t.cache, literal)
			break
		}
	}
}

func (t *trieSelector) clearCache() {
	t.cache = make(map[string]RuleSet)
}

func quoteSchemaTable(schema, table string) string {
	if len(schema) == 0 {
		return ""
	}

	if len(table) > 0 {
		return fmt.Sprintf("`%s`.`%s`", schema, table)
	}

	return fmt.Sprintf("`%s`", schema)
}
