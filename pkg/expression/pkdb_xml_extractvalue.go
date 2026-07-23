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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/antchfx/xmlquery"
	"github.com/antchfx/xpath"
)

type extractValueResult struct {
	matches  []extractValueMatch
	scalar   string
	isScalar bool
}

type extractValueMatch struct {
	node    *xmlquery.Node
	attrIdx int
}

// evalExtractValue uses MySQL-compatible element string values when XPath
// predicates compare an element node to a scalar.
func evalExtractValue(top *xmlquery.Node, expr string) (extractValueResult, error) {
	compiled, err := xpath.Compile(rewriteExtractValuePredicateCountDot(expr))
	if err != nil {
		return extractValueResult{}, errUnknown.GenWithStack("XPATH syntax error: '%s'", expr)
	}
	switch v := compiled.Evaluate(createExtractValueXPathNavigator(top)).(type) {
	case *xpath.NodeIterator:
		var matches []extractValueMatch
		for v.MoveNext() {
			current := v.Current()
			nav, ok := current.(*extractValueNodeNavigator)
			if !ok {
				return extractValueResult{}, errUnknown.GenWithStack("unexpected navigator type %T", current)
			}
			matches = append(matches, extractValueMatch{node: nav.curr, attrIdx: nav.attr})
		}
		return extractValueResult{matches: matches}, nil
	case string:
		return extractValueResult{scalar: v, isScalar: true}, nil
	case float64:
		return extractValueResult{scalar: strconv.FormatFloat(v, 'g', -1, 64), isScalar: true}, nil
	case bool:
		return extractValueResult{scalar: strconv.FormatBool(v), isScalar: true}, nil
	case nil:
		return extractValueResult{scalar: "", isScalar: true}, nil
	default:
		return extractValueResult{scalar: fmt.Sprint(v), isScalar: true}, nil
	}
}

func forEachExtractValueDirectText(node *xmlquery.Node, fn func(*xmlquery.Node)) {
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if child.Type != xmlquery.TextNode && child.Type != xmlquery.CharDataNode {
			continue
		}
		// Pretty-printed XML often creates whitespace-only text nodes that MySQL
		// does not surface here.
		if strings.TrimSpace(child.Data) == "" {
			continue
		}
		fn(child)
	}
}

func extractValueElementStringValue(node *xmlquery.Node) string {
	var b strings.Builder
	forEachExtractValueDirectText(node, func(child *xmlquery.Node) {
		b.WriteString(child.Data)
	})
	return b.String()
}

// MySQL ExtractValue treats count(.) inside predicates as the size of the
// current candidate node list, which matches last() for these expressions.
func rewriteExtractValuePredicateCountDot(expr string) string {
	if !strings.Contains(expr, "count") || !strings.Contains(expr, "[") {
		return expr
	}

	var b strings.Builder
	b.Grow(len(expr))

	inSingle := false
	inDouble := false
	predicateDepth := 0

	for i := 0; i < len(expr); {
		ch := expr[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
			b.WriteByte(ch)
			i++
			continue
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
			b.WriteByte(ch)
			i++
			continue
		}

		if !inSingle && !inDouble {
			switch ch {
			case '[':
				predicateDepth++
				b.WriteByte(ch)
				i++
				continue
			case ']':
				if predicateDepth > 0 {
					predicateDepth--
				}
				b.WriteByte(ch)
				i++
				continue
			}

			if predicateDepth > 0 {
				if end, ok := matchExtractValuePredicateCountDot(expr, i); ok {
					b.WriteString("last()")
					i = end
					continue
				}
			}
		}

		b.WriteByte(ch)
		i++
	}

	return b.String()
}

func matchExtractValuePredicateCountDot(expr string, start int) (int, bool) {
	if !strings.HasPrefix(expr[start:], "count") {
		return 0, false
	}
	if start > 0 {
		prev := expr[start-1]
		if isExtractValueXPathNameChar(prev) || prev == ':' {
			return 0, false
		}
	}

	i := start + len("count")
	if i < len(expr) && isExtractValueXPathNameChar(expr[i]) {
		return 0, false
	}
	i = skipExtractValueXPathSpaces(expr, i)
	if i >= len(expr) || expr[i] != '(' {
		return 0, false
	}
	i++
	i = skipExtractValueXPathSpaces(expr, i)
	if i >= len(expr) || expr[i] != '.' {
		return 0, false
	}
	i++
	i = skipExtractValueXPathSpaces(expr, i)
	if i >= len(expr) || expr[i] != ')' {
		return 0, false
	}
	return i + 1, true
}

func skipExtractValueXPathSpaces(expr string, i int) int {
	for i < len(expr) {
		switch expr[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

func isExtractValueXPathNameChar(ch byte) bool {
	return ch == '_' || ch == '-' || ch == '.' ||
		(ch >= '0' && ch <= '9') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z')
}

// extractValueNodeNavigator keeps MySQL-compatible duplicate-attribute
// position() handling and direct-text element string values for ExtractValue,
// including queries that rely on rewriteExtractValuePredicateCountDot.
type extractValueNodeNavigator struct {
	root *xmlquery.Node
	curr *xmlquery.Node
	attr int
}

func createExtractValueXPathNavigator(top *xmlquery.Node) *extractValueNodeNavigator {
	return &extractValueNodeNavigator{root: top, curr: top, attr: -1}
}

func (x *extractValueNodeNavigator) NodeType() xpath.NodeType {
	switch x.curr.Type {
	case xmlquery.CommentNode:
		return xpath.CommentNode
	case xmlquery.TextNode, xmlquery.CharDataNode, xmlquery.NotationNode:
		return xpath.TextNode
	case xmlquery.DeclarationNode, xmlquery.DocumentNode:
		return xpath.RootNode
	case xmlquery.ElementNode:
		if x.attr != -1 {
			return xpath.AttributeNode
		}
		return xpath.ElementNode
	case xmlquery.AttributeNode:
		return xpath.AttributeNode
	case xmlquery.ProcessingInstruction:
		return xpath.ElementNode
	}
	return xpath.TextNode
}

func (x *extractValueNodeNavigator) LocalName() string {
	if x.attr != -1 {
		return x.curr.Attr[x.attr].Name.Local
	}
	return x.curr.Data
}

func (x *extractValueNodeNavigator) Prefix() string {
	if x.attr != -1 {
		return x.curr.Attr[x.attr].Name.Space
	}
	return x.curr.Prefix
}

func (x *extractValueNodeNavigator) NamespaceURL() string {
	if x.attr != -1 {
		return x.curr.Attr[x.attr].NamespaceURI
	}
	return x.curr.NamespaceURI
}

func (x *extractValueNodeNavigator) Value() string {
	if x.attr != -1 {
		return x.curr.Attr[x.attr].Value
	}
	switch x.curr.Type {
	case xmlquery.CommentNode:
		return x.curr.Data
	case xmlquery.ElementNode:
		return extractValueElementStringValue(x.curr)
	case xmlquery.TextNode, xmlquery.CharDataNode, xmlquery.NotationNode:
		return x.curr.Data
	}
	return ""
}

func (x *extractValueNodeNavigator) Copy() xpath.NodeNavigator {
	n := *x
	return &n
}

func (x *extractValueNodeNavigator) MoveToRoot() {
	x.curr = x.root
	x.attr = -1
}

func (x *extractValueNodeNavigator) MoveToParent() bool {
	if x.attr != -1 {
		x.attr = -1
		return true
	}
	if x.curr.Parent == nil {
		return false
	}
	x.curr = x.curr.Parent
	return true
}

func (x *extractValueNodeNavigator) MoveToNextAttribute() bool {
	next := x.attr + 1
	if next >= len(x.curr.Attr) {
		return false
	}
	x.attr = next
	return true
}

func (x *extractValueNodeNavigator) MoveToChild() bool {
	if x.attr != -1 || x.curr.FirstChild == nil {
		return false
	}
	x.curr = x.curr.FirstChild
	return true
}

func (x *extractValueNodeNavigator) MoveToFirst() bool {
	if x.attr != -1 {
		if x.attr == 0 {
			return false
		}
		x.attr = 0
		return true
	}
	if x.curr.PrevSibling == nil {
		return false
	}
	for x.curr.PrevSibling != nil {
		x.curr = x.curr.PrevSibling
	}
	return true
}

func (x *extractValueNodeNavigator) String() string {
	return x.Value()
}

func (x *extractValueNodeNavigator) MoveToNext() bool {
	if x.attr != -1 {
		next := x.attr + 1
		if next >= len(x.curr.Attr) {
			return false
		}
		x.attr = next
		return true
	}
	if x.curr.NextSibling == nil {
		return false
	}
	x.curr = x.curr.NextSibling
	return true
}

func (x *extractValueNodeNavigator) MoveToPrevious() bool {
	if x.attr != -1 {
		prev := x.attr - 1
		if prev < 0 {
			return false
		}
		x.attr = prev
		return true
	}
	if x.curr.PrevSibling == nil {
		return false
	}
	x.curr = x.curr.PrevSibling
	return true
}

func (x *extractValueNodeNavigator) MoveTo(other xpath.NodeNavigator) bool {
	node, ok := other.(*extractValueNodeNavigator)
	if !ok || node.root != x.root {
		return false
	}
	x.curr = node.curr
	x.attr = node.attr
	return true
}

func renderExtractValueMatches(root *xmlquery.Node, matches []extractValueMatch) string {
	if len(matches) == 0 {
		return ""
	}

	selectedAttrs := make(map[*xmlquery.Node]map[int]struct{}, len(matches))
	selectedText := make(map[*xmlquery.Node]struct{}, len(matches))
	for _, match := range matches {
		if match.attrIdx >= 0 {
			attrs, ok := selectedAttrs[match.node]
			if !ok {
				attrs = make(map[int]struct{})
				selectedAttrs[match.node] = attrs
			}
			attrs[match.attrIdx] = struct{}{}
			continue
		}
		switch match.node.Type {
		case xmlquery.TextNode, xmlquery.CharDataNode:
			// Explicit text() matches keep formatting whitespace; only zero-length
			// nodes are ignored so they do not create empty join segments.
			if match.node.Data != "" {
				selectedText[match.node] = struct{}{}
			}
		case xmlquery.ElementNode:
			forEachExtractValueDirectText(match.node, func(child *xmlquery.Node) {
				selectedText[child] = struct{}{}
			})
		}
	}

	parts := make([]string, 0, len(matches))
	var walk func(node *xmlquery.Node)
	walk = func(node *xmlquery.Node) {
		if node == nil {
			return
		}
		if node.Type == xmlquery.ElementNode {
			if attrs, ok := selectedAttrs[node]; ok {
				for idx, attr := range node.Attr {
					if _, ok := attrs[idx]; ok && attr.Value != "" {
						parts = append(parts, attr.Value)
					}
				}
			}
		}
		if _, ok := selectedText[node]; ok {
			parts = append(parts, node.Data)
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}

	walk(root)
	return strings.Join(parts, " ")
}
