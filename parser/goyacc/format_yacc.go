// Copyright 2019 PingCAP, Inc.
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

package main

import (
	"bufio"
	"fmt"
	gofmt "go/format"
	"go/token"
	"io/ioutil"
	"os"
	"regexp"
	"strings"

	parser "github.com/cznic/parser/yacc"
	"github.com/cznic/strutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/format"
)

func Format(inputFilename string, goldenFilename string) (err error) {
	spec, err := parseFileToSpec(inputFilename)
	if err != nil {
		return err
	}

	yFmt := &OutputFormatter{}
	if err = yFmt.Setup(goldenFilename); err != nil {
		return err
	}
	defer func() {
		teardownErr := yFmt.Teardown()
		if err == nil {
			err = teardownErr
		}
	}()

	if err = printDefinitions(yFmt, spec.Defs); err != nil {
		return err
	}

	if err = printRules(yFmt, spec.Rules); err != nil {
		return err
	}
	return nil
}

func parseFileToSpec(inputFilename string) (*parser.Specification, error) {
	src, err := ioutil.ReadFile(inputFilename)
	if err != nil {
		return nil, err
	}
	return parser.Parse(token.NewFileSet(), inputFilename, src)
}

// Definition represents data reduced by productions:
//
//	Definition:
//	        START IDENTIFIER
//	|       UNION                      // Case 1
//	|       LCURL RCURL                // Case 2
//	|       ReservedWord Tag NameList  // Case 3
//	|       ReservedWord Tag           // Case 4
//	|       ERROR_VERBOSE              // Case 5
const (
	StartIdentifierCase = iota
	UnionDefinitionCase
	LCURLRCURLCase
	ReservedWordTagNameListCase
	ReservedWordTagCase
)

func printDefinitions(formatter format.Formatter, definitions []*parser.Definition) error {
	for _, def := range definitions {
		var err error
		switch def.Case {
		case StartIdentifierCase:
			err = handleStart(formatter, def)
		case UnionDefinitionCase:
			err = handleUnion(formatter, def)
		case LCURLRCURLCase:
			err = handleProlog(formatter, def)
		case ReservedWordTagNameListCase, ReservedWordTagCase:
			err = handleReservedWordTagNameList(formatter, def)
		}
		if err != nil {
			return err
		}
	}
	_, err := formatter.Format("\n%%%%")
	return err
}

func handleStart(f format.Formatter, definition *parser.Definition) error {
	if err := Ensure(definition).
		and(definition.Token2).
		and(definition.Token2).NotNil(); err != nil {
		return err
	}
	cmt1 := strings.Join(definition.Token.Comments, "\n")
	cmt2 := strings.Join(definition.Token2.Comments, "\n")
	_, err := f.Format("\n%s%s\t%s%s\n", cmt1, definition.Token.Val, cmt2, definition.Token2.Val)
	return err
}

func handleUnion(f format.Formatter, definition *parser.Definition) error {
	if err := Ensure(definition).
		and(definition.Value).NotNil(); err != nil {
		return err
	}
	if len(definition.Value) != 0 {
		_, err := f.Format("%%union%i%s%u\n\n", definition.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

func handleProlog(f format.Formatter, definition *parser.Definition) error {
	if err := Ensure(definition).
		and(definition.Value).NotNil(); err != nil {
		return err
	}
	_, err := f.Format("%%{%s%%}\n\n", definition.Value)
	return err
}

func handleReservedWordTagNameList(f format.Formatter, def *parser.Definition) error {
	if err := Ensure(def).
		and(def.ReservedWord).
		and(def.ReservedWord.Token).NotNil(); err != nil {
		return err
	}
	comment := getTokenComment(def.ReservedWord.Token, divNewLineStringLayout)
	directive := def.ReservedWord.Token.Val

	hasTag := def.Tag != nil
	var wordAfterDirective string
	if hasTag {
		wordAfterDirective = joinTag(def.Tag)
	} else {
		wordAfterDirective = joinNames(def.Nlist)
	}

	if _, err := f.Format("%s%s%s%i", comment, directive, wordAfterDirective); err != nil {
		return err
	}
	if hasTag {
		if _, err := f.Format("\n"); err != nil {
			return err
		}
		if err := printNameListVertical(f, def.Nlist); err != nil {
			return err
		}
	}
	_, err := f.Format("%u\n")
	return err
}

func joinTag(tag *parser.Tag) string {
	var sb strings.Builder
	sb.WriteString("\t")
	if tag.Token != nil {
		sb.WriteString(tag.Token.Val)
	}
	if tag.Token2 != nil {
		sb.WriteString(tag.Token2.Val)
	}
	if tag.Token3 != nil {
		sb.WriteString(tag.Token3.Val)
	}
	return sb.String()
}

type stringLayout int8

const (
	spanStringLayout stringLayout = iota
	divStringLayout
	divNewLineStringLayout
)

func getTokenComment(token *parser.Token, layout stringLayout) string {
	if len(token.Comments) == 0 {
		return ""
	}
	var splitter, beforeComment string
	switch layout {
	case spanStringLayout:
		splitter, beforeComment = " ", ""
	case divStringLayout:
		splitter, beforeComment = "\n", ""
	case divNewLineStringLayout:
		splitter, beforeComment = "\n", "\n"
	default:
		panic(errors.Errorf("unsupported stringLayout: %v", layout))
	}

	var sb strings.Builder
	sb.WriteString(beforeComment)
	for _, comment := range token.Comments {
		sb.WriteString(comment)
		sb.WriteString(splitter)
	}
	return sb.String()
}

func printNameListVertical(f format.Formatter, names NameArr) (err error) {
	rest := names
	for len(rest) != 0 {
		var processing NameArr
		processing, rest = rest[:1], rest[1:]

		var noComments NameArr
		noComments, rest = rest.span(noComment)
		processing = append(processing, noComments...)

		maxCharLength := processing.findMaxLength()
		for _, name := range processing {
			if err := printSingleName(f, name, maxCharLength); err != nil {
				return err
			}
		}
	}
	return nil
}

func joinNames(names NameArr) string {
	var sb strings.Builder
	for _, name := range names {
		sb.WriteString(" ")
		sb.WriteString(getTokenComment(name.Token, spanStringLayout))
		sb.WriteString(name.Token.Val)
	}
	return sb.String()
}

func printSingleName(f format.Formatter, name *parser.Name, maxCharLength int) error {
	cmt := getTokenComment(name.Token, divNewLineStringLayout)
	if _, err := f.Format(escapePercent(cmt)); err != nil {
		return err
	}
	strLit := name.LiteralStringOpt
	if strLit != nil && strLit.Token != nil {
		_, err := f.Format("%-*s %s\n", maxCharLength, name.Token.Val, strLit.Token.Val)
		return err
	} else {
		_, err := f.Format("%s\n", name.Token.Val)
		return err
	}
}

type NameArr []*parser.Name

func (ns NameArr) span(pred func(*parser.Name) bool) (NameArr, NameArr) {
	first := ns.takeWhile(pred)
	second := ns[len(first):]
	return first, second
}

func (ns NameArr) takeWhile(pred func(*parser.Name) bool) NameArr {
	for i, def := range ns {
		if pred(def) {
			continue
		}
		return ns[:i]
	}
	return ns
}

func (ns NameArr) findMaxLength() int {
	maxLen := -1
	for _, s := range ns {
		if len(s.Token.Val) > maxLen {
			maxLen = len(s.Token.Val)
		}
	}
	return maxLen
}

func hasComments(n *parser.Name) bool {
	return len(n.Token.Comments) != 0
}

func noComment(n *parser.Name) bool {
	return !hasComments(n)
}

func containsActionInRule(rule *parser.Rule) bool {
	for _, b := range rule.Body {
		if _, ok := b.(*parser.Action); ok {
			return true
		}
	}
	return false
}

type RuleArr []*parser.Rule

func printRules(f format.Formatter, rules RuleArr) (err error) {
	var lastRuleName string
	for _, rule := range rules {
		if rule.Name.Val == lastRuleName {
			cmt := getTokenComment(rule.Token, divStringLayout)
			_, err = f.Format("\n%s|\t%i", cmt)
		} else {
			cmt := getTokenComment(rule.Name, divStringLayout)
			_, err = f.Format("\n\n%s%s:%i\n", cmt, rule.Name.Val)
		}
		if err != nil {
			return err
		}
		lastRuleName = rule.Name.Val

		if err = printRuleBody(f, rule); err != nil {
			return err
		}
		if _, err = f.Format("%u"); err != nil {
			return err
		}
	}
	_, err = f.Format("\n%%%%\n")
	return err
}

type ruleItemType int8

const (
	identRuleItemType      ruleItemType = 1
	actionRuleItemType     ruleItemType = 2
	strLiteralRuleItemType ruleItemType = 3
)

func printRuleBody(f format.Formatter, rule *parser.Rule) error {
	firstRuleItem, counter := rule.RuleItemList, 0
	for ri := rule.RuleItemList; ri != nil; ri = ri.RuleItemList {
		switch ruleItemType(ri.Case) {
		case identRuleItemType, strLiteralRuleItemType:
			term := fmt.Sprintf(" %s", ri.Token.Val)
			if ri == firstRuleItem {
				term = term[1:]
			}
			cmt := getTokenComment(ri.Token, divStringLayout)

			if _, err := f.Format(escapePercent(cmt)); err != nil {
				return err
			}
			if _, err := f.Format("%s", term); err != nil {
				return err
			}
		case actionRuleItemType:
			isFirstRuleItem := ri == firstRuleItem
			if err := handlePrecedence(f, rule.Precedence, isFirstRuleItem); err != nil {
				return err
			}
			if err := handleAction(f, rule, ri.Action, isFirstRuleItem); err != nil {
				return err
			}
		}
		counter += 1
	}
	if err := checkInconsistencyInYaccParser(f, rule, counter); err != nil {
		return err
	}
	if !containsActionInRule(rule) {
		if err := handlePrecedence(f, rule.Precedence, counter == 0); err != nil {
			return err
		}
	}
	return nil
}

func handleAction(f format.Formatter, rule *parser.Rule, action *parser.Action, isFirstItem bool) error {
	if !isFirstItem || rule.Precedence != nil {
		if _, err := f.Format("\n"); err != nil {
			return err
		}
	}

	cmt := getTokenComment(action.Token, divStringLayout)
	if _, err := f.Format(escapePercent(cmt)); err != nil {
		return err
	}

	goSnippet, err := formatGoSnippet(action.Values)
	goSnippet = escapePercent(goSnippet)
	if err != nil {
		return err
	}
	snippet := "{}"
	if len(goSnippet) != 0 {
		snippet = fmt.Sprintf("{%%i\n%s%%u\n}", goSnippet)
	}
	_, err = f.Format(snippet)
	return err
}

func handlePrecedence(f format.Formatter, p *parser.Precedence, isFirstItem bool) error {
	if p == nil {
		return nil
	}
	if err := Ensure(p.Token).
		and(p.Token2).NotNil(); err != nil {
		return err
	}
	cmt := getTokenComment(p.Token, spanStringLayout)
	if !isFirstItem {
		if _, err := f.Format(" "); err != nil {
			return err
		}
	}
	_, err := f.Format("%s%s %s", cmt, p.Token.Val, p.Token2.Val)
	return err
}

func formatGoSnippet(actVal []*parser.ActionValue) (string, error) {
	tran := &SpecialActionValTransformer{
		store: map[string]string{},
	}
	goSnippet := collectGoSnippet(tran, actVal)
	formatted, err := gofmt.Source([]byte(goSnippet))
	if err != nil {
		return "", err
	}
	formattedSnippet := tran.restore(string(formatted))
	return strings.TrimSpace(formattedSnippet), nil
}

func collectGoSnippet(tran *SpecialActionValTransformer, actionValArr []*parser.ActionValue) string {
	var sb strings.Builder
	for _, value := range actionValArr {
		trimTab := removeLineBeginBlanks(value.Src)
		sb.WriteString(tran.transform(trimTab))
	}
	snipWithPar := strings.TrimSpace(sb.String())
	if strings.HasPrefix(snipWithPar, "{") && strings.HasSuffix(snipWithPar, "}") {
		return snipWithPar[1 : len(snipWithPar)-1]
	}
	return ""
}

var lineBeginBlankRegex = regexp.MustCompile("(?m)^[\t ]+")

func removeLineBeginBlanks(src string) string {
	return lineBeginBlankRegex.ReplaceAllString(src, "")
}

type SpecialActionValTransformer struct {
	store map[string]string
}

const yaccFmtVar = "_yaccfmt_var_"

var yaccFmtVarRegex = regexp.MustCompile("_yaccfmt_var_[0-9]{1,5}")

func (s *SpecialActionValTransformer) transform(val string) string {
	if strings.HasPrefix(val, "$") {
		generated := fmt.Sprintf("%s%d", yaccFmtVar, len(s.store))
		s.store[generated] = val
		return generated
	}
	return val
}

func (s *SpecialActionValTransformer) restore(src string) string {
	return yaccFmtVarRegex.ReplaceAllStringFunc(src, func(matched string) string {
		origin, ok := s.store[matched]
		if !ok {
			panic(errors.Errorf("mismatch in SpecialActionValTransformer"))
		}
		return origin
	})
}

type OutputFormatter struct {
	file      *os.File
	readBytes []byte
	out       *bufio.Writer
	formatter strutil.Formatter
}

func (y *OutputFormatter) Setup(filename string) (err error) {
	if y.file, err = os.Create(filename); err != nil {
		return
	}
	y.out = bufio.NewWriter(y.file)
	y.formatter = strutil.IndentFormatter(y.out, "\t")
	return
}

func (y *OutputFormatter) Teardown() error {
	if y.out != nil {
		if err := y.out.Flush(); err != nil {
			return err
		}
	}
	if y.file != nil {
		if err := y.file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (y *OutputFormatter) Format(format string, args ...interface{}) (int, error) {
	return y.formatter.Format(format, args...)
}

func (y *OutputFormatter) Write(bytes []byte) (int, error) {
	return y.formatter.Write(bytes)
}

type NotNilAssert struct {
	idx int
	err error
}

func (n *NotNilAssert) and(target interface{}) *NotNilAssert {
	if n.err != nil {
		return n
	}
	if target == nil {
		n.err = errors.Errorf("encounter nil, index: %d", n.idx)
	}
	n.idx += 1
	return n
}

func (n *NotNilAssert) NotNil() error {
	return n.err
}

func Ensure(target interface{}) *NotNilAssert {
	return (&NotNilAssert{}).and(target)
}

func escapePercent(src string) string {
	return strings.ReplaceAll(src, "%", "%%")
}

func checkInconsistencyInYaccParser(f format.Formatter, rule *parser.Rule, counter int) error {
	if counter == len(rule.Body) {
		return nil
	}
	// pickup rule item in ruleBody
	for i := counter; i < len(rule.Body); i++ {
		body := rule.Body[i]
		switch b := body.(type) {
		case string, int:
			if bInt, ok := b.(int); ok {
				b = fmt.Sprintf("'%c'", bInt)
			}
			term := fmt.Sprintf(" %s", b)
			if i == 0 {
				term = term[1:]
			}
			_, err := f.Format("%s", term)
			return err
		case *parser.Action:
			isFirstRuleItem := i == 0
			if err := handlePrecedence(f, rule.Precedence, isFirstRuleItem); err != nil {
				return err
			}
			if err := handleAction(f, rule, b, isFirstRuleItem); err != nil {
				return err
			}
		}
	}
	return nil
}
