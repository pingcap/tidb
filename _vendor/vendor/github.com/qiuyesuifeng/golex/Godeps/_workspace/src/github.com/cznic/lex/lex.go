// Copyright (c) 2014 The lex Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package lex provides support for a *nix (f)lex like tool on .l sources.
// The syntax is similar to a subset of (f)lex, see also: http://flex.sourceforge.net/manual/Format.html#Format
//
// Changelog
//
// 2014-11-18: Add option for marking an accepting state. Required to support
// POSIX longest match.
//
// Some feature examples:
//
//	/* Unindented multiline Go comments in the definitions section */
//
//		Any indented text in the definitions section
//
//	%{
//	Any text in the definitions section within %{ and %}
//	%}
//
//	D [0-9]
//
//	%s non-exclusive-start-condition s2 s3
//
//	%x exclusive-start-condition e2
//
//	%yyt getTopState() // not required when only INITIAL start condition exists
//	%yyb last == '\n' || last = '\0'
//	%yyc getCurrentChar()
//	%yyn move() // get next character
//	%yym mark() // now in accepting state
//
//	%%
//		Indented text before the first rule is presumably treated specially (renderer specific)
//
//	{D}+	return(INT)
//
//	{D}+\.{D}+
//		return(FLOAT)
//
//	[a-z][a-z0-9]+
//		/* identifier found */
//		return(IDENT)
//
//	A"[foo]\"bar"Z println(`A[foo]"barZ`)
//
//	^bol|eol$
//
//	<non-exclusive-start-condition>foo
//	%{
//		println("foo found")
//	%}
//
//	<s2,s3>bar
//
//	<INITIAL,e2>abc
//
//	<*>"always" println("active in all start conditions")
//
//	%%
//	The optional user code section. Possibly the place where a lexem recognition fail will
//	be handled (renderer specific).
// Missing/differing functionality of the .l parser/FSM generator (compared to flex):
//	- Trailing context (re1/re2).
//	- No requirement of an action to start on the same line as the pattern.
//	- Processing of actions enclosed in braces. This package mostly treats
//	  any non blank text following a pattern up to the next pattern as an action source code.
//	- All flex % prefixed options except %s and %x.
//	- Flex incompatible %yy* options
//	- No cclasses ([[:digit:]]).
//	- Anything special after '(?'.
//	- Matching <<EOF>>. Still \0 is OK in a pattern.
//	- And probably more.
package lex

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/qiuyesuifeng/golex/Godeps/_workspace/src/github.com/cznic/lexer"
	"go/token"
	"io"
	"sort"
	"strings"

	"github.com/qiuyesuifeng/golex/Godeps/_workspace/src/github.com/cznic/fileutil"
)

type rule struct {
	conds               []string
	pattern, re, action string
	in, bolIn           *lexer.NfaState
	bol, eol            bool
}

var (
	defs             = map[string]string{}
	defPos           = map[string]token.Position{}
	defCode          []string
	defRE            = map[string]string{}
	errors_          []string
	rules            = []rule{{}}
	rulePos          = []token.Position{{}}
	unreachableRules = map[int]bool{}
	usrCode          string
	sStarts          = []string{"INITIAL"}
	xStarts          []string
	isXStart         = map[string]bool{}
	iStarts          = map[string]int{"INITIAL": 0}
	defStarts        = map[string]bool{"INITIAL": true}
	unrefStarts      = map[string]bool{}
	allDfa           *dfa
	_yyt             = "yyt"
	_yyb             = "yyb"
	_yyc             = "yyc"
	_yyn             = "yyn"
	_yym             = "yym"
	nodfaopt         bool
	bits32           bool // enables unicode rune processing, standard is byte
)

func logErr(s string) {
	errors_ = append(errors_, s)
}

// Rule represents data for a pattern/action
type Rule struct {
	Conds   []string // Start conditions of the rule
	Pattern string   // Original rule's pattern
	BOL     bool     // Pattern starts with beginning of line assertion (^)
	EOL     bool     // Pattern ends wih end of line ($) assertion
	RE      string   // Pattern translated to a regular expression
	Action  string   // Rule's associated action source code
}

// L represents selected data structures found in / generated from a .l source.
// A [command line] tool using this package may then render L to some
// programming language source code and/or data table(s).
type L struct {
	// Source code lines for rendering from the definitions section
	DefCode []string
	// Names of declared start conditions with their respective numeric
	// identificators
	StartConditions map[string]int
	// Start conditions numeric identificators with their respective DFA
	// start state
	StartConditionsStates map[int]*lexer.NfaState
	// Beginnig of line start conditions numeric identificators with their
	// respective DFA start state
	StartConditionsBolStates map[int]*lexer.NfaState
	// Rule[0] is a pseudo rule. It's action contains the source code for
	// rendering from the rules section before firts rule
	Rules []Rule
	// The generated FSM
	Dfa lexer.Nfa
	// Accept states with their respective rule index
	Accepts map[*lexer.NfaState]int
	// Source code for rendering from the user code section
	UserCode string
	// Source code for rendering of get_current_start_condition. Set by
	// %yyt.
	YYT string
	// Source code for rendering of get_bol, i.e. if we are at the
	// beginning of line right now. Set by %yyb.
	YYB string
	// Source code for rendering of get_peek_char, i.e. the char the lexer
	// will now consider in making of a decision. Set by %yyc.
	YYC string
	// Source code for rendering of move_to_next_char, i.e. "consume" the
	// current peek char and go to the next one. Set by %yyn.
	YYN string
	// Source code for rendering of mark_accepting, support to accept
	// longest matching but reusing the "overflowed" input. Set by %yym.
	YYM string
}

// DfaString returns the textual representation of the Dfa field.
func (l *L) DfaString() string {
	buf := bytes.NewBuffer(nil)

	buf.WriteString("StartConditions:\n")

	// Stabilize
	a := []string{}
	for name := range l.StartConditions {
		a = append(a, name)
	}
	sort.Strings(a)
	for _, name := range a {
		id := l.StartConditions[name]
		if l.StartConditionsStates[id] != nil {
			buf.WriteString(fmt.Sprintf("\t%s, scId:%d, stateId:%d\n", name, id, l.StartConditionsStates[id].Index))
		}
		if l.StartConditionsBolStates[id] != nil {
			buf.WriteString(fmt.Sprintf("\t^%s, scId:%d, stateId:%d\n", name, id, l.StartConditionsBolStates[id].Index))
		}
	}

	buf.WriteString(fmt.Sprintf("DFA:%s\n", l.Dfa.String()))

	// Stabilize
	as, ar := []int{}, map[int]int{}
	for state, rule := range l.Accepts {
		i := int(state.Index)
		as = append(as, i)
		ar[i] = rule
	}
	sort.Ints(as)
	for _, state := range as {
		rule := ar[state]
		buf.WriteString(fmt.Sprintf("state %d accepts rule %d\n", state, rule))
	}

	return buf.String()
}

func (l *L) String() string {
	buf := bytes.NewBuffer(nil)

	if s := l.DefCode; len(s) != 0 {
		buf.WriteString(fmt.Sprintf("DefCode: %q\n", strings.Join(s, "")))
	}
	for id, rule := range l.Rules {
		if id == 0 && rule.Action == "" {
			continue
		}

		buf.WriteString(fmt.Sprintf("Rule %d\n", id))
		if s := rule.Conds; len(s) != 0 {
			buf.WriteString(fmt.Sprintf("\tsc:<%s>\n", strings.Join(s, ",")))
		}
		if s := rule.Pattern; s != "" {
			buf.WriteString(fmt.Sprintf("\tpattern:`%s`\n", s))
			if rule.BOL || rule.EOL {
				buf.WriteString("\tasserts: ")
				if rule.BOL {
					buf.WriteString("BOL ")
				}
				if rule.EOL {
					buf.WriteString("EOL")
				}
				buf.WriteString("\n")
			}
			buf.WriteString(fmt.Sprintf("\tre:`%s`\n", rule.RE))
		}
		if s := rule.Action; s != "" {
			buf.WriteString(fmt.Sprintf("\taction:%q\n", s))
		}
	}
	if s := l.YYT; s != "" {
		buf.WriteString(fmt.Sprintf("YYT: `%s`\n", s))
	}
	if s := l.YYB; s != "" {
		buf.WriteString(fmt.Sprintf("YYB: `%s`\n", s))
	}
	if s := l.YYC; s != "" {
		buf.WriteString(fmt.Sprintf("YYC: `%s`\n", s))
	}
	if s := l.YYN; s != "" {
		buf.WriteString(fmt.Sprintf("YYN: `%s`\n", s))
	}
	buf.WriteString(l.DfaString())
	if s := l.UserCode; s != "" {
		buf.WriteString(fmt.Sprintf("UserCode: %q\n", s))
	}

	return buf.String()
}

var hook bool

// NewL parses a .l source fname from src, returns L or an error if any.
// Currently it is not reentrant and not invokable more than once in an application
// (which is assumed tolerable for a "lex" tool).
// The unoptdfa argument allows to disable optimization of the produced DFA.
// The mode32 parameter is not yet supported and must be false.
func NewL(fname string, src io.RuneReader, unoptdfa, mode32 bool) (l *L, err error) {
	if mode32 {
		return nil, errors.New("lex.NewL: mode32 unsupported yet")
	}

	nodfaopt, bits32 = unoptdfa, mode32
	l = &L{}

	if !hook {
		defer func() {
			if e := recover(); e != nil {
				l = nil
				err = e.(error)
			}
		}()
	}

	// Support \r\n line separators too
	in := []rune{}
loop:
	for {
		r, _, err := src.ReadRune()
		switch {
		case err == nil:
			in = append(in, r)
		case fileutil.IsEOF(err):
			break loop
		default:
			return nil, err
		}
	}
	src = bytes.NewBufferString(strings.Replace(string(in), "\r\n", "\n", -1))

	scanner := lxr.Scanner(fname, src)
	if y := yyParse(newTokenizer(scanner)); y != 0 || len(errors_) != 0 {
		return nil, errors.New(strings.Join(errors_, "\n"))
	}

	computePartialDFAs()
	if len(errors_) != 0 {
		return nil, errors.New(strings.Join(errors_, "\n"))
	}

	computeAllNfa()
	allDfa = allNfa.powerSet()
	for _, irule := range allDfa.acceptRule {
		delete(unreachableRules, irule)
	}
	for irule := range unreachableRules {
		logErr(fmt.Sprintf("%s - pattern `%s` unreachable", rulePos[irule], rules[irule].pattern))
	}
	if len(errors_) != 0 {
		return nil, errors.New(strings.Join(errors_, "\n"))
	}

	l.DefCode = defCode
	l.UserCode = usrCode
	l.StartConditions = iStarts
	l.StartConditionsStates = make(map[int]*lexer.NfaState)
	l.StartConditionsBolStates = make(map[int]*lexer.NfaState)
	for _, edge0 := range allDfa.nfa.in.Consuming {
		switch edge := edge0.(type) {
		default:
			panic(errors.New("internal error"))
		case *lexer.RuneEdge:
			if _, ok := l.StartConditionsStates[int(edge.Rune)]; ok {
				panic(errors.New("internal error"))
			}
			if edge.Rune < 128 {
				l.StartConditionsStates[int(edge.Rune)] = edge.Target()
			} else {
				l.StartConditionsBolStates[int(edge.Rune)-128] = edge.Target()
			}
		case *lexer.RangesEdge:
			for _, rng := range edge.Ranges.R32 {
				for arune := rng.Lo; arune <= rng.Hi; arune += rng.Stride {
					if _, ok := l.StartConditionsStates[int(arune)]; ok {
						panic(errors.New("internal error"))
					}
					if arune < 128 {
						l.StartConditionsStates[int(arune)] = edge.Target()
					} else {
						l.StartConditionsBolStates[int(arune)-128] = edge.Target()
					}
				}
			}
		}

	}
	for _, rule := range rules {
		l.Rules = append(l.Rules, Rule{Conds: rule.conds, Pattern: rule.pattern, RE: rule.re, Action: rule.action, BOL: rule.bol, EOL: rule.eol})
	}
	l.Dfa = allDfa.nfa.nfa[1:]
	l.Accepts = map[*lexer.NfaState]int{}
	for id, state := range allDfa.accept {
		l.Accepts[state] = allDfa.acceptRule[id]
	}
	l.YYT = _yyt
	l.YYB = _yyb
	l.YYC = _yyc
	l.YYN = _yyn
	l.YYM = _yym
	return
}
