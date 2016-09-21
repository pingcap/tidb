// Copyright (c) 2014 The golex Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"github.com/cznic/lex"
	"github.com/cznic/lexer"
	"log"
	"sort"
	"strings"
)

type renderGo struct {
	noRender
	scStates map[int]bool
}

func (r *renderGo) prolog(l *lex.L) {
	for _, state := range l.StartConditionsStates {
		r.scStates[int(state.Index)] = true
	}
	for _, state := range l.StartConditionsBolStates {
		r.scStates[int(state.Index)] = true
	}
	r.w.Write([]byte("// CAUTION: Generated file - DO NOT EDIT.\n\n"))
	for _, line := range l.DefCode {
		r.w.Write([]byte(line))
	}
	r.wprintf("\nyystate0:\n")
	if l.YYM != "yym" {
		r.wprintf("yyrule := -1\n_ = yyrule")
	}
	if action0 := l.Rules[0].Action; action0 != "" {
		r.w.Write([]byte(action0))
	}
	scNames := map[int]string{}
	for name, i := range l.StartConditions {
		scNames[i] = name
	}
	if len(l.StartConditionsStates) > 1 || len(l.StartConditionsBolStates) != 0 {
		if len(l.StartConditionsBolStates) == 0 {
			r.wprintf("\n\nswitch yyt := %s; yyt {\n", l.YYT)
		} else {
			r.wprintf("\n\nswitch yyt, yyb := %s, %s; yyt {\n", l.YYT, l.YYB)
		}
		r.wprintf("default:\npanic(fmt.Errorf(`invalid start condition %%d`, yyt))\n")

		// Stabilize map ranging
		x := []int{}
		for sc := range l.StartConditionsStates {
			x = append(x, sc)
		}
		sort.Ints(x)

		for _, sc := range x {
			state := l.StartConditionsStates[sc]
			r.wprintf("case %d: // start condition: %s\n", sc, scNames[sc])
			if state, ok := l.StartConditionsBolStates[sc]; ok {
				r.wprintf("if yyb { goto yystart%d }\n", state.Index)
			}
			r.wprintf("goto yystart%d\n", state.Index)
		}
		r.wprintf("}\n\n")
	} else {
		r.wprintf("\n\ngoto yystart%d\n\n", l.StartConditionsStates[0].Index)
	}
}

func isReturn(code string) bool {
	const ret = "return"
	lenret := len(ret)
	lines := strings.Split(code, "\n")
	for {
		l := len(lines)
		if l == 0 {
			break
		}

		line := strings.TrimSpace(lines[l-1])
		if line == "" {
			lines = lines[:l-1]
			continue
		}

		if len(line) >= lenret && line[:lenret] == ret {
			if len(line) == lenret {
				return true
			}

			if c := line[lenret]; c == ' ' || c == '\t' {
				return true
			}
		}

		break

	}
	return false
}

func (r *renderGo) rules(l *lex.L) {
	for i := 1; i < len(l.Rules); i++ {
		rule := l.Rules[i]
		r.wprintf("yyrule%d: // %s\n", i, rule.Pattern)
		act := strings.TrimSpace(rule.Action)
		if act != "" && act != "|" {
			r.wprintf("{\n")
			r.w.Write([]byte(rule.Action))
		}
		if act != "|" {
			r.wprintf("\n")
			if !isReturn(rule.Action) {
				r.wprintf("goto yystate0\n")
			}
		}
		if act != "" && act != "|" {
			r.wprintf("}\n")
		}
	}
	r.wprintf(`panic("unreachable")` + "\n")
}

func (r *renderGo) scanFail(l *lex.L) {
	r.wprintf("\ngoto yyabort // silence unused label error\n")
	r.wprintf("\nyyabort: // no lexem recognized\n")
}

func (r *renderGo) userCode(l *lex.L) {
	if userCode := l.UserCode; userCode != "" {
		r.w.Write([]byte(userCode))
	}
}

func (r *renderGo) defaultTransition(l *lex.L, state *lexer.NfaState) (defaultEdge *lexer.RangesEdge) {
	r.wprintf("default:\n")
	if rule, ok := l.Accepts[state]; ok {
		r.wprintf("goto yyrule%d\n", rule)
		return
	}

	cases := map[rune]bool{}
	for i := 0; i < 256; i++ {
		cases[rune(i)] = true
	}
	for _, edge0 := range state.Consuming {
		switch edge := edge0.(type) {
		default:
			log.Fatalf("unexpected type %T", edge0)
		case *lexer.RuneEdge:
			delete(cases, edge.Rune)
		case *lexer.RangesEdge:
			if defaultEdge == nil || len(edge.Ranges.R32) > len(defaultEdge.Ranges.R32) {
				defaultEdge = edge
			}
			for _, rng := range edge.Ranges.R32 {
				for c := rng.Lo; c <= rng.Hi; c += rng.Stride {
					delete(cases, rune(c))
				}
			}
		}
	}
	if len(cases) != 0 {
		r.wprintf("goto yyabort\n")
		return nil
	}

	if defaultEdge != nil {
		r.wprintf("goto yystate%d // %s\n", defaultEdge.Target().Index, r.rangesEdgeString(defaultEdge, l))
		return
	}

	panic("internal error")
}

func (r *renderGo) rangesEdgeString(edge *lexer.RangesEdge, l *lex.L) string {
	a := []string{}
	for _, rng := range edge.Ranges.R32 {
		if rng.Stride != 1 {
			panic("internal error")
		}

		if rng.Hi-rng.Lo == 1 {
			a = append(a, fmt.Sprintf("%s == %s || %s == %s", l.YYC, q(rng.Lo), l.YYC, q(rng.Hi)))
			continue
		}

		if rng.Hi-rng.Lo > 0 {
			a = append(a, fmt.Sprintf("%s >= %s && %s <= %s", l.YYC, q(rng.Lo), l.YYC, q(rng.Hi)))
			continue
		}

		// rng.Hi == rng.Lo
		a = append(a, fmt.Sprintf("%s == %s", l.YYC, q(rng.Lo)))
	}
	return strings.Replace(strings.Join(a, " || "), "%", "%%", -1)
}

func (r *renderGo) transitions(l *lex.L, state *lexer.NfaState) {
	r.wprintf("switch {\n")
	var defaultEdge lexer.Edger = r.defaultTransition(l, state)

	// Stabilize case order
	a := []string{}
	m := map[string]uint{}
	for _, edge0 := range state.Consuming {
		if edge0 == defaultEdge {
			continue
		}

		s := ""
		switch edge := edge0.(type) {
		default:
			log.Fatalf("unexpected type %T", edge0)
		case *lexer.RuneEdge:
			s = fmt.Sprintf("%s == %s", l.YYC, q(uint32(edge.Rune)))
		case *lexer.RangesEdge:
			s = fmt.Sprintf(r.rangesEdgeString(edge, l))
		}
		a = append(a, s)
		m[s] = edge0.Target().Index
	}
	sort.Strings(a)
	for _, s := range a {
		r.wprintf("case %s:\ngoto yystate%d\n", s, m[s])
	}

	r.wprintf("}\n\n")
}

func (r *renderGo) states(l *lex.L) {
	yym := l.YYM != "yym"
	r.wprintf("goto yystate%d // silence unused label error\n", 0)
	if yym {
		r.wprintf("goto yyAction // silence unused label error\n")
		r.wprintf("yyAction:\n")
		r.wprintf("switch yyrule {\n")
		for i := range l.Rules[1:] {
			r.wprintf("case %d:\ngoto yyrule%d\n", i+1, i+1)
		}
		r.wprintf("}\n")
	}
	for _, state := range l.Dfa {
		iState := int(state.Index)
		if _, ok := r.scStates[iState]; ok {
			r.wprintf("goto yystate%d // silence unused label error\n", iState)
		}
		r.wprintf("yystate%d:\n", iState)
		rule, ok := l.Accepts[state]
		if !ok || !l.Rules[rule].EOL {
			r.wprintf("%s\n", l.YYN)
		}
		if ok && l.YYM != "yym" {
			r.wprintf("yyrule = %d\n", rule)
			r.wprintf("%s\n", l.YYM)
		}
		if _, ok := r.scStates[iState]; ok {
			r.wprintf("yystart%d:\n", iState)
		}
		if len(state.Consuming) != 0 {
			r.transitions(l, state)
		} else {
			if rule, ok := l.Accepts[state]; ok {
				r.wprintf("goto yyrule%d\n\n", rule)
			} else {
				panic("internal error")
			}
		}
	}
}

func (r renderGo) render(srcname string, l *lex.L) {
	r.prolog(l)
	r.states(l)
	r.rules(l)
	r.scanFail(l)
	r.userCode(l)
}
