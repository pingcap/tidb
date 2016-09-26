// Copyright 2015 The parser Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package parser

import (
	"bytes"
	"fmt"
	"go/scanner"
	"go/token"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/cznic/golex/lex"
	"github.com/cznic/strutil"
)

// Node represents an AST node.
type Node interface {
	Pos() token.Pos
}

const (
	ccEOF = iota + 0x80
	ccOther
)

type lexer struct {
	*lex.Lexer
	comments        []string
	defs            []*Definition
	errors          scanner.ErrorList
	example         interface{}
	exampleRule     int
	hold            *Token
	lastCommentLast lex.Char
	lastTok         *Token
	marks           int
	pos             token.Pos
	pos2            token.Pos
	positions       []token.Pos
	positions2      []token.Pos
	ruleName        *Token
	rules           []*Rule
	spec            *Specification
	state           int
	value           string
	value2          string
	values          []string
	values2         []string
}

func newLexer(file *token.File, src io.RuneReader) (_ *lexer, err error) {
	l := &lexer{}
	if l.Lexer, err = lex.New(
		file,
		src,
		lex.ErrorFunc(func(pos token.Pos, msg string) {
			l.errors.Add(l.File.Position(pos), msg)
		}),
		lex.RuneClass(runeClass),
	); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *lexer) scan0() lex.Char {
again:
	switch c := l.scan(); c.Rune {
	case COMMENT:
		val := string(l.TokenBytes(nil))
		lastCommentLastLine := -1
		if p := l.lastCommentLast.Pos(); p.IsValid() {
			lastCommentLastLine = l.File.Position(p).Line
		}
		firstLine := l.File.Position(l.First.Pos()).Line
		switch {
		case firstLine-lastCommentLastLine <= 1: // Adjacent
			l.comments = append(l.comments, val)
		default: // New comment block
			l.comments = []string{val}
		}
		l.lastCommentLast = l.Prev
		goto again
	default:
		lastCommentLastLine := -1
		if p := l.lastCommentLast.Pos(); p.IsValid() {
			lastCommentLastLine = l.File.Position(p).Line
		}
		firstLine := l.File.Position(l.First.Pos()).Line
		if firstLine-lastCommentLastLine > 1 { // Non adjacent comment(s)
			l.comments = nil
		}
		return c
	}
}

func (l *lexer) lex() *Token {
	var c lex.Char
	var t *Token
	if t = l.hold; t != nil {
		l.hold = nil
		c = t.Char
	} else {

		c = l.scan0()
		t = &Token{File: l.File, Char: c, Val: string(l.TokenBytes(l.tokenBuilder)), Comments: l.comments}
	}
	l.comments = nil
	if r := c.Rune; r != IDENTIFIER {
		return t
	}

	c2 := l.scan0()
	if c2.Rune == ':' {
		t.Char.Rune = C_IDENTIFIER
		return t
	}

	l.hold = &Token{File: l.File, Char: c2, Val: string(l.TokenBytes(l.tokenBuilder)), Comments: l.comments}
	l.comments = nil
	return t
}

// yyLexer
func (l *lexer) Lex(lval *yySymType) int {
	t := l.lex()
	switch t.Char.Rune {
	case MARK:
		l.marks++
		if l.marks == 2 {
			l.Rule0()
			for l.Next() != lex.RuneEOF {
			}
			v := l.TokenBytes(nil)
			l.value = string(v[:len(v)-1])
		}
	case UNION:
		l.state = 0
		l.pos = l.Lookahead().Pos()
		var val []byte
	loop:
		for balance := -1; balance != 0; {
			l.Rule0()
			c := l.scanGo()
			r := c.Rune
			if r != lex.RuneEOF {
				val = append(val, l.TokenBytes(nil)...)
			}
			switch r := c.Rune; r {
			case lex.RuneEOF:
				break loop
			case '{':
				if balance < 0 {
					balance = 1
					break
				}

				balance++
			case '}':
				balance--
			}
		}
		l.value = string(val)
	case LCURL:
		l.state = 0
		l.pos = l.Lookahead().Pos()
		var val []byte
		var prev lex.Char
	loop2:
		for {
			l.Rule0()
			c := l.scanGo()
			r := c.Rune
			if r != lex.RuneEOF {
				val = append(val, l.TokenBytes(nil)...)
			}
			switch r := c.Rune; r {
			case lex.RuneEOF:
				break loop2
			case '}':
				if prev.Rune == '%' {
					l.Unget(c, prev)
					break loop2
				}
			}
			prev = l.Prev
		}
		l.value = string(val[:len(val)-2])
	case '{':
		l.state = 1
		l.pos = l.Prev.Pos()
		l.values = []string{string(l.TokenBytes(nil))}
		l.positions = []token.Pos{l.First.Pos()}
		balance := 1
	loop3:
		for {
			l.Rule0()
			c := l.scanGo()
			r := c.Rune
			part := string(l.TokenBytes(nil))
			if r != lex.RuneEOF {
				switch {
				case strings.HasPrefix(part, "$"):
					l.values = append(l.values, part)
					l.positions = append(l.positions, l.First.Pos())
				default:
					n := len(l.values) - 1
					s := l.values[n]
					if strings.HasPrefix(s, "$") {
						l.values = append(l.values, part)
						l.positions = append(l.positions, l.First.Pos())
						break
					}

					l.values[n] = s + part
				}
			}
			switch r {
			case lex.RuneEOF:
				break loop3
			case '{':
				balance++
			case '}':
				balance--
				if balance == 0 {
					l.Unget(l.Lookahead(), c)
					break loop3
				}
			}
		}
	}
	lval.Token = t
	l.lastTok = t
	return int(t.Rune)
}

// yyLexer
func (l *lexer) Error(msg string) {
	l.err(l.lastTok.Char.Pos(), "%v", msg)
}

// yyLexerEx
func (l *lexer) Reduced(rule, state int, lval *yySymType) (stop bool) {
	if n := l.exampleRule; n >= 0 && rule != n {
		return false
	}

	switch x := lval.node.(type) {
	case interface {
		fragment() interface{}
	}:
		l.example = x.fragment()
	default:
		l.example = x
	}
	return true
}

func (l *lexer) err(pos token.Pos, msg string, arg ...interface{}) {
	l.errors.Add(l.File.Position(pos), fmt.Sprintf(msg, arg...))
}

func (l *lexer) char(r int) lex.Char {
	return lex.NewChar(l.First.Pos(), rune(r))
}

func (l *lexer) errChar(c lex.Char, msg string, arg ...interface{}) {
	l.err(c.Pos(), msg, arg...)
}

func (l *lexer) byteValue(dst *bytes.Buffer, in []lex.Char, report rune) int {
	switch r := in[1].Rune; {
	case r == '\'':
		if r == report {
			l.errChar(in[1], "unknown escape sequence: '")
		}
		dst.WriteString(strconv.QuoteRune('\''))
		return 2
	case r == '"':
		if r == report {
			l.errChar(in[1], "unknown escape sequence: \"")
		}
		dst.WriteString(strconv.QuoteRune('"'))
		return 2
	case r == '\\':
		dst.WriteString(strconv.QuoteRune('\\'))
		return 2
	case r == 'a':
		dst.WriteString(strconv.QuoteRune('\a'))
		return 2
	case r == 'b':
		dst.WriteString(strconv.QuoteRune('\b'))
		return 2
	case r == 'f':
		dst.WriteString(strconv.QuoteRune('\f'))
		return 2
	case r == 'n':
		dst.WriteString(strconv.QuoteRune('\n'))
		return 2
	case r == 'r':
		dst.WriteString(strconv.QuoteRune('\r'))
		return 2
	case r == 't':
		dst.WriteString(strconv.QuoteRune('\t'))
		return 2
	case r == 'v':
		dst.WriteString(strconv.QuoteRune('\v'))
		return 2
	case r >= '0' && r <= '7':
		val := r - '0'
		n := 2
		for _, v := range in[2:] {
			r = v.Rune
			if r < '0' || r > '7' {
				l.errChar(v, "non-octal character in escape sequence: %c", r)
				return n
			}
			val = val<<3 + r - '0'

			n++
			if n == 4 {
				dst.WriteString(strconv.QuoteRune(rune(byte(val))))
				return n
			}
		}
	case r == 'x':
		val := 0
		n := 2
		for _, v := range in[2:] {
			r = v.Rune
			if !isHex(r) {
				l.errChar(v, "non-hex character in escape sequence: %c", r)
				return n
			}
			val = val<<4 + hexNibble(r)

			n++
			if n == 4 {
				dst.WriteString(strconv.QuoteRune(rune(byte(val))))
				return n
			}
		}
	case r == 'u':
		r := rune(hexNibble(in[2].Rune)<<12 |
			hexNibble(in[3].Rune)<<8 |
			hexNibble(in[4].Rune)<<4 |
			hexNibble(in[5].Rune))
		if !isValidRune(r) {
			l.errChar(l.First, "escape sequence is invalid Unicode code point")
		}
		dst.WriteString(strconv.QuoteRune(r))
		return 6
	case r == 'U':
		r := rune(hexNibble(in[2].Rune)<<28 |
			hexNibble(in[3].Rune)<<24 |
			hexNibble(in[4].Rune)<<20 |
			hexNibble(in[5].Rune)<<16 |
			hexNibble(in[6].Rune)<<12 |
			hexNibble(in[7].Rune)<<8 |
			hexNibble(in[8].Rune)<<4 |
			hexNibble(in[9].Rune))
		if !isValidRune(r) {
			l.errChar(l.First, "escape sequence is invalid Unicode code point")
		}
		dst.WriteString(strconv.QuoteRune(r))
		return 10
	}
	panic("internal error")
}

func (l *lexer) parseActionValue(pos token.Pos, src string) *ActionValue {
	src0 := src
	if !strings.HasPrefix(src, "$") {
		return &ActionValue{Type: ActionValueGo, Src: src0, Pos: pos}
	}

	if src == "$$" {
		return &ActionValue{Type: ActionValueDlrDlr, Src: src0, Pos: pos}
	}

	var tag string

	src = src[1:] // Remove leading $
	if strings.HasPrefix(src, "<") {
		i := strings.Index(src, ">")
		if i < 0 {
			panic("internal error")
		}

		tag = src[len("<"):i]
		src = src[i+1:]
	}

	if src == "$" {
		return &ActionValue{Type: ActionValueDlrTagDlr, Tag: tag, Src: src0, Pos: pos}
	}

	n, err := strconv.ParseInt(src, 10, 31)
	if err != nil {
		l.err(pos, "%v: %s", err, src)
		return nil
	}

	if tag != "" {
		return &ActionValue{Type: ActionValueDlrTagNum, Tag: tag, Num: int(n), Src: src0, Pos: pos}
	}

	return &ActionValue{Type: ActionValueDlrNum, Num: int(n), Src: src0, Pos: pos}
}

func (l *lexer) tokenBuilder(buf *bytes.Buffer) {
	in := l.Token()
	switch r := in[0].Rune; {
	case r == '\'':
		r := in[1].Rune
		if r == '\\' {
			l.byteValue(buf, in[1:], '"')
			return
		}

		if r == '\'' {
			l.errChar(in[1], "empty character literal or unescaped ' in character literal")
		}
		buf.WriteString(strconv.QuoteRune(r))
	default:
		for _, c := range in {
			buf.WriteRune(c.Rune)
		}
	}
}

func (l *lexer) ident(t *Token) interface{} {
	s := t.Val
	if s[0] == '\'' {
		s, err := strconv.Unquote(s)
		if err != nil && l != nil {
			l.err(t.Pos(), "%v", err)
			return 0
		}

		return int([]rune(s)[0])
	}

	return s
}

func (l *lexer) number(t *Token) int {
	n, err := strconv.ParseUint(t.Val, 10, 31)
	if err != nil {
		l.err(t.Pos(), "%v", err)
	}
	return int(n)
}

func isValidRune(r rune) bool {
	return !(r >= 0xd800 && r <= 0xdfff || r > 0x10ffff)
}

func isHex(r rune) bool {
	return r >= '0' && r <= '9' || r >= 'a' && r <= 'f' || r >= 'A' && r <= 'F'
}

func hexNibble(r rune) int {
	if r <= '9' {
		return int(r) - '0'
	}

	if r >= 'a' {
		return int(r) - 'a' + 10
	}

	return int(r) - 'A' + 10
}

func runeClass(r rune) int {
	if r >= 0 && r < 0x80 {
		return int(r)
	}

	if r == lex.RuneEOF {
		return ccEOF
	}

	return ccOther
}

func exampleAST(rule int, src string) interface{} {
	r := bytes.NewBufferString(src)
	file := token.NewFileSet().AddFile("example.y", -1, len(src))
	lx, err := newLexer(file, r)
	if err != nil {
		return fmt.Errorf("failed: %v", err)
	}

	lx.exampleRule = rule
	yyParse(lx)
	return lx.example
}

func prettyString(v interface{}) string {
	var b bytes.Buffer
	prettyPrint(&b, v)
	return b.String()
}

func prettyPrint(w io.Writer, v interface{}) {
	if v == nil {
		return
	}

	f := strutil.IndentFormatter(w, "· ")

	defer func() {
		if e := recover(); e != nil {
			f.Format("\npanic: %v", e)
		}
	}()

	prettyPrint0(nil, f, "", "", v)
}

func prettyPrint0(protect map[interface{}]struct{}, sf strutil.Formatter, prefix, suffix string, v interface{}) {
	if v == nil {
		return
	}

	switch x := v.(type) {
	case *Token:
		if x == nil {
			return
		}

		sf.Format("%s%v"+suffix, prefix, x.String())
		return
	}

	rt := reflect.TypeOf(v)
	rv := reflect.ValueOf(v)
	switch rt.Kind() {
	case reflect.Slice:
		if rv.Len() == 0 {
			return
		}

		sf.Format("%s[]%T{ // len %d%i\n", prefix, rv.Index(0).Interface(), rv.Len())
		for i := 0; i < rv.Len(); i++ {
			prettyPrint0(protect, sf, fmt.Sprintf("%d: ", i), ",\n", rv.Index(i).Interface())
		}
		sf.Format("%u}" + suffix)
	case reflect.Struct:
		sf.Format("%s%T{%i\n", prefix, v)
		for i := 0; i < rt.NumField(); i++ {
			f := rv.Field(i)
			if !f.CanInterface() {
				continue
			}

			prettyPrint0(protect, sf, fmt.Sprintf("%s: ", rt.Field(i).Name), ",\n", f.Interface())
		}
		sf.Format("%u}" + suffix)
	case reflect.Ptr:
		if rv.IsNil() {
			return
		}

		rvi := rv.Interface()
		if _, ok := protect[rvi]; ok {
			sf.Format("%s&%T{ /* recursive/repetitive pointee not shown */ }"+suffix, prefix, rv.Elem().Interface())
			return
		}

		if protect == nil {
			protect = map[interface{}]struct{}{}
		}
		protect[rvi] = struct{}{}
		prettyPrint0(protect, sf, prefix+"&", suffix, rv.Elem().Interface())
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8:
		if v := rv.Int(); v != 0 {
			sf.Format("%s%v"+suffix, prefix, v)
		}
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8:
		if v := rv.Uint(); v != 0 {
			sf.Format("%s%v"+suffix, prefix, rv.Uint())
		}
	case reflect.Bool:
		if v := rv.Bool(); v {
			sf.Format("%s%v"+suffix, prefix, rv.Bool())
		}
	case reflect.String:
		s := rv.Interface().(string)
		if s == "" {
			return
		}

		sf.Format("%s%q"+suffix, prefix, s)
	case reflect.Map:
		keys := rv.MapKeys()
		if len(keys) == 0 {
			return
		}

		var buf bytes.Buffer
		nf := strutil.IndentFormatter(&buf, "· ")
		var skeys []string
		for i, k := range keys {
			prettyPrint0(protect, nf, "", "", k.Interface())
			skeys = append(skeys, fmt.Sprintf("%s%10d", buf.Bytes(), i))
		}
		sort.Strings(skeys)
		sf.Format("%s%T{%i\n", prefix, v)
		for _, k := range skeys {
			si := strings.TrimSpace(k[len(k)-10:])
			k = k[:len(k)-10]
			n, _ := strconv.ParseUint(si, 10, 64)
			mv := rv.MapIndex(keys[n])
			prettyPrint0(protect, sf, fmt.Sprintf("%s: ", k), ",\n", mv.Interface())
		}
		sf.Format("%u}" + suffix)
	default:
		panic(fmt.Sprintf("prettyPrint: missing support for reflect.Kind == %v", rt.Kind()))
	}
}

func (r *Rule) collect() {
	for n := r.RuleItemList; n != nil; n = n.RuleItemList {
		switch n.Case {
		case 0: // /* empty */
			return
		case 1: // RuleItemList IDENTIFIER
			r.Body = append(r.Body, (*lexer)(nil).ident(n.Token))
		case 2: // RuleItemList Action
			r.Body = append(r.Body, n.Action)
		case 3: // RuleItemList STRING_LITERAL
			r.Body = append(r.Body, n.Token.Val)
		}
	}
	p := r.Precedence
	if p == nil {
		return
	}

	for p != nil && p.Case == 3 { // Precedence ';'
		p = p.Precedence
	}

	if p != nil && p.Action != nil {
		r.Body = append(r.Body, p.Action)
	}
}
