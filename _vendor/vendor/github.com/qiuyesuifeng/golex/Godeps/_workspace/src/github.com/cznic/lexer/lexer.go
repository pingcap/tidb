// Copyright (c) 2014 The lexer Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//TODO(jnml) complete package docs

/*

Package lexer provides generating actionless scanners (lexeme recognizers) at run time.

Scanners are defined by regular expressions
and/or lexical grammars, mapping between those definitions, token numeric identifiers and
an optional set of starting id sets, providing simmilar functionality as switching start states in *nix LEX.
The generated FSMs are Unicode arune based and all unicode.Categories and unicode.Scripts are supported by the
regexp syntax using the \p{name} construct.

Syntax supported by ParseRE (ATM a very basic subset of RE2, docs bellow are a mod of: http://code.google.com/p/re2/wiki/Syntax, original docs license unclear)

Single characters:
	.            any character, excluding newline
	[xyz]        character class
	[^xyz]       negated character class
	\p{Greek}    Unicode character class
	\P{Greek}    negated Unicode character class

Composites:
	xy           x followed by y
	x|y          x or y

Repetitions:
	x*           zero or more x
	x+           one or more x
	x?           zero or one x

Grouping:
	(re)         group

Empty strings:
	^            at beginning of text or line
	$            at end of text or line
	\A           at beginning of text
	\z           at end of text

Escape sequences:
	\a           bell (≡ \007)
	\b           backspace (≡ \010)
	\f           form feed (≡ \014)
	\n           newline (≡ \012)
	\r           carriage return (≡ \015)
	\t           horizontal tab (≡ \011)
	\v           vertical tab character (≡ \013)
	\M           M is one of metachars \.+*?()|[]^$
	\xhh         arune \u00hh, h is a hex digit

Character class elements:
	x            single Unicode character
	A-Z          Unicode character range (inclusive)

Unicode character class names--general category:
	Cc           control
	Cf           format
	Co           private use
	Cs           surrogate
	letter       Lu, Ll, Lt, Lm, or Lo
	Ll           lowercase letter
	Lm           modifier letter
	Lo           other letter
	Lt           titlecase letter
	Lu           uppercase letter
	Mc           spacing mark
	Me           enclosing mark
	Mn           non-spacing mark
	Nd           decimal number
	Nl           letter number
	No           other number
	Pc           connector punctuation
	Pd           dash punctuation
	Pe           close punctuation
	Pf           final punctuation
	Pi           initial punctuation
	Po           other punctuation
	Ps           open punctuation
	Sc           currency symbol
	Sk           modifier symbol
	Sm           math symbol
	So           other symbol
	Zl           line separator
	Zp           paragraph separator
	Zs           space separator

Unicode character class names--scripts:
	Arabic                 Arabic
	Armenian               Armenian
	Avestan                Avestan
	Balinese               Balinese
	Bamum                  Bamum
	Bengali                Bengali
	Bopomofo               Bopomofo
	Braille                Braille
	Buginese               Buginese
	Buhid                  Buhid
	Canadian_Aboriginal    Canadian Aboriginal
	Carian                 Carian
	Common                 Common
	Coptic                 Coptic
	Cuneiform              Cuneiform
	Cypriot                Cypriot
	Cyrillic               Cyrillic
	Deseret                Deseret
	Devanagari             Devanagari
	Egyptian_Hieroglyphs   Egyptian Hieroglyphs
	Ethiopic               Ethiopic
	Georgian               Georgian
	Glagolitic             Glagolitic
	Gothic                 Gothic
	Greek                  Greek
	Gujarati               Gujarati
	Gurmukhi               Gurmukhi
	Hangul                 Hangul
	Han                    Han
	Hanunoo                Hanunoo
	Hebrew                 Hebrew
	Hiragana               Hiragana
	Cham                   Cham
	Cherokee               Cherokee
	Imperial_Aramaic       Imperial Aramaic
	Inherited              Inherited
	Inscriptional_Pahlavi  Inscriptional Pahlavi
	Inscriptional_Parthian Inscriptional Parthian
	Javanese               Javanese
	Kaithi                 Kaithi
	Kannada                Kannada
	Katakana               Katakana
	Kayah_Li               Kayah Li
	Kharoshthi             Kharoshthi
	Khmer                  Khmer
	Lao                    Lao
	Latin                  Latin
	Lepcha                 Lepcha
	Limbu                  Limbu
	Linear_B               Linear B
	Lisu                   Lisu
	Lycian                 Lycian
	Lydian                 Lydian
	Malayalam              Malayalam
	Meetei_Mayek           Meetei Mayek
	Mongolian              Mongolian
	Myanmar                Myanmar
	New_Tai_Lue            New Tai Lue
	Nko                    Nko
	Ogham                  Ogham
	Old_Italic             Old Italic
	Old_Persian            Old Persian
	Old_South_Arabian      Old South Arabian
	Old_Turkic             Old Turkic
	Ol_Chiki               Ol Chiki
	Oriya                  Oriya
	Osmanya                Osmanya
	Phags_Pa               Phags Pa
	Phoenician             Phoenician
	Rejang                 Rejang
	Runic                  Runic
	Samaritan              Samaritan
	Saurashtra             Saurashtra
	Shavian                Shavian
	Sinhala                Sinhala
	Sundanese              Sundanese
	Syloti_Nagri           Syloti Nagri
	Syriac                 Syriac
	Tagalog                Tagalog
	Tagbanwa               Tagbanwa
	Tai_Le                 Tai Le
	Tai_Tham               Tai Tham
	Tai_Viet               Tai Viet
	Tamil                  Tamil
	Telugu                 Telugu
	Thaana                 Thaana
	Thai                   Thai
	Tibetan                Tibetan
	Tifinagh               Tifinagh
	Ugaritic               Ugaritic
	Vai                    Vai
	Yi                     Yi

*/
package lexer

import (
	"bytes"
	"fmt"
	"go/scanner"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"unicode"

	"github.com/qiuyesuifeng/golex/Godeps/_workspace/src/golang.org/x/exp/ebnf"
)

type Lexer struct {
	nfa    Nfa
	starts []*NfaState
	accept *NfaState
}

// StartSetID is a type of a lexer start set identificator.
// It is used by Begin and PushState.
type StartSetID int

//TODO:full docs
func CompileLexer(starts [][]int, tokdefs map[string]int, grammar, start string) (lexer *Lexer, err error) {
	lexer = &Lexer{}

	defer func() {
		if e := recover(); e != nil {
			lexer = nil
			err = e.(error)
		}
	}()

	var prodnames string
	res, xref := map[int]string{}, map[int]string{}

	for tokdef, id := range tokdefs {
		if _, ok := res[id]; ok {
			panic(fmt.Errorf("duplicate id %d for token %q", id, tokdef))
		}

		xref[id] = fmt.Sprintf("id-%d", id)
		if re, ok := isRE(tokdef); ok {
			res[id] = re
			continue
		}

		if grammar == "" || !isIdent(tokdef) {
			res[id] = regexp.QuoteMeta(tokdef)
			continue
		}

		if prodnames != "" {
			prodnames += " | "
		}
		prodnames += tokdef
		res[id] = ""
	}

	if prodnames != "" {
		var g ebnf.Grammar
		ebnfSrc := grammar + fmt.Sprintf("\n%s = %s .", start, prodnames)
		if g, err = ebnf.Parse(start, bytes.NewBufferString(ebnfSrc)); err != nil {
			panic(err)
		}

		if err = ebnf.Verify(g, start); err != nil {
			panic(err)
		}

		grammarREs := map[*ebnf.Production]string{}
		for tokdef, id := range tokdefs {
			if isIdent(tokdef) {
				res[id], xref[id] = ebnf2RE(g, tokdef, grammarREs), tokdef
			}
		}
	}

	if starts == nil { // create the default, all inclusive start set
		starts = [][]int{{}}
		for id := range res {
			starts[0] = append(starts[0], id)
		}
	}

	lexer.accept = lexer.nfa.NewState()
	lexer.starts = make([]*NfaState, len(starts))
	for i, set := range starts {
		state := lexer.nfa.NewState()
		lexer.starts[i] = state
		for _, id := range set {
			var in, out *NfaState
			re, ok := res[int(id)]
			if !ok {
				panic(fmt.Errorf("unknown token id %d in set %d", id, i))
			}

			if in, out, err = lexer.nfa.ParseRE(fmt.Sprintf("%s-%s", start, xref[int(id)]), re); err != nil {
				panic(err)
			}

			state.AddNonConsuming(&EpsilonEdge{int(id), in})
			out.AddNonConsuming(&EpsilonEdge{0, lexer.accept})
		}
	}

	lexer.nfa.reduce()
	return
}

// MustCompileLexer is like CompileLexer but panics if the definitions cannot be compiled.
// It simplifies safe initialization of global variables holding compiled Lexers.
func MustCompileLexer(starts [][]int, tokdefs map[string]int, grammar, start string) (lexer *Lexer) {
	var err error
	if lexer, err = CompileLexer(starts, tokdefs, grammar, start); err != nil {
		if list, ok := err.(scanner.ErrorList); ok {
			scanner.PrintError(os.Stderr, list)
		} else {
			log.Fatal(err)
		}
		panic(err)
	}
	return
}

func (lx *Lexer) String() (s string) {
	s = lx.nfa.String()
	for i, set := range lx.starts {
		s += fmt.Sprintf("\nstart set %d = {", i)
		for _, edge := range set.NonConsuming {
			s += " " + strconv.Itoa(int(edge.Target().Index))
		}
		s += " }"
	}
	s += "\naccept: " + strconv.Itoa(int(lx.accept.Index))
	return
}

func identFirst(arune rune) bool {
	return unicode.IsLetter(arune) || arune == '_'
}

func identNext(arune rune) bool {
	return identFirst(arune) || unicode.IsDigit(arune)
}

func isIdent(s string) bool {
	for i, arune := range s {
		if i == 0 && !identFirst(arune) {
			return false
		}

		if !identNext(arune) {
			return false
		}
	}
	return true
}

// isRE checks if a string starts and ends in '/'. If so, return the string w/o the leading and trailing '/' and true.
// Otherwise return the original string and false.
func isRE(s string) (string, bool) {
	if n := len(s); n > 2 && s[0] == '/' && s[n-1] == '/' {
		return s[1 : n-1], true
	}
	return s, false
}

var pipe = map[bool]string{false: "", true: "|"}

func ebnf2RE(g ebnf.Grammar, name string, res map[*ebnf.Production]string) (re string) {
	p := g[name]
	if r, ok := res[p]; ok {
		return r
	}

	buf := bytes.NewBuffer(nil)
	var compile func(string, interface{}, string)

	compile = func(pre string, item interface{}, post string) {
		buf.WriteString(pre)
		switch x := item.(type) {
		default:
			panic(fmt.Errorf("unexpected type %T", x))
		case ebnf.Alternative:
			for i, item := range x {
				compile(pipe[i > 0], item, "")
			}
		case *ebnf.Group:
			compile("(", x.Body, ")")
		case *ebnf.Name:
			buf.WriteString("(" + ebnf2RE(g, x.String, res) + ")")
		case *ebnf.Option:
			compile("(", x.Body, ")?")
		case *ebnf.Range:
			buf.WriteString(fmt.Sprintf("[%s-%s]", regexp.QuoteMeta(x.Begin.String), regexp.QuoteMeta(x.End.String)))
		case *ebnf.Repetition:
			compile("(", x.Body, ")*")
		case ebnf.Sequence:
			for _, item := range x {
				compile("", item, "")
			}
		case *ebnf.Token:
			if s, ok := isRE(x.String); ok {
				buf.WriteString(s)
			} else {
				buf.WriteString(regexp.QuoteMeta(s))
			}
		}
		buf.WriteString(post)
	}

	compile("", p.Expr, "")
	re = buf.String()
	res[p] = re
	return
}

// Scanner returns a new Scanner which can run the Lexer FSM. A Scanner is not safe for concurent access
// but many Scanners can safely share the same Lexer.
//
// The RuneReader can be nil. Then an EOFReader is supplied and
// the real RuneReader(s) can be Included anytime afterwards.
func (lx *Lexer) Scanner(fname string, r io.RuneReader) *Scanner {
	return newScanner(lx, NewScannerSource(fname, r))
}
