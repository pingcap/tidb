// Copyright 2014 The y Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package y

import (
	"bytes"
	"flag"
	"fmt"
	"go/scanner"
	"go/token"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	yparser "github.com/cznic/parser/yacc"
)

func init() {
	isTesting = true
}

func caller(s string, va ...interface{}) {
	_, fn, fl, _ := runtime.Caller(2)
	fmt.Fprintf(os.Stderr, "caller: %s:%d: ", path.Base(fn), fl)
	fmt.Fprintf(os.Stderr, s, va...)
	fmt.Fprintln(os.Stderr)
	_, fn, fl, _ = runtime.Caller(1)
	fmt.Fprintf(os.Stderr, "\tcallee: %s:%d: ", path.Base(fn), fl)
	fmt.Fprintln(os.Stderr)
}

func dbg(s string, va ...interface{}) {
	if s == "" {
		s = strings.Repeat("%v ", len(va))
	}
	_, fn, fl, _ := runtime.Caller(1)
	fmt.Fprintf(os.Stderr, "dbg %s:%d: ", path.Base(fn), fl)
	fmt.Fprintf(os.Stderr, s, va...)
	fmt.Fprintln(os.Stderr)
}

func TODO(...interface{}) string {
	_, fn, fl, _ := runtime.Caller(1)
	return fmt.Sprintf("TODO: %s:%d:\n", path.Base(fn), fl)
}

func use(...interface{}) {}

// ============================================================================

var (
	//oNoDefault = flag.Bool("nodefault", false, "Disable $default")
	oClosures  = flag.Bool("cls", false, "Report non kernel items.")
	oDebugSyms = flag.Bool("ds", false, "Debug symbols.")
	oDev       = flag.String("dev", "", "Process testdata/dev/regex file(s).")
	oLA        = flag.Bool("la", false, "Report all lookahead sets.")
	oNoErr     = flag.Bool("noerr", false, "Disable errors for 'make cpu'.")
	oRE        = flag.String("re", "", "Regexp filter.")
)

func (s itemSet) dump(y *y) string {
	var buf bytes.Buffer
	for _, v := range s {
		buf.WriteString(v.dump(y))
		buf.WriteString("\n")
	}
	return buf.String()
}

func (s itemSet1) dump(y *y) string {
	var a []string
	for v := range s {
		a = append(a, v.dump(y))
	}
	sort.Strings(a)
	return strings.Join(a, "\n")
}

func test0(t *testing.T, root string, filter func(pth string) bool, opts *Options, xerrors bool) {
	const (
		cc    = "testdata/ok/cc.y"
		mysql = "testdata/ok/mysql.y"
	)
	var re *regexp.Regexp
	if s := *oRE; s != "" {
		var err error
		re, err = regexp.CompilePOSIX(s)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := filepath.Walk(root, func(pth string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		ok, err := filepath.Match("*.y", filepath.Base(pth))
		if err != nil {
			t.Fatal(err)
		}

		if !ok || filter != nil && !filter(pth) {
			return nil
		}

		if re != nil && !re.MatchString(pth) {
			return nil
		}

		t0 := time.Now()
		p, err := ProcessFile(token.NewFileSet(), pth, opts)
		t.Log(pth, time.Since(t0))
		if (pth == cc || pth == mysql) && err == nil {
			t.Errorf("%s: should have produced error", cc)
		}
		if err != nil {
			switch x := err.(type) {
			case scanner.ErrorList:
				if pth == cc && len(x) == 1 && strings.Contains(x[0].Error(), " 3 shift/reduce") {
					t.Log(err)
					break
				}

				if pth == mysql {
					a := []string{}
					for _, v := range x {
						a = append(a, v.Error())
					}
					s := strings.Join(a, "\n")
					if len(x) == 2 &&
						strings.Contains(s, "conflicts: 145 shift/reduce") &&
						strings.Contains(s, "conflicts: 2 reduce/reduce") {
						t.Log(err)
						break
					}
				}

				for _, v := range x {
					switch *oNoErr {
					case true:
						t.Log(v)
					default:
						t.Error(v)
					}
				}
			default:
				switch *oNoErr {
				case true:
					t.Logf("%q\n%v", pth, err)
				default:
					t.Errorf("%q\n%v", pth, err)
				}
			}
		}

		if p == nil {
			return nil
		}

		if xerrors {
			var buf bytes.Buffer
			if err := p.SkeletonXErrors(&buf); err != nil {
				t.Error(err)
			} else {
				t.Logf("\n%s", buf.Bytes())
			}
		}

		y := p.y

		if err == nil {
			for si, state := range y.States {
				syms, la := state.Syms0()
				if la != nil {
					syms = append(syms, la)
				}
				stop, err := y.Parser.parse(si, func() *Symbol {
					if len(syms) == 0 {
						return nil
					}

					r := syms[0]
					syms = syms[1:]
					return r
				})

				if stop == si {
					continue
				}

				if err != nil {
					t.Error(err)
				}

				if g, e := stop, si; g != e {
					t.Errorf("state %d not reached (final state %d)", si, stop)
				}
			}
		}

		t.Logf("\tstates %d, parse table entries %d", len(y.States), y.entries)
		if _, err = newBison(pth+".bison", y); err != nil {
			if !os.IsNotExist(err) {
				switch x := err.(type) {
				case scanner.ErrorList:
					for _, v := range x {
						t.Error(v)
					}
				default:
					t.Error(err)
				}
			}
		}

		return nil

	}); err != nil {
		t.Fatal(err)
	}
}

func Test0(t *testing.T) {
	test0(t, "testdata/ok", nil, &Options{}, false)
}

func TestDev(t *testing.T) {
	src := *oDev
	if src == "" {
		return
	}

	re, err := regexp.Compile(src)
	if err != nil {
		t.Fatal(err)
	}

	test0(
		t,
		"./",
		func(pth string) bool {
			return re.MatchString(pth)
		},
		&Options{
			//noDefault: *oNoDefault,
			Closures:  *oClosures,
			LA:        *oLA,
			Report:    os.Stderr,
			Resolved:  true,
			debugSyms: *oDebugSyms,
		},
		true,
	)
}

func benchmark(b *testing.B, pth string) {
	src, err := ioutil.ReadFile(pth)
	if err != nil {
		b.Fatal(err)
	}

	fset := token.NewFileSet()
	ast, err := yparser.Parse(fset, pth, src)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processAST(fset, ast, nil)
	}
}

func BenchmarkCC(b *testing.B) {
	benchmark(b, "testdata/ok/cc.y")
}

func BenchmarkTest(b *testing.B) {
	benchmark(b, "testdata/ok/test.y")
}

func BenchmarkQL(b *testing.B) {
	benchmark(b, "testdata/ok/ql.y")
}

func BenchmarkYacc(b *testing.B) {
	benchmark(b, "testdata/ok/yacc.y")
}

type bison struct {
	lines []string
	xlat  map[int]int
	errl  scanner.ErrorList
}

func newBison(pth string, y *y) (*bison, error) {
	buf, err := ioutil.ReadFile(pth)
	if err != nil {
		return nil, err
	}

	b := &bison{
		xlat: map[int]int{},
	}
	src := strings.Replace(string(buf), "\r", "", -1)
	b.lines = strings.Split(src, "\n")
	for {
		peek, ok := b.peekLine()
		if !ok {
			return nil, fmt.Errorf("bison report: no states found")
		}

		if strings.HasPrefix(peek, "state ") || strings.HasPrefix(peek, "State ") {
			break
		}

		b.mustLine()
	}

	bisonState := 0
	var state itemSet
states:
	for { // state
		line := b.mustLine()[len("state "):]
		n, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}

		if g, e := bisonState, n; g != e {
			return nil, fmt.Errorf("got state number %d, expected %d", g, e)
		}

		for { // skip blanks before items
			l, ok := b.peekLine()
			if !ok {
				return nil, fmt.Errorf("bison report: unexpected EOF")
			}

			if l != "" {
				break
			}

			b.mustLine()
		}

		state = state[:0]
		i2la := map[item]string{}
		for { // items
			l, ok := b.peekLine()
			if !ok {
				return nil, fmt.Errorf("bison report: unexpected EOF")
			}

			if l == "" { // blank -> no more items
				break
			}

			l = b.mustLine()
			l = strings.TrimSpace(l)
			a := strings.Fields(l)
			rn, err := strconv.Atoi(a[0])
			if err != nil {
				return nil, fmt.Errorf("bison report: %v", err)
			}

			var la string
			for i, v := range a {
				if strings.HasPrefix(v, "[") {
					las := append([]string(nil), a[i:]...)
					s := las[0]
					las[0] = s[1:] // -[
					s = las[len(las)-1]
					las[len(las)-1] = s[:len(s)-1] // -]
					for i, v := range las {
						if v[len(v)-1] == ',' {
							las[i] = v[:len(v)-1]
						}
					}
					sort.Strings(las)
					la = strings.Join(las, ", ")
				}
			}
			for dot, v := range a[2:] {
				if v == "." {
					item := newItem(rn, dot)
					i2la[item] = la
					if item == 0 || dot != 0 {
						state, _ = state.add(item)
						break
					}
				}
			}
		}
		s, ok := state.state0(y)
		if !ok {
			switch {
			case len(state) == 1 && state[0].rule() == 0 && state[0].dot() == 2:
			default:
				return nil, fmt.Errorf("bison report: state %d not found", bisonState)
			}
		}

		if ok {
			b.xlat[bisonState] = s
			for item, la := range i2la {
				if la == "" {
					continue
				}

				ys := y.States[s]
				i, ok := ys.kernel.find(item)
				if ok {
					if g, e := ys.lookahead[i].dump(y), la; g != e {
						b.err(
							"state %d (bison state %d), item %s [%s] // bison LA [%s]",
							s, bisonState, item.dump(y), g, e,
						)
					}
					continue
				}

				i, ok = ys.xitems.find(item)
				if !ok {
					return nil, fmt.Errorf("state %d (bison state %d): cannot find item %s", s, bisonState, item.dump(y))
				}

				if g, e := ys.xla[i].dump(y), la; g != e {
					b.err(
						"state %d (bison state %d), item %s [%s] // bison LA [%s]",
						s, bisonState, item.dump(y), g, e,
					)
				}
			}
		}
		for { // skip blanks before actions
			l, ok := b.peekLine()
			if !ok {
				return nil, fmt.Errorf("bison report: unexpected EOF")
			}

			if l != "" {
				break
			}

			b.mustLine()
		}

		for { // actions
			l, ok := b.peekLine()
			if !ok {
				break states
			}

			if strings.HasPrefix(l, "state ") || strings.HasPrefix(l, "State ") {
				break
			}

			if l == "" {
				b.mustLine()
				continue
			}

			l = b.mustLine()
			//TODO process action
		}

		bisonState++
	}

	if g, e := len(b.xlat), len(y.States); g != e {
		return nil, fmt.Errorf("seen %d bison states, expected %d", g, e)
	}
	//TODO more processing
	if len(b.errl) != 0 {
		return b, b.errl
	}

	return b, nil
}

func (b *bison) err(format string, arg ...interface{}) {
	b.errl.Add(token.Position{}, fmt.Sprintf(format, arg...))
}

func (b *bison) line() (string, bool) {
	r, ok := b.peekLine()
	if ok {
		b.lines = b.lines[1:]
	}
	return r, ok
}

func (b *bison) mustLine() string {
	r, ok := b.line()
	if !ok {
		panic(ok)
	}

	return r
}

func (b *bison) peekLine() (string, bool) {
	if len(b.lines) == 0 {
		return "", false
	}

	return b.lines[0], true
}

func (s itemSet) state0(y *y) (int, bool) {
	n, ok := y.itemSets[y.id(s)]
	return n, ok
}

func TestAllocValue(t *testing.T) {
	y := newY(nil, nil, nil)
	for e := 0xe000; e <= 0xf8ff; e++ {
		if y.allocatedValues[e] {
			continue
		}

		if g := y.allocValue(); g != e {
			t.Fatal(g, e)
		}
	}
	for e := 0xf0000; e <= 0xffffd; e++ {
		if g := y.allocValue(); g != e {
			t.Fatal(g, e)
		}
	}
	for e := 0x100000; e <= 0x10fffd; e++ {
		if g := y.allocValue(); g != e {
			t.Fatal(g, e)
		}
	}

	ok := false
	func() {
		defer func() {
			ok = recover() != nil
		}()
		y.allocValue()
	}()

	if g, e := ok, true; g != e {
		t.Fatal(g, e)
	}
}

func Example() {
	p, err := ProcessSource(
		token.NewFileSet(),
		"example.y",
		[]byte(`
// ----------------------------------------------------------------- yacc code
//

%token NUM

%left '+'
%left '*'

%%

E: E '*' E
|  E '+' E
|  NUM

// ---------------------------------------------------------------------------

`),
		&Options{
			Closures: true,
			Report:   os.Stdout,
			Resolved: true,
			XErrorsSrc: []byte(`
// ------------------------------------------------------------ error examples
//

'+' |
'-' "unary operator not supported"

NUM '+' |
NUM '*' "missing operand"

NUM '/' |
NUM '-' "binary operator not supported"

NUM error "expected operator"

error "expected number"

// ---------------------------------------------------------------------------

`),
		})
	if err != nil {
		panic(err)
	}
	fmt.Printf("XErrors\n")
	for i, v := range p.XErrors {
		fmt.Printf("%d: %v lookahead %q, msg %q\n", i, v.Stack, v.Lookahead, v.Msg)
	}

	// Output:
	// state 0 //
	//
	//     0 $accept: . E
	//     1 E: . E '*' E  // assoc %left, prec 2
	//     2 E: . E '+' E  // assoc %left, prec 1
	//     3 E: . NUM
	//
	//     NUM  shift, and goto state 2
	//
	//     E  goto state 1
	//
	// state 1 // NUM [$end]
	//
	//     0 $accept: E .  [$end]
	//     1 E: E . '*' E  // assoc %left, prec 2
	//     2 E: E . '+' E  // assoc %left, prec 1
	//
	//     $end  accept
	//     '*'   shift, and goto state 3
	//     '+'   shift, and goto state 4
	//
	// state 2 // NUM
	//
	//     3 E: NUM .  [$end, '*', '+']
	//
	//     $end  reduce using rule 3 (E)
	//     '*'   reduce using rule 3 (E)
	//     '+'   reduce using rule 3 (E)
	//
	// state 3 // NUM '*'
	//
	//     1 E: . E '*' E  // assoc %left, prec 2
	//     1 E: E '*' . E  // assoc %left, prec 2
	//     2 E: . E '+' E  // assoc %left, prec 1
	//     3 E: . NUM
	//
	//     NUM  shift, and goto state 2
	//
	//     E  goto state 6
	//
	// state 4 // NUM '+'
	//
	//     1 E: . E '*' E  // assoc %left, prec 2
	//     2 E: . E '+' E  // assoc %left, prec 1
	//     2 E: E '+' . E  // assoc %left, prec 1
	//     3 E: . NUM
	//
	//     NUM  shift, and goto state 2
	//
	//     E  goto state 5
	//
	// state 5 // NUM '+' NUM [$end]
	//
	//     1 E: E . '*' E  // assoc %left, prec 2
	//     2 E: E . '+' E  // assoc %left, prec 1
	//     2 E: E '+' E .  [$end, '+']  // assoc %left, prec 1
	//
	//     $end  reduce using rule 2 (E)
	//     '*'   shift, and goto state 3
	//     '+'   reduce using rule 2 (E)
	//
	//     Conflict between rule 2 and token '*' resolved as shift ('+' < '*').
	//     Conflict between rule 2 and token '+' resolved as reduce (%left '+').
	//
	// state 6 // NUM '*' NUM [$end]
	//
	//     1 E: E . '*' E  // assoc %left, prec 2
	//     1 E: E '*' E .  [$end, '*', '+']  // assoc %left, prec 2
	//     2 E: E . '+' E  // assoc %left, prec 1
	//
	//     $end  reduce using rule 1 (E)
	//     '*'   reduce using rule 1 (E)
	//     '+'   reduce using rule 1 (E)
	//
	//     Conflict between rule 1 and token '*' resolved as reduce (%left '*').
	//     Conflict between rule 1 and token '+' resolved as reduce ('+' < '*').
	//
	// XErrors
	// 0: [0] lookahead "'+'", msg "unary operator not supported"
	// 1: [0] lookahead "'-'", msg "unary operator not supported"
	// 2: [0 1 4] lookahead "$end", msg "missing operand"
	// 3: [0 1 3] lookahead "$end", msg "missing operand"
	// 4: [0 2] lookahead "'/'", msg "binary operator not supported"
	// 5: [0 2] lookahead "'-'", msg "binary operator not supported"
	// 6: [0 2] lookahead "<nil>", msg "expected operator"
	// 7: [0] lookahead "<nil>", msg "expected number"
}

func Example_skeletonXErrors() {
	p, err := ProcessSource(
		token.NewFileSet(),
		"example.y",
		[]byte(`
%token NUMBER

%left                '+' '-'
%left                '*' '/'
%precedence        UNARY

%%

Expression:
        Term
|   Term '+' Term
|   Term '-' Term

Term:
        Factor
|   Factor '*' Factor        
|   Factor '/' Factor

Factor:
        NUMBER
|   '+' NUMBER %prec UNARY
|   '-' NUMBER %prec UNARY

`),
		&Options{})
	if err != nil {
		panic(err)
	}

	p.SkeletonXErrors(os.Stdout)
	// Output:
	// /*
	// 	Reject empty input
	// */
	// 0
	// "invalid empty input"
	//
	// 1 // NUMBER
	// 15 // NUMBER '-' NUMBER
	// 16 // NUMBER '+' NUMBER
	// error "expected $end"
	//
	// 0
	// error "expected Expression or one of ['+', '-', NUMBER]"
	//
	// 9 // NUMBER '*'
	// 10 // NUMBER '/'
	// error "expected Factor or one of ['+', '-', NUMBER]"
	//
	// 5 // '+'
	// 6 // '-'
	// error "expected NUMBER"
	//
	// 13 // NUMBER '+'
	// 14 // NUMBER '-'
	// error "expected Term or one of ['+', '-', NUMBER]"
	//
	// 3 // NUMBER
	// 4 // NUMBER
	// 7 // '-' NUMBER
	// 8 // '+' NUMBER
	// error "expected one of [$end, '*', '+', '-', '/']"
	//
	// 2 // NUMBER
	// 11 // NUMBER '/' NUMBER
	// 12 // NUMBER '*' NUMBER
	// error "expected one of [$end, '+', '-']"
}
