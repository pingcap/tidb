// Copyright (c) 2014 The golex Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Golex is a lex/flex like (not fully POSIX lex compatible) utility.
// It renders .l formated data (http://flex.sourceforge.net/manual/Format.html#Format) to Go source code.
// The .l data can come from a file named in a command line argument.
// If no non-opt args are given, golex reads stdin.
//
// Options:
//	-DFA            print the DFA to stdout and quit
//	-nodfaopt       disable DFA optimization - don't use this for production code
//	-o fname        write to file `fname`, default is `lex.yy.go`
//	-t              write to stdout
//	-v              write some scanner statistics to stderr
//	-32bit          assume unicode rune lexer (partially implemented, disabled)
//
// To get the latest golex version:
//
//	$ go get -u github.com/cznic/golex
//
// Run time library
//
// Please see http://godoc.org/github.com/cznic/golex/lex.
//
// Changelog
//
// 2014-11-18: Golex now supports %yym - a hook which can be used to mark an
// accepting state.
//
// Implementing POSIX-like longest match
//
// Consider for example this .l file:
//
//	$ cat main.l
//	%{
//	package main
//
//	import (
//		"flag"
//		"fmt"
//	)
//
//	var (
//		c    byte
//		src  string
//		in   []byte
//		un   []byte
//		mark int
//	)
//
//	func lex() (s string) {
//	%}
//
//	%yyn next()
//	%yyc c
//	%yym fmt.Printf("\tstate accepts: %q\n", in); mark = len(in)
//
//	%%
//		in = in[:0]
//		mark = -1
//
//	\0
//		return "EOF"
//
//	a([bcd]*z([efg]*z)?)?
//		return fmt.Sprintf("match(%q)", in)
//
//	%%
//		if mark >= 0 {
//			if len(in) > mark {
//				unget(c)
//				for i := len(in)-1; i >= mark; i-- {
//					unget(in[i])
//				}
//				next()
//			}
//			in = in[:mark]
//			goto yyAction // Hook: Execute the semantic action of the last matched rule.
//		}
//
//		switch n := len(in); n {
//		case 0: // [] z
//			s = fmt.Sprintf("%q", c)
//			next()
//		case 1: // [x] z
//			s = fmt.Sprintf("%q", in[0])
//		default: // [x, y, ...], z
//			s = fmt.Sprintf("%q", in[0])
//			unget(c) // z
//			for i := n - 1; i > 1; i-- {
//				unget(in[i]) // ...
//			}
//			c = in[1] // y
//		}
//		return s
//	}
//
//	func next() {
//		if len(un) != 0 {
//			c = un[len(un)-1]
//			un = un[:len(un)-1]
//			return
//		}
//
//		in = append(in, c)
//		if len(src) == 0 {
//			c = 0
//			return
//		}
//
//		c = src[0]
//		fmt.Printf("\tnext: %q\n", c)
//		src = src[1:]
//	}
//
//	func unget(b byte) {
//		un = append(un, b)
//	}
//
//	func main() {
//		flag.Parse()
//		if flag.NArg() > 0 {
//			src = flag.Arg(0)
//		}
//		next()
//		for {
//			s := lex()
//			fmt.Println(s)
//			if s == "EOF" {
//				break
//			}
//		}
//	}
//	$
//
// Execution and output:
//
//	$ golex -o main.go main.l && go run main.go abzez0abzefgxy
//		next: 'a'
//		next: 'b'
//		state accepts: "a"
//		next: 'z'
//		next: 'e'
//		state accepts: "abz"
//		next: 'z'
//		next: '0'
//		state accepts: "abzez"
//	match("abzez")
//		next: 'a'
//	'0'
//		next: 'b'
//		state accepts: "a"
//		next: 'z'
//		next: 'e'
//		state accepts: "abz"
//		next: 'f'
//		next: 'g'
//		next: 'x'
//	match("abz")
//	'e'
//	'f'
//	'g'
//		next: 'y'
//	'x'
//	'y'
//		state accepts: "\x00"
//	EOF
//	$
//
// 2014-11-15: Golex's output is now gofmt'ed, if possible.
//
// Missing/differing functionality of the current renderer (compared to flex):
//	- No runtime tokenizer package/environment
//	  (but the freedom to have/write any fitting one's specific task(s)).
//	- The generated FSM picks the rules in the order of their appearance in the .l source,
//	  but "flex picks the rule that matches the most text".
//	- And probably more.
// Further limitations on the .l source are listed in the cznic/lex package godocs.
//
// A simple golex program example (make example1 && ./example1):
//
//	%{
//	package main
//
//	import (
//	    "bufio"
//	    "fmt"
//	    "log"
//	    "os"
//	)
//
//	var (
//	    src      = bufio.NewReader(os.Stdin)
//	    buf      []byte
//	    current  byte
//	)
//
//	func getc() byte {
//	    if current != 0 {
//	        buf = append(buf, current)
//	    }
//	    current = 0
//	    if b, err := src.ReadByte(); err == nil {
//	        current = b
//	    }
//	    return current
//	}
//
//	//    %yyc is a "macro" to access the "current" character.
//	//
//	//    %yyn is a "macro" to move to the "next" character.
//	//
//	//    %yyb is a "macro" to return the begining-of-line status (a bool typed value).
//	//        It is used for patterns like `^re`.
//	//        Example: %yyb prev == 0 || prev == '\n'
//	//
//	//    %yyt is a "macro" to return the top/current start condition (an int typed value).
//	//        It is used when there are patterns with conditions like `<cond>re`.
//	//        Example: %yyt startCond
//
//	func main() { // This left brace is closed by *1
//	    c := getc() // init
//	%}
//
//	%yyc c
//	%yyn c = getc()
//
//	D   [0-9]+
//
//	%%
//	    buf = buf[:0]   // Code before the first rule is executed before every scan cycle (state 0 action)
//
//	[ \t\n\r]+          // Ignore whitespace
//
//	{D}                 fmt.Printf("int %q\n", buf)
//
//	{D}\.{D}?|\.{D}     fmt.Printf("float %q\n", buf)
//
//	\0                  return // Exit on EOF or any other error
//
//	.                   fmt.Printf("%q\n", buf) // Printout any other unrecognized stuff
//
//	%%
//	    // The rendered scanner enters top of the user code section when
//	    // lexem recongition fails. In this example it should never happen.
//	    log.Fatal("scanner internal error")
//
//	} // *1 this right brace
package main
