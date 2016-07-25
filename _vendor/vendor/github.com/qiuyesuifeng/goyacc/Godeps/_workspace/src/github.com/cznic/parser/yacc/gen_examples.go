// +build ignore

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/qiuyesuifeng/goyacc/Godeps/_workspace/src/github.com/cznic/strutil"
)

var static = map[string]string{
	"$end":          "",
	"'{'":           "\n\t{",
	"'}'":           "\n\t}",
	"ERROR_VERBOSE": "%error-verbose",
	"LCURL":         "\n\n%{",
	"LEFT":          "%left",
	"MARK":          "\n\n%%\n\n",
	"NONASSOC":      "%nonassoc",
	"PREC":          "%prec",
	"PRECEDENCE":    "%precedence",
	"RCURL":         "\n\n%}",
	"RIGHT":         "%right",
	"START":         "%start",
	"TOKEN":         "%token",
	"TYPE":          "%type",
	"UNION":         "%union",
}

func main() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	flag.Parse()
	if flag.NArg() != 2 {
		log.Fatal("expected input and output file name")
	}

	if err := main1(flag.Arg(0), flag.Arg(1)); err != nil {
		log.Fatal(err)
	}
}

func main1(fn, gn string) (err error) {
	b, err := ioutil.ReadFile(fn)
	if err != nil {
		return err
	}

	if len(b) == 0 {
		return fmt.Errorf("empty input examples file")
	}

	b = b[:len(b)-1] // Strip trailing new line.

	f0, err := os.Create(gn)
	if err != nil {
		return err
	}

	defer func() {
		if e := f0.Close(); e != nil && err == nil {
			err = e
		}
	}()

	f := strutil.IndentFormatter(f0, "\t")
	f.Format(`// Copyright 2015 The parser Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// CAUTION: Generated file - DO NOT EDIT!

package parser

import (
	"fmt"
)
`)

	for _, r := range strutil.SplitFields(string(b), "\n") {
		flds := strutil.SplitFields(r, "|")
		rn, err := strconv.Atoi(flds[0])
		if err != nil {
			return err
		}

		nm := flds[1]
		isNil := flds[2] == "nil"
		src := flds[3] // raw
		if src == "" {
			a := make([]string, len(flds)-4)
			var s string
			lit := 0
			for i, tok := range flds[4:] {
				switch x, ok := static[tok]; {
				case ok:
					s = x
				default:
					switch tok {
					case "C_IDENTIFIER":
						s = fmt.Sprintf("%c:\n", lit+'a')
						lit++
					case "IDENTIFIER":
						s = fmt.Sprintf("\n\t%c", lit+'a')
						lit++
					case "NUMBER":
						s = fmt.Sprintf("%d", lit)
						lit++
					case "STRING_LITERAL":
						s = fmt.Sprintf(`"@%c"`, lit+'a')
						lit++
					default:
						if tok[0] == '\'' {
							if s, err = strconv.Unquote(tok); err != nil {
								return fmt.Errorf("%v: %q", err, tok)
							}
							break
						}

						return fmt.Errorf("unsupported token %q", tok)
					}
				}
				if s != "" {
					a[i] = s
				}
			}
			src = strings.Join(a, " ")
			src = strings.Replace(src, "\n\n\n", "\n\n", -1)
			a = strings.Split(src, "\n")
			for i, v := range a {
				a[i] = strings.Trim(v, " ")
			}
			src = "\n" + strings.TrimSpace(strings.Join(a, "\n")) + "\n"
		}

		f.Format("\nfunc %s() {%i\n", nm)
		switch {
		case isNil:
			typ := strings.Split(nm[len("Example"):], "_")[0]
			f.Format("fmt.Println(exampleAST(%d, %u`\n%s\n`%i) == (*%s)(nil))\n", rn, src, typ)
			f.Format(`// Output:
// true
`)
		default:
			f.Format("fmt.Println(exampleAST(%d, %u`\n%s\n`%i))\n", rn, src)
			f.Format("// Output:\n")
		}
		f.Format("%u}\n")
	}

	return nil
}
