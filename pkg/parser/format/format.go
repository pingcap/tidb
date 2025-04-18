// Copyright (c) 2014 The sortutil Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/STRUTIL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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

package format

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

const (
	st0 = iota
	stBOL
	stPERC
	stBOLPERC
)

// Formatter is an io.Writer extended formatter by a fmt.Printf like function Format.
type Formatter interface {
	io.Writer
	Format(format string, args ...interface{}) (n int, errno error)
}

type indentFormatter struct {
	io.Writer
	indent      []byte
	indentLevel int
	state       int
}

var replace = map[rune]string{
	'\000': "\\0",
	'\'':   "''",
	'\n':   "\\n",
	'\r':   "\\r",
}

// IndentFormatter returns a new Formatter which interprets %i and %u in the
// Format() formats string as indent and unindent commands. The commands can
// nest. The Formatter writes to io.Writer 'w' and inserts one 'indent'
// string per current indent level value.
// Behaviour of commands reaching negative indent levels is undefined.
//
//	IndentFormatter(os.Stdout, "\t").Format("abc%d%%e%i\nx\ny\n%uz\n", 3)
//
// output:
//
//	abc3%e
//	    x
//	    y
//	z
//
// The Go quoted string literal form of the above is:
//
//	"abc%%e\n\tx\n\tx\nz\n"
//
// The commands can be scattered between separate invocations of Format(),
// i.e. the formatter keeps track of the indent level and knows if it is
// positioned on start of a line and should emit indentation(s).
// The same output as above can be produced by e.g.:
//
//	f := IndentFormatter(os.Stdout, " ")
//	f.Format("abc%d%%e%i\nx\n", 3)
//	f.Format("y\n%uz\n")
func IndentFormatter(w io.Writer, indent string) Formatter {
	return &indentFormatter{w, []byte(indent), 0, stBOL}
}

func (f *indentFormatter) format(flat bool, format string, args ...interface{}) (n int, errno error) {
	var buf = make([]byte, 0)
	for i := 0; i < len(format); i++ {
		c := format[i]
		switch f.state {
		case st0:
			switch c {
			case '\n':
				cc := c
				if flat && f.indentLevel != 0 {
					cc = ' '
				}
				buf = append(buf, cc)
				f.state = stBOL
			case '%':
				f.state = stPERC
			default:
				buf = append(buf, c)
			}
		case stBOL:
			switch c {
			case '\n':
				cc := c
				if flat && f.indentLevel != 0 {
					cc = ' '
				}
				buf = append(buf, cc)
			case '%':
				f.state = stBOLPERC
			default:
				if !flat {
					for i := 0; i < f.indentLevel; i++ {
						buf = append(buf, f.indent...)
					}
				}
				buf = append(buf, c)
				f.state = st0
			}
		case stBOLPERC:
			switch c {
			case 'i':
				f.indentLevel++
				f.state = stBOL
			case 'u':
				f.indentLevel--
				f.state = stBOL
			default:
				if !flat {
					for i := 0; i < f.indentLevel; i++ {
						buf = append(buf, f.indent...)
					}
				}
				buf = append(buf, '%', c)
				f.state = st0
			}
		case stPERC:
			switch c {
			case 'i':
				f.indentLevel++
				f.state = st0
			case 'u':
				f.indentLevel--
				f.state = st0
			default:
				buf = append(buf, '%', c)
				f.state = st0
			}
		default:
			panic("unexpected state")
		}
	}
	switch f.state {
	case stPERC, stBOLPERC:
		buf = append(buf, '%')
	}
	return fmt.Fprintf(f, string(buf), args...)
}

// Format implements Format interface.
func (f *indentFormatter) Format(format string, args ...interface{}) (n int, errno error) {
	return f.format(false, format, args...)
}

type flatFormatter indentFormatter

// FlatFormatter returns a newly created Formatter with the same functionality as the one returned
// by IndentFormatter except it allows a newline in the 'format' string argument of Format
// to pass through if the indent level is current zero.
//
// If the indent level is non-zero then such new lines are changed to a space character.
// There is no indent string, the %i and %u format verbs are used solely to determine the indent level.
//
// The FlatFormatter is intended for flattening of normally nested structure textual representation to
// a one top level structure per line form.
//
//	FlatFormatter(os.Stdout, " ").Format("abc%d%%e%i\nx\ny\n%uz\n", 3)
//
// output in the form of a Go quoted string literal:
//
//	"abc3%%e x y z\n"
func FlatFormatter(w io.Writer) Formatter {
	return (*flatFormatter)(IndentFormatter(w, "").(*indentFormatter))
}

// Format implements Format interface.
func (f *flatFormatter) Format(format string, args ...interface{}) (n int, errno error) {
	return (*indentFormatter)(f).format(true, format, args...)
}

// OutputFormat output escape character with backslash.
func OutputFormat(s string) string {
	var buf bytes.Buffer
	for _, old := range s {
		if newVal, ok := replace[old]; ok {
			buf.WriteString(newVal)
			continue
		}
		buf.WriteRune(old)
	}

	return buf.String()
}

// RestoreFlags mark the Restore format
type RestoreFlags uint64

// Mutually exclusive group of `RestoreFlags`:
// [RestoreStringSingleQuotes, RestoreStringDoubleQuotes]
// [RestoreKeyWordUppercase, RestoreKeyWordLowercase]
// [RestoreNameUppercase, RestoreNameLowercase]
// [RestoreNameDoubleQuotes, RestoreNameBackQuotes]
// The flag with the left position in each group has a higher priority.
const (
	RestoreStringSingleQuotes RestoreFlags = 1 << iota
	RestoreStringDoubleQuotes
	RestoreStringEscapeBackslash

	RestoreKeyWordUppercase
	RestoreKeyWordLowercase

	RestoreNameUppercase
	RestoreNameLowercase
	RestoreNameDoubleQuotes
	RestoreNameBackQuotes

	RestoreSpacesAroundBinaryOperation
	RestoreBracketAroundBinaryOperation

	RestoreStringWithoutCharset
	RestoreStringWithoutDefaultCharset

	RestoreTiDBSpecialComment
	SkipPlacementRuleForRestore
	RestoreWithTTLEnableOff
	RestoreWithoutSchemaName
	RestoreWithoutTableName
	RestoreForNonPrepPlanCache

	RestoreBracketAroundBetweenExpr
)

const (
	// DefaultRestoreFlags is the default value of RestoreFlags.
	DefaultRestoreFlags = RestoreStringSingleQuotes | RestoreKeyWordUppercase | RestoreNameBackQuotes
)

func (rf RestoreFlags) has(flag RestoreFlags) bool {
	return rf&flag != 0
}

// HasWithoutSchemaNameFlag returns a boolean indicating when `rf` has `RestoreWithoutSchemaName` flag.
func (rf RestoreFlags) HasWithoutSchemaNameFlag() bool {
	return rf.has(RestoreWithoutSchemaName)
}

// HasWithoutTableNameFlag returns a boolean indicating when `rf` has `RestoreWithoutTableName` flag.
func (rf RestoreFlags) HasWithoutTableNameFlag() bool {
	return rf.has(RestoreWithoutTableName)
}

// HasStringSingleQuotesFlag returns a boolean indicating when `rf` has `RestoreStringSingleQuotes` flag.
func (rf RestoreFlags) HasStringSingleQuotesFlag() bool {
	return rf.has(RestoreStringSingleQuotes)
}

// HasStringDoubleQuotesFlag returns a boolean indicating whether `rf` has `RestoreStringDoubleQuotes` flag.
func (rf RestoreFlags) HasStringDoubleQuotesFlag() bool {
	return rf.has(RestoreStringDoubleQuotes)
}

// HasStringEscapeBackslashFlag returns a boolean indicating whether `rf` has `RestoreStringEscapeBackslash` flag.
func (rf RestoreFlags) HasStringEscapeBackslashFlag() bool {
	return rf.has(RestoreStringEscapeBackslash)
}

// HasKeyWordUppercaseFlag returns a boolean indicating whether `rf` has `RestoreKeyWordUppercase` flag.
func (rf RestoreFlags) HasKeyWordUppercaseFlag() bool {
	return rf.has(RestoreKeyWordUppercase)
}

// HasKeyWordLowercaseFlag returns a boolean indicating whether `rf` has `RestoreKeyWordLowercase` flag.
func (rf RestoreFlags) HasKeyWordLowercaseFlag() bool {
	return rf.has(RestoreKeyWordLowercase)
}

// HasNameUppercaseFlag returns a boolean indicating whether `rf` has `RestoreNameUppercase` flag.
func (rf RestoreFlags) HasNameUppercaseFlag() bool {
	return rf.has(RestoreNameUppercase)
}

// HasNameLowercaseFlag returns a boolean indicating whether `rf` has `RestoreNameLowercase` flag.
func (rf RestoreFlags) HasNameLowercaseFlag() bool {
	return rf.has(RestoreNameLowercase)
}

// HasNameDoubleQuotesFlag returns a boolean indicating whether `rf` has `RestoreNameDoubleQuotes` flag.
func (rf RestoreFlags) HasNameDoubleQuotesFlag() bool {
	return rf.has(RestoreNameDoubleQuotes)
}

// HasNameBackQuotesFlag returns a boolean indicating whether `rf` has `RestoreNameBackQuotes` flag.
func (rf RestoreFlags) HasNameBackQuotesFlag() bool {
	return rf.has(RestoreNameBackQuotes)
}

// HasSpacesAroundBinaryOperationFlag returns a boolean indicating
// whether `rf` has `RestoreSpacesAroundBinaryOperation` flag.
func (rf RestoreFlags) HasSpacesAroundBinaryOperationFlag() bool {
	return rf.has(RestoreSpacesAroundBinaryOperation)
}

// HasRestoreBracketAroundBinaryOperation returns a boolean indicating
// whether `rf` has `RestoreBracketAroundBinaryOperation` flag.
func (rf RestoreFlags) HasRestoreBracketAroundBinaryOperation() bool {
	return rf.has(RestoreBracketAroundBinaryOperation)
}

// HasStringWithoutDefaultCharset returns a boolean indicating
// whether `rf` has `RestoreStringWithoutDefaultCharset` flag.
func (rf RestoreFlags) HasStringWithoutDefaultCharset() bool {
	return rf.has(RestoreStringWithoutDefaultCharset)
}

// HasRestoreBracketAroundBetweenExpr returns a boolean indicating
// whether `rf` has `RestoreBracketAroundBetweenExpr` flag.
func (rf RestoreFlags) HasRestoreBracketAroundBetweenExpr() bool {
	return rf.has(RestoreBracketAroundBetweenExpr)
}

// HasStringWithoutCharset returns a boolean indicating whether `rf` has `RestoreStringWithoutCharset` flag.
func (rf RestoreFlags) HasStringWithoutCharset() bool {
	return rf.has(RestoreStringWithoutCharset)
}

// HasTiDBSpecialCommentFlag returns a boolean indicating whether `rf` has `RestoreTiDBSpecialComment` flag.
func (rf RestoreFlags) HasTiDBSpecialCommentFlag() bool {
	return rf.has(RestoreTiDBSpecialComment)
}

// HasSkipPlacementRuleForRestoreFlag returns a boolean indicating whether `rf` has `SkipPlacementRuleForRestore` flag.
func (rf RestoreFlags) HasSkipPlacementRuleForRestoreFlag() bool {
	return rf.has(SkipPlacementRuleForRestore)
}

// HasRestoreWithTTLEnableOff returns a boolean indicating
// whether to force set TTL_ENABLE='OFF' when restoring a TTL table
func (rf RestoreFlags) HasRestoreWithTTLEnableOff() bool {
	return rf.has(RestoreWithTTLEnableOff)
}

// HasRestoreForNonPrepPlanCache returns a boolean indicating whether `rf` has `RestoreForNonPrepPlanCache` flag.
func (rf RestoreFlags) HasRestoreForNonPrepPlanCache() bool {
	return rf.has(RestoreForNonPrepPlanCache)
}

// RestoreWriter is the interface for `Restore` to write.
type RestoreWriter interface {
	io.Writer
	io.StringWriter
}

// RestoreCtx is `Restore` context to hold flags and writer.
type RestoreCtx struct {
	Flags     RestoreFlags
	In        RestoreWriter
	DefaultDB string
	CTERestorer
}

// NewRestoreCtx returns a new `RestoreCtx`.
func NewRestoreCtx(flags RestoreFlags, in RestoreWriter) *RestoreCtx {
	return &RestoreCtx{Flags: flags, In: in, DefaultDB: ""}
}

// WriteKeyWord writes the `keyWord` into writer.
// `keyWord` will be converted format(uppercase and lowercase for now) according to `RestoreFlags`.
func (ctx *RestoreCtx) WriteKeyWord(keyWord string) {
	switch {
	case ctx.Flags.HasKeyWordUppercaseFlag():
		keyWord = strings.ToUpper(keyWord)
	case ctx.Flags.HasKeyWordLowercaseFlag():
		keyWord = strings.ToLower(keyWord)
	}
	ctx.In.WriteString(keyWord)
}

// WriteWithSpecialComments writes a string with a special comment wrapped.
func (ctx *RestoreCtx) WriteWithSpecialComments(featureID string, fn func() error) error {
	if !ctx.Flags.HasTiDBSpecialCommentFlag() {
		return fn()
	}
	ctx.WritePlain("/*T!")
	if len(featureID) != 0 {
		ctx.WritePlainf("[%s]", featureID)
	}
	ctx.WritePlain(" ")
	if err := fn(); err != nil {
		return err
	}
	ctx.WritePlain(" */")
	return nil
}

// WriteString writes the string into writer
// `str` may be wrapped in quotes and escaped according to RestoreFlags.
func (ctx *RestoreCtx) WriteString(str string) {
	if ctx.Flags.HasStringEscapeBackslashFlag() {
		str = strings.ReplaceAll(str, `\`, `\\`)
	}
	quotes := ""
	switch {
	case ctx.Flags.HasStringSingleQuotesFlag():
		str = strings.ReplaceAll(str, `'`, `''`)
		quotes = `'`
	case ctx.Flags.HasStringDoubleQuotesFlag():
		str = strings.ReplaceAll(str, `"`, `""`)
		quotes = `"`
	}
	ctx.In.WriteString(quotes)
	ctx.In.WriteString(str)
	ctx.In.WriteString(quotes)
}

// WriteName writes the name into writer
// `name` maybe wrapped in quotes and escaped according to RestoreFlags.
func (ctx *RestoreCtx) WriteName(name string) {
	switch {
	case ctx.Flags.HasNameUppercaseFlag():
		name = strings.ToUpper(name)
	case ctx.Flags.HasNameLowercaseFlag():
		name = strings.ToLower(name)
	}
	quotes := ""
	switch {
	case ctx.Flags.HasNameDoubleQuotesFlag():
		name = strings.ReplaceAll(name, `"`, `""`)
		quotes = `"`
	case ctx.Flags.HasNameBackQuotesFlag():
		name = strings.ReplaceAll(name, "`", "``")
		quotes = "`"
	}

	// use `WriteString` directly instead of `fmt.Fprint` to get a better performance.
	ctx.In.WriteString(quotes)
	ctx.In.WriteString(name)
	ctx.In.WriteString(quotes)
}

// WritePlain writes the plain text into writer without any handling.
func (ctx *RestoreCtx) WritePlain(plainText string) {
	ctx.In.WriteString(plainText)
}

// WritePlainf write the plain text into writer without any handling.
func (ctx *RestoreCtx) WritePlainf(format string, a ...interface{}) {
	fmt.Fprintf(ctx.In, format, a...)
}

// CTERestorer is used by WithClause related nodes restore.
type CTERestorer struct {
	CTENames []string
}

// IsCTETableName returns true if the given tableName comes from CTE.
func (c *CTERestorer) IsCTETableName(nameL string) bool {
	for _, n := range c.CTENames {
		if n == nameL {
			return true
		}
	}
	return false
}

// RecordCTEName records the CTE name.
func (c *CTERestorer) RecordCTEName(nameL string) {
	c.CTENames = append(c.CTENames, nameL)
}

// RestoreCTEFunc is used to restore CTE.
func (c *CTERestorer) RestoreCTEFunc() func() {
	l := len(c.CTENames)
	return func() {
		if l == 0 {
			c.CTENames = nil
		} else {
			c.CTENames = c.CTENames[:l]
		}
	}
}
