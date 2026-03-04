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

package ast

import (
	"bytes"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/parser/util"
)

const hexDigits = "0123456789abcdef"

// node is the struct implements Node interface except for Accept method.
// Node implementations should embed it in.
type node struct {
	utf8Text           string
	enc                charset.Encoding
	noBackslashEscapes bool
	once               *sync.Once

	text   string
	offset int
}

// SetOriginTextPosition implements Node interface.
func (n *node) SetOriginTextPosition(offset int) {
	n.offset = offset
}

// OriginTextPosition implements Node interface.
func (n *node) OriginTextPosition() int {
	return n.offset
}

// SetText implements Node interface.
func (n *node) SetText(enc charset.Encoding, text string) {
	n.enc = enc
	n.text = text
	n.once = &sync.Once{}
}

// SetNoBackslashEscapes marks that the SQL mode NO_BACKSLASH_ESCAPES was active
// when this node was parsed, so backslash is not treated as an escape character
// in string literals
func (n *node) SetNoBackslashEscapes(val bool) {
	n.noBackslashEscapes = val
}

// Text implements Node interface.
func (n *node) Text() string {
	if n.once == nil {
		return n.text
	}
	n.once.Do(func() {
		if n.enc == nil {
			n.utf8Text = n.text
			return
		}
		n.utf8Text = convertBinaryStringLiterals(n.text, n.enc, n.noBackslashEscapes)
	})
	return n.utf8Text
}

// OriginalText implements Node interface.
func (n *node) OriginalText() string {
	return n.text
}

func isPrintable(s []byte) bool {
	if !utf8.Valid(s) {
		return false
	}
	for _, r := range string(s) {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// convertBinaryStringLiterals processes raw SQL text, converting non-printable
// single- or double-quoted string literals to 0x hex literals and applying
// OpDecodeReplace to everything else.
func convertBinaryStringLiterals(text string, enc charset.Encoding, noBackslashEscapes bool) string {
	src := charset.HackSlice(text)

	// Fast path: if no quotes, just transform the whole thing
	if bytes.IndexByte(src, '\'') < 0 && bytes.IndexByte(src, '"') < 0 {
		result, _ := enc.Transform(nil, src, charset.OpDecodeReplace)
		return charset.HackString(result)
	}

	var buf *bytes.Buffer
	var tmp bytes.Buffer
	lastCopied := 0
	i := 0

	for i < len(src) {
		if src[i] != '\'' && src[i] != '"' {
			i++
			continue
		}

		quoteStart := i
		quote := src[i]
		i++

		// Find closing quote
		terminated := false
		for i < len(src) {
			ch := src[i]
			if ch == quote {
				i++
				if i >= len(src) || src[i] != quote {
					terminated = true
					break
				}
				i++ // doubled quote escape
			} else if ch == '\\' && !noBackslashEscapes && i+1 < len(src) {
				i += 2 // skip escaped char
			} else {
				i++
			}
		}

		decoded, err := enc.Transform(nil, src[quoteStart+1:i-1], charset.OpDecode)
		if (!terminated) || (err == nil && isPrintable(decoded)) {
			continue
		}

		// String contains non-printable UTF8 characters so rescan
		// the string literal to build the content bytes
		var content []byte
		j := quoteStart + 1
		for j < i-1 {
			ch := src[j]
			if ch == quote {
				j++
				if j < i-1 && src[j] == quote {
					content = append(content, quote)
					j++
				}
			} else if ch == '\\' && !noBackslashEscapes && j+1 < i-1 {
				j++
				content = append(content, util.UnescapeChar(src[j])...)
				j++
			} else {
				content = append(content, ch)
				j++
			}
		}

		// Lazy-allocate output buffer on first binary string found
		if buf == nil {
			buf = &bytes.Buffer{}
			buf.Grow(len(src))
		}

		// Flush everything from lastCopied to quoteStart through Transform
		flushTo(buf, &tmp, src[lastCopied:quoteStart], enc)

		// Strip _<charset> prefix if present
		stripCharsetIntroducer(buf)

		buf.WriteString("0x")
		for _, b := range content {
			buf.WriteByte(hexDigits[b>>4])
			buf.WriteByte(hexDigits[b&0xf])
		}
		lastCopied = i
	}

	// No binary strings found — transform the whole thing in one call
	if buf == nil {
		utf8Lit, _ := enc.Transform(nil, src, charset.OpDecodeReplace)
		return charset.HackString(utf8Lit)
	}

	// Flush remaining text
	if lastCopied < len(src) {
		flushTo(buf, &tmp, src[lastCopied:], enc)
	}
	return buf.String()
}

// flushTo transforms src through the encoding and appends to buf
// tmp is a reusable scratch buffer (Transform calls Reset on it)
func flushTo(buf, tmp *bytes.Buffer, src []byte, enc charset.Encoding) {
	if len(src) == 0 {
		return
	}
	utf8Lit, _ := enc.Transform(tmp, src, charset.OpDecodeReplace)
	buf.Write(utf8Lit)
}

// stripCharsetIntroducer removes a trailing _<charset> introducer
// (e.g. "_binary ", "_utf8mb4 ") from the buffer if present
func stripCharsetIntroducer(buf *bytes.Buffer) {
	b := buf.Bytes()
	end := len(b)
	// Trim trailing whitespace between introducer and the quote
	for end > 0 && unicode.IsSpace(rune(b[end-1])) {
		end--
	}
	if end == 0 {
		return
	}
	// Scan backwards past alphanumeric characters (potential charset name)
	nameEnd := end
	for end > 0 && (b[end-1] >= 'a' && b[end-1] <= 'z' || b[end-1] >= 'A' && b[end-1] <= 'Z' || b[end-1] >= '0' && b[end-1] <= '9') {
		end--
	}
	if end == 0 || b[end-1] != '_' {
		return
	}
	// The underscore must be a token boundary, not part of an identifier like col_binary
	if end >= 2 {
		prev := b[end-2]
		if prev >= 'a' && prev <= 'z' || prev >= 'A' && prev <= 'Z' || prev >= '0' && prev <= '9' || prev == '_' {
			return
		}
	}
	// Validate that this is a known charset name
	name := string(b[end:nameEnd])
	if _, err := charset.GetCharsetInfo(name); err != nil {
		return
	}
	buf.Truncate(end - 1)
}

// stmtNode implements StmtNode interface.
// Statement implementations should embed it in.
type stmtNode struct {
	node
}

// statement implements StmtNode interface.
func (sn *stmtNode) statement() {}

// ddlNode implements DDLNode interface.
// DDL implementations should embed it in.
type ddlNode struct {
	stmtNode
}

// ddlStatement implements DDLNode interface.
func (dn *ddlNode) ddlStatement() {}

// dmlNode is the struct implements DMLNode interface.
// DML implementations should embed it in.
type dmlNode struct {
	stmtNode
}

// dmlStatement implements DMLNode interface.
func (dn *dmlNode) dmlStatement() {}

// exprNode is the struct implements Expression interface.
// Expression implementations should embed it in.
type exprNode struct {
	node
	Type types.FieldType
	flag uint64
}

// TexprNode is exported for parser driver.
type TexprNode = exprNode

// SetType implements ExprNode interface.
func (en *exprNode) SetType(tp *types.FieldType) {
	en.Type = *tp
}

// GetType implements ExprNode interface.
func (en *exprNode) GetType() *types.FieldType {
	return &en.Type
}

// SetFlag implements ExprNode interface.
func (en *exprNode) SetFlag(flag uint64) {
	en.flag = flag
}

// GetFlag implements ExprNode interface.
func (en *exprNode) GetFlag() uint64 {
	return en.flag
}

type funcNode struct {
	exprNode
}

// functionExpression implements FunctionNode interface.
func (fn *funcNode) functionExpression() {}
