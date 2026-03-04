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
	utf8Text string
	enc      charset.Encoding
	once     *sync.Once

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
		n.utf8Text = convertBinaryStringLiterals(n.text, n.enc)
	})
	return n.utf8Text
}

// OriginalText implements Node interface.
func (n *node) OriginalText() string {
	return n.text
}

// isPrintable checks if a string contains only valid, printable UTF-8.
func isPrintable(s []byte) bool {
	for len(s) > 0 {
		r, size := utf8.DecodeRune(s)
		if r == utf8.RuneError && size <= 1 {
			return false
		}
		if unicode.IsControl(r) {
			return false
		}
		s = s[size:]
	}
	return true
}

// convertBinaryStringLiterals processes raw SQL text, converting non-printable
// single- or double-quoted string literals to 0x hex literals and applying
// OpDecodeHexReplace to everything else.
func convertBinaryStringLiterals(text string, enc charset.Encoding) string {
	src := charset.HackSlice(text)

	// Fast path: if no quotes, just transform the whole thing
	if bytes.IndexByte(src, '\'') < 0 && bytes.IndexByte(src, '"') < 0 {
		result, _ := enc.Transform(nil, src, charset.OpDecodeHexReplace)
		return charset.HackString(result)
	}

	var buf bytes.Buffer
	var tmp bytes.Buffer
	buf.Grow(len(src))
	i := 0

	for i < len(src) {
		if src[i] != '\'' && src[i] != '"' {
			i++
			continue
		}

		// Flush non-string text before this quote through the encoding transform
		flushTo(&buf, &tmp, src[:i], enc)
		src = src[i:]
		i = 0

		quote := src[0]
		i = 1

		// Collect unescaped content bytes and find closing quote
		var content []byte
		terminated := false
		for i < len(src) {
			ch := src[i]
			if ch == quote {
				i++
				if i < len(src) && src[i] == quote {
					content = append(content, quote)
					i++
				} else {
					terminated = true
					break
				}
			} else if ch == '\\' && i+1 < len(src) {
				i++
				content = append(content, util.UnescapeChar(src[i])...)
				i++
			} else {
				content = append(content, ch)
				i++
			}
		}

		if !terminated || isPrintable(content) {
			// Not a binary string — transform the raw literal through encoding
			flushTo(&buf, &tmp, src[:i], enc)
			src = src[i:]
			i = 0
			continue
		}

		// Binary string literal — check for _<charset> prefix in buf and strip it
		stripCharsetIntroducer(&buf)

		buf.WriteString("0x")
		for _, b := range content {
			buf.WriteByte(hexDigits[b>>4])
			buf.WriteByte(hexDigits[b&0xf])
		}
		src = src[i:]
		i = 0
	}

	// Flush any remaining text
	if len(src) > 0 {
		flushTo(&buf, &tmp, src, enc)
	}
	return buf.String()
}

// flushTo transforms src through the encoding and appends to buf
// tmp is a reusable scratch buffer (Transform calls Reset on it)
func flushTo(buf, tmp *bytes.Buffer, src []byte, enc charset.Encoding) {
	if len(src) == 0 {
		return
	}
	result, _ := enc.Transform(tmp, src, charset.OpDecodeHexReplace)
	buf.Write(result)
}

// stripCharsetIntroducer removes a trailing _<charset> introducer
// (e.g. "_binary ", "_utf8mb4 ") from the buffer if present
func stripCharsetIntroducer(buf *bytes.Buffer) {
	b := buf.Bytes()
	end := len(b)
	// Trim trailing spaces between introducer and the quote
	for end > 0 && b[end-1] == ' ' {
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
