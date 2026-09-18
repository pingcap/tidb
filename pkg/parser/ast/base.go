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
	if n.noBackslashEscapes == val {
		return
	}
	n.noBackslashEscapes = val
	if n.once != nil {
		n.once = &sync.Once{}
	}
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
// single- or double-quoted string literals to 0x hex literals and decoding
// everything else to UTF-8.
//
// The function first transforms the entire text to UTF-8 and then scans the
// UTF-8 result for string literal boundaries. This avoids ambiguity in
// encodings like GBK/GB18030 where ASCII-range bytes (e.g. 0x5C backslash)
// can appear as trail bytes of multibyte characters — UTF-8 never reuses
// ASCII byte values in multibyte sequences, so quote and backslash detection
// is always correct.
//
// A parallel index into the original byte sequence is maintained so that
// non-printable strings can be hex-encoded from their original bytes.
// Quote bytes (0x22, 0x27) are below the trail-byte range of all supported
// multibyte encodings, so finding them in the original text is always safe.
func convertBinaryStringLiterals(text string, enc charset.Encoding, noBackslashEscapes bool) string {
	src := charset.HackSlice(text)

	// Fast path: if no quotes, just transform the whole thing.
	if bytes.IndexByte(src, '\'') < 0 && bytes.IndexByte(src, '"') < 0 {
		result, _ := enc.Transform(nil, src, charset.OpDecodeReplace)
		return charset.HackString(result)
	}

	// Transform entire text to UTF-8 for safe scanning.
	utf8Text, _ := enc.Transform(nil, src, charset.OpDecodeReplace)

	var buf *bytes.Buffer
	lastCopiedIdx := 0 // tracks position in utf8Text for output assembly
	origIdx := 0       // tracks position in src (original bytes)
	i := 0             // scan position in utf8Text

	for i < len(utf8Text) {
		if utf8Text[i] != '\'' && utf8Text[i] != '"' {
			i++
			continue
		}

		utf8QuoteStart := i
		quote := utf8Text[i]
		i++

		// Find the corresponding opening quote in the original text.
		origQuoteStart := advanceOrigTo(src, &origIdx, quote)
		if origQuoteStart < 0 {
			break
		}

		// Find closing quote in UTF-8 text, keeping origIdx in sync.
		terminated := false
		var origQuoteEnd int
		for i < len(utf8Text) {
			ch := utf8Text[i]
			if ch == quote {
				i++
				origClose := advanceOrigTo(src, &origIdx, quote)
				if origClose < 0 {
					break
				}
				if i >= len(utf8Text) || utf8Text[i] != quote {
					// Closing quote.
					terminated = true
					origQuoteEnd = origClose + 1
					break
				}
				// Doubled quote escape — advance past second quote in original.
				i++
				if advanceOrigTo(src, &origIdx, quote) < 0 {
					break
				}
			} else if ch == '\\' && !noBackslashEscapes && i+1 < len(utf8Text) {
				nextCh := utf8Text[i+1]
				i += 2
				// If the escaped character is a quote byte, advance origIdx
				// past the corresponding quote in the original text so the
				// pairing stays in sync.
				if nextCh == '\'' || nextCh == '"' {
					if advanceOrigTo(src, &origIdx, nextCh) < 0 {
						break
					}
				}
			} else {
				i++
			}
		}

		if !terminated {
			continue
		}

		// Check printability by decoding the original string content.
		// OpDecode returns an error if the bytes are invalid in the source encoding;
		// isPrintable then rejects control characters in the decoded UTF-8.
		decoded, err := enc.Transform(nil, src[origQuoteStart+1:origQuoteEnd-1], charset.OpDecode)
		if err == nil && isPrintable(decoded) {
			continue
		}

		// Non-printable: extract content from original bytes and hex-encode.
		var content []byte
		j := origQuoteStart + 1
		origEnd := origQuoteEnd - 1
		for j < origEnd {
			ch := src[j]
			if ch == quote {
				j++
				if j < origEnd && src[j] == quote {
					content = append(content, quote)
					j++
				}
			} else if ch == '\\' && !noBackslashEscapes && j+1 < origEnd {
				j++
				content = append(content, util.UnescapeChar(src[j])...)
				j++
			} else {
				content = append(content, ch)
				j++
			}
		}

		// Lazy-allocate output buffer on first binary string found.
		if buf == nil {
			buf = &bytes.Buffer{}
			buf.Grow(len(utf8Text))
		}

		buf.Write(utf8Text[lastCopiedIdx:utf8QuoteStart])
		buf.WriteString("0x")
		for _, b := range content {
			buf.WriteByte(hexDigits[b>>4])
			buf.WriteByte(hexDigits[b&0xf])
		}
		lastCopiedIdx = i
	}

	if buf == nil {
		return charset.HackString(utf8Text)
	}

	if lastCopiedIdx < len(utf8Text) {
		buf.Write(utf8Text[lastCopiedIdx:])
	}
	return buf.String()
}

// advanceOrigTo scans src from *idx forward until it finds a byte equal to b.
// It returns the position of that byte and advances *idx past it, or returns -1
// if not found.
func advanceOrigTo(src []byte, idx *int, b byte) int {
	for *idx < len(src) {
		if src[*idx] == b {
			pos := *idx
			*idx++
			return pos
		}
		*idx++
	}
	return -1
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
