// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"

	"github.com/pingcap/tidb/pkg/util/hack"
)

// jsonTextValue describes an already encoded value in the parser's shared buffer.
// Semantic errors are deferred because a later duplicate key may discard them.
type jsonTextValue struct {
	start, end int
	depth      uint16
	typeCode   byte
	literal    byte
	err        error
}

type jsonTextField struct {
	key   string
	value jsonTextValue
}

type binaryJSONTextParser struct {
	input   bytes.Buffer
	decoder jsontext.Decoder
	buf     []byte
	text    []byte
	entries []byte
	fields  []jsonTextField
}

var binaryJSONTextParsers = sync.Pool{New: func() any { return new(binaryJSONTextParser) }}

// parseBinaryJSON consumes one complete document and writes BinaryJSON without an any tree.
func parseBinaryJSON(data []byte) (BinaryJSON, error) {
	p := binaryJSONTextParsers.Get().(*binaryJSONTextParser)
	p.input = *bytes.NewBuffer(data)
	p.decoder.Reset(&p.input, jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true))
	defer func() {
		p.input = bytes.Buffer{}
		p.decoder.Reset(&p.input)
		p.buf, p.text, p.entries = p.buf[:0], p.text[:0], p.entries[:0]
		clear(p.fields[:cap(p.fields)])
		p.fields = p.fields[:0]
		if p.reusable(len(data)) {
			binaryJSONTextParsers.Put(p)
		}
	}()
	v, err := p.readValue()
	if err != nil {
		return BinaryJSON{}, err
	}
	if _, err := p.decoder.ReadToken(); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("the document root must not be followed by other values")
		}
		return BinaryJSON{}, err
	}
	if v.err != nil {
		return BinaryJSON{}, v.err
	}
	if v.depth-1 > maxJSONDepth {
		return BinaryJSON{}, ErrJSONDocumentTooDeep
	}
	if v.typeCode == JSONTypeCodeLiteral {
		p.buf = append(p.buf, v.literal)
	}
	// Transfer large unpooled buffers, but do not retain a large overwritten value.
	result := p.buf
	if p.reusable(len(data)) || len(result) < cap(result)/2 {
		result = bytes.Clone(result)
	}
	return BinaryJSON{TypeCode: v.typeCode, Value: result}, nil
}

func (p *binaryJSONTextParser) reusable(inputLen int) bool {
	return inputLen <= 256*1024 && cap(p.buf)+cap(p.text)+cap(p.entries) <= 256*1024 && cap(p.fields) <= 1024
}

func (p *binaryJSONTextParser) readValue() (v jsonTextValue, err error) {
	v.start, v.depth = len(p.buf), 1
	switch p.decoder.PeekKind() {
	case '"':
		var raw jsontext.Value
		raw, err = p.decoder.ReadValue()
		if err == nil {
			// ReadValue checked the grammar; AppendUnquote also replaces invalid UTF-8.
			p.text, _ = jsontext.AppendUnquote(p.text[:0], raw)
			p.buf = binary.AppendUvarint(p.buf, uint64(len(p.text)))
			p.buf = append(p.buf, p.text...)
			v.typeCode = JSONTypeCodeString
		}
	case '0':
		var raw jsontext.Value
		raw, err = p.decoder.ReadValue()
		if err == nil {
			var buf []byte
			v.typeCode, buf, v.err = appendBinaryNumber(p.buf, json.Number(hack.String(raw)))
			if v.err == nil {
				p.buf = buf
			}
		}
	default:
		var tok jsontext.Token
		tok, err = p.decoder.ReadToken()
		if err != nil {
			return v, err
		}
		switch tok.Kind() {
		case 'n', 't', 'f':
			v.typeCode, v.literal = JSONTypeCodeLiteral, JSONLiteralNil
			if tok.Kind() == 't' {
				v.literal = JSONLiteralTrue
			} else if tok.Kind() == 'f' {
				v.literal = JSONLiteralFalse
			}
		case '[':
			return p.readArray()
		case '{':
			return p.readObject()
		default:
			return v, fmt.Errorf("unexpected JSON token %v", tok.Kind())
		}
	}
	v.end = len(p.buf)
	return v, err
}

func (p *binaryJSONTextParser) readArray() (v jsonTextValue, err error) {
	v = jsonTextValue{start: len(p.buf), depth: 1, typeCode: JSONTypeCodeArray}
	mark := len(p.entries)
	defer func() { p.entries = p.entries[:mark] }()
	for p.decoder.PeekKind() != ']' {
		child, err := p.readValue()
		if err != nil {
			return v, err
		}
		v.depth = min(maxJSONDepth+2, max(v.depth, child.depth+1))
		if v.err == nil {
			v.err = child.err
		}
		entry := len(p.entries)
		if cap(p.entries)-entry < valEntrySize {
			p.entries = slices.Grow(p.entries, max(cap(p.entries), valEntrySize))
		}
		p.entries = appendZero(p.entries, valEntrySize)
		p.entries[entry] = child.typeCode
		if child.typeCode == JSONTypeCodeLiteral {
			p.entries[entry+1] = child.literal
		} else {
			jsonEndian.PutUint32(p.entries[entry+1:], uint32(child.start-v.start))
		}
	}
	if _, err = p.decoder.ReadToken(); err != nil {
		return v, err
	}
	if v.err != nil || v.depth > maxJSONDepth+1 {
		p.buf = p.buf[:v.start]
		v.end = v.start
		return v, nil
	}
	entries := p.entries[mark:]
	prefix := headerSize + len(entries)
	end := len(p.buf)
	p.buf = append(p.buf, make([]byte, prefix)...)
	copy(p.buf[v.start+prefix:], p.buf[v.start:end])
	header := p.buf[v.start:]
	jsonEndian.PutUint32(header, uint32(len(entries)/valEntrySize))
	jsonEndian.PutUint32(header[dataSizeOff:], uint32(len(header)))
	copy(header[headerSize:], entries)
	for i := headerSize; i < prefix; i += valEntrySize {
		if header[i] != JSONTypeCodeLiteral {
			off := jsonEndian.Uint32(header[i+1:])
			jsonEndian.PutUint32(header[i+1:], off+uint32(prefix))
		}
	}
	v.end = len(p.buf)
	return v, nil
}

func (p *binaryJSONTextParser) readObject() (v jsonTextValue, err error) {
	v = jsonTextValue{start: len(p.buf), depth: 1, typeCode: JSONTypeCodeObject}
	mark := len(p.fields)
	defer func() {
		clear(p.fields[mark:])
		p.fields = p.fields[:mark]
	}()
	index := make(map[string]int)
	dead := 0
	for p.decoder.PeekKind() != '}' {
		key, err := p.decoder.ReadToken()
		if err != nil {
			return v, err
		}
		name := key.String()
		i, exists := index[name]
		if !exists {
			i = len(p.fields)
			index[name] = i
			if len(p.fields) == cap(p.fields) {
				p.fields = slices.Grow(p.fields, max(cap(p.fields), 8))
			}
			p.fields = append(p.fields, jsonTextField{key: name})
		} else {
			old := p.fields[i].value
			if old.end == len(p.buf) {
				p.buf = p.buf[:old.start]
			} else {
				dead += old.end - old.start
			}
		}
		child, err := p.readValue()
		if err != nil {
			return v, err
		}
		p.fields[i].value = child
		// Compact only when garbage exceeds live data, bounding both copying and memory.
		if dead > len(p.buf)-v.start-dead {
			p.text = p.text[:0]
			for j := range p.fields[mark:] {
				child := &p.fields[mark+j].value
				begin := len(p.text)
				p.text = append(p.text, p.buf[child.start:child.end]...)
				child.start, child.end = v.start+begin, v.start+len(p.text)
			}
			p.buf = append(p.buf[:v.start], p.text...)
			dead = 0
		}
	}
	if _, err = p.decoder.ReadToken(); err != nil {
		return v, err
	}
	fields := p.fields[mark:]
	slices.SortFunc(fields, func(a, b jsonTextField) int { return cmp.Compare(a.key, b.key) })
	// Match the existing encoder's order: all keys, then values; depth is checked last.
	valueSize := headerSize + len(fields)*(keyEntrySize+valEntrySize)
	for _, f := range fields {
		if len(f.key) > math.MaxUint16 {
			v.err = ErrJSONObjectKeyTooLong
			break
		}
		valueSize += len(f.key)
	}
	for _, f := range fields {
		if f.value.typeCode != JSONTypeCodeLiteral {
			valueSize += f.value.end - f.value.start
		}
		v.depth = min(maxJSONDepth+2, max(v.depth, f.value.depth+1))
		if v.err == nil {
			v.err = f.value.err
		}
	}
	if v.err != nil || v.depth > maxJSONDepth+1 {
		p.buf = p.buf[:v.start]
		v.end = v.start
		return v, nil
	}
	// Reuse scratch instead of growing the shared arena to hold two object copies.
	p.text = slices.Grow(p.text[:0], valueSize)
	p.text = append(p.text, make([]byte, headerSize+len(fields)*(keyEntrySize+valEntrySize))...)
	jsonEndian.PutUint32(p.text, uint32(len(fields)))
	for i, f := range fields {
		entry := headerSize + i*keyEntrySize
		jsonEndian.PutUint32(p.text[entry:], uint32(len(p.text)))
		jsonEndian.PutUint16(p.text[entry+keyLenOff:], uint16(len(f.key)))
		p.text = append(p.text, f.key...)
	}
	for i, f := range fields {
		entry := headerSize + len(fields)*keyEntrySize + i*valEntrySize
		p.text[entry] = f.value.typeCode
		if f.value.typeCode == JSONTypeCodeLiteral {
			p.text[entry+1] = f.value.literal
		} else {
			jsonEndian.PutUint32(p.text[entry+1:], uint32(len(p.text)))
			p.text = append(p.text, p.buf[f.value.start:f.value.end]...)
		}
	}
	jsonEndian.PutUint32(p.text[dataSizeOff:], uint32(len(p.text)))
	p.buf = append(p.buf[:v.start], p.text...)
	v.end = len(p.buf)
	return v, nil
}
