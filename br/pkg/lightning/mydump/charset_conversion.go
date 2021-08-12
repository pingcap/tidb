// Copyright 2021 PingCAP, Inc.
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

package mydump

import (
	"bytes"
	"fmt"
	"unicode/utf8"

	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

type Charset int

const (
	BINARY Charset = iota
	UTF8MB4
	GB18030
	GBK
)

func (c Charset) String() string {
	switch c {
	case BINARY:
		return "binary"
	case UTF8MB4:
		return "utf8mb4"
	case GB18030:
		return "gb18030"
	case GBK:
		return "gbk"
	default:
		return "unknown_charset"
	}
}

// CharsetConvertor is used to convert a character set to another.
// In Lightning, we mainly use it to do the GB18030/GBK -> UTF8MB4 conversion.
type CharsetConvertor struct {
	// sourceCharacterSet represents the charset that the source file uses.
	sourceCharacterSet Charset
	// invalidCharReplacement is the default replacement character for the invalid content, e.g "\ufffd".
	invalidCharReplacement rune

	originalReader ReadSeekCloser
}

// NewCharsetConvertor creates a new CharsetConvertor.
func NewCharsetConvertor(sourceCharacterSet Charset, invalidCharReplacement rune, reader ReadSeekCloser) ReadSeekCloser {
	// No need to convert the charset encoding, just return the original reader.
	if sourceCharacterSet == BINARY || sourceCharacterSet == UTF8MB4 {
		return reader
	}

	return &CharsetConvertor{
		sourceCharacterSet:     sourceCharacterSet,
		invalidCharReplacement: invalidCharReplacement,
		originalReader:         reader,
	}
}

func (cc *CharsetConvertor) Read(p []byte) (n int, err error) {
	// Read from the original reader first.
	originalData := make([]byte, len(p))
	n, err = cc.originalReader.Read(originalData)
	if err != nil {
		return n, err
	}
	// Create the convertor reader first.
	var transformer *transform.Reader
	transformer, err = cc.buildTransformer(originalData)
	if err != nil {
		return 0, err
	}
	// Do the conversion then.
	n, err = transformer.Read(p)
	if err != nil {
		return n, err
	}
	cc.replaceInvalidChar(p)
	return n, nil
}

func (cc *CharsetConvertor) buildTransformer(data []byte) (*transform.Reader, error) {
	switch cc.sourceCharacterSet {
	case GB18030:
		return transform.NewReader(bytes.NewReader(data), simplifiedchinese.GB18030.NewDecoder()), nil
	case GBK:
		return transform.NewReader(bytes.NewReader(data), simplifiedchinese.GBK.NewDecoder()), nil
	default:
		return nil, fmt.Errorf("not support %s as the conversion source yet", cc.sourceCharacterSet)
	}
}

func (cc *CharsetConvertor) replaceInvalidChar(data []byte) {
	offset := 0
	for offset < len(data) {
		r, size := utf8.DecodeRune(data[offset:])
		if r == utf8.RuneError {
			copy(data[offset:], []byte(string(cc.invalidCharReplacement)))
		}
		offset += size
	}
}

func (cc *CharsetConvertor) Seek(offset int64, whence int) (int64, error) {
	return cc.originalReader.Seek(offset, whence)
}

func (cc *CharsetConvertor) Close() error { return cc.originalReader.Close() }
