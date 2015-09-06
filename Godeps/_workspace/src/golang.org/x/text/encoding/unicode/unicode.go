// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package unicode provides Unicode encodings such as UTF-16.
package unicode

import (
	"errors"
	"unicode/utf16"
	"unicode/utf8"

	"golang.org/x/text/encoding"
	"golang.org/x/text/transform"
)

// UTF16 returns a UTF-16 Encoding for the given default endianness and byte
// order mark (BOM) policy.
//
// When decoding from UTF-16 to UTF-8, if the BOMPolicy is IgnoreBOM then
// neither BOMs U+FEFF nor noncharacters U+FFFE in the input stream will affect
// the endianness used for decoding, and will instead be output as their
// standard UTF-8 encodings: "\xef\xbb\xbf" and "\xef\xbf\xbe". If the
// BOMPolicy is ExpectBOM then the input stream is expected to start with a
// BOM, and the transformation will return early with an ErrMissingBOM error if
// it does not. That starting BOM is not written to the UTF-8 output. Instead,
// it overrides the default endianness e for the remainder of the
// transformation. Any subsequent BOMs U+FEFF or noncharacters U+FFFE will not
// affect the endianness used, and will instead be output as their standard
// UTF-8 encodings.
//
// When encoding from UTF-8 to UTF-16, a BOM will be inserted at the start of
// the output if the BOMPolicy is ExpectBOM. Otherwise, a BOM will not be
// inserted. The UTF-8 input does not need to contain a BOM.
//
// There is no concept of a 'native' endianness. If the UTF-16 data is produced
// and consumed in a greater context that implies a certain endianness, use
// IgnoreBOM. Otherwise, use ExpectBOM and always produce and consume a BOM.
//
// In the language of http://www.unicode.org/faq/utf_bom.html#bom10, IgnoreBOM
// corresponds to "Where the precise type of the data stream is known... the
// BOM should not be used" and ExpectBOM corresponds to "A particular
// protocol... may require use of the BOM".
func UTF16(e Endianness, b BOMPolicy) encoding.Encoding {
	return utf16Encoding{e, b}
}

// BOMPolicy is a UTF-16 encoding's byte order mark policy.
type BOMPolicy bool

const (
	// IgnoreBOM means to ignore any byte order marks.
	IgnoreBOM BOMPolicy = false
	// ExpectBOM means that the UTF-16 form is expected to start with a
	// byte order mark.
	ExpectBOM BOMPolicy = true
)

// Endianness is a UTF-16 encoding's default endianness.
type Endianness bool

const (
	// BigEndian is UTF-16BE.
	BigEndian Endianness = false
	// LittleEndian is UTF-16LE.
	LittleEndian Endianness = true
)

// ErrMissingBOM means that decoding UTF-16 input with ExpectBOM did not find a
// starting byte order mark.
var ErrMissingBOM = errors.New("encoding: missing byte order mark")

type utf16Encoding struct {
	endianness Endianness
	bomPolicy  BOMPolicy
}

func (u utf16Encoding) NewDecoder() transform.Transformer {
	return &utf16Decoder{
		endianness:       u.endianness,
		initialBOMPolicy: u.bomPolicy,
		currentBOMPolicy: u.bomPolicy,
	}
}

func (u utf16Encoding) NewEncoder() transform.Transformer {
	return &utf16Encoder{
		endianness:       u.endianness,
		initialBOMPolicy: u.bomPolicy,
		currentBOMPolicy: u.bomPolicy,
	}
}

func (u utf16Encoding) String() string {
	e, b := "B", "Ignore"
	if u.endianness == LittleEndian {
		e = "L"
	}
	if u.bomPolicy == ExpectBOM {
		b = "Expect"
	}
	return "UTF-16" + e + "E (" + b + " BOM)"
}

type utf16Decoder struct {
	endianness       Endianness
	initialBOMPolicy BOMPolicy
	currentBOMPolicy BOMPolicy
}

func (u *utf16Decoder) Reset() {
	u.currentBOMPolicy = u.initialBOMPolicy
}

func (u *utf16Decoder) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error) {
	if u.currentBOMPolicy == ExpectBOM {
		if len(src) < 2 {
			return 0, 0, transform.ErrShortSrc
		}
		switch {
		case src[0] == 0xfe && src[1] == 0xff:
			u.endianness = BigEndian
		case src[0] == 0xff && src[1] == 0xfe:
			u.endianness = LittleEndian
		default:
			return 0, 0, ErrMissingBOM
		}
		u.currentBOMPolicy = IgnoreBOM
		nSrc = 2
	}

	for nSrc+1 < len(src) {
		x := uint16(src[nSrc+0])<<8 | uint16(src[nSrc+1])
		if u.endianness == LittleEndian {
			x = x>>8 | x<<8
		}
		r, sSize := rune(x), 2
		if utf16.IsSurrogate(r) {
			if nSrc+3 >= len(src) {
				break
			}
			x = uint16(src[nSrc+2])<<8 | uint16(src[nSrc+3])
			if u.endianness == LittleEndian {
				x = x>>8 | x<<8
			}
			r, sSize = utf16.DecodeRune(r, rune(x)), 4
		}
		dSize := utf8.RuneLen(r)
		if dSize < 0 {
			r, dSize = utf8.RuneError, 3
		}
		if nDst+dSize > len(dst) {
			err = transform.ErrShortDst
			break
		}
		nDst += utf8.EncodeRune(dst[nDst:], r)
		nSrc += sSize
	}

	if err == nil && nSrc != len(src) {
		err = transform.ErrShortSrc
	}
	return nDst, nSrc, err
}

type utf16Encoder struct {
	endianness       Endianness
	initialBOMPolicy BOMPolicy
	currentBOMPolicy BOMPolicy
}

func (u *utf16Encoder) Reset() {
	u.currentBOMPolicy = u.initialBOMPolicy
}

func (u *utf16Encoder) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error) {
	if u.currentBOMPolicy == ExpectBOM {
		if len(dst) < 2 {
			return 0, 0, transform.ErrShortDst
		}
		dst[0], dst[1] = 0xfe, 0xff
		u.currentBOMPolicy = IgnoreBOM
		nDst = 2
	}

	r, size := rune(0), 0
	for nSrc < len(src) {
		r = rune(src[nSrc])

		// Decode a 1-byte rune.
		if r < utf8.RuneSelf {
			size = 1

		} else {
			// Decode a multi-byte rune.
			r, size = utf8.DecodeRune(src[nSrc:])
			if size == 1 {
				// All valid runes of size 1 (those below utf8.RuneSelf) were
				// handled above. We have invalid UTF-8 or we haven't seen the
				// full character yet.
				if !atEOF && !utf8.FullRune(src[nSrc:]) {
					err = transform.ErrShortSrc
					break
				}
			}
		}

		if r <= 0xffff {
			if nDst+2 > len(dst) {
				err = transform.ErrShortDst
				break
			}
			dst[nDst+0] = uint8(r >> 8)
			dst[nDst+1] = uint8(r)
			nDst += 2
		} else {
			if nDst+4 > len(dst) {
				err = transform.ErrShortDst
				break
			}
			r1, r2 := utf16.EncodeRune(r)
			dst[nDst+0] = uint8(r1 >> 8)
			dst[nDst+1] = uint8(r1)
			dst[nDst+2] = uint8(r2 >> 8)
			dst[nDst+3] = uint8(r2)
			nDst += 4
		}
		nSrc += size
	}

	if u.endianness == LittleEndian {
		for i := 0; i < nDst; i += 2 {
			dst[i], dst[i+1] = dst[i+1], dst[i]
		}
	}
	return nDst, nSrc, err
}
