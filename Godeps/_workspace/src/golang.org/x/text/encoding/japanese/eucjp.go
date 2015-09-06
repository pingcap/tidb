// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package japanese

import (
	"errors"
	"unicode/utf8"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/internal"
	"golang.org/x/text/encoding/internal/identifier"
	"golang.org/x/text/transform"
)

// EUCJP is the EUC-JP encoding.
var EUCJP encoding.Encoding = &eucJP

var eucJP = internal.Encoding{
	&internal.SimpleEncoding{eucJPDecoder{}, eucJPEncoder{}},
	"EUC-JP",
	identifier.EUCPkdFmtJapanese,
}

var errInvalidEUCJP = errors.New("japanese: invalid EUC-JP encoding")

type eucJPDecoder struct{ transform.NopResetter }

func (eucJPDecoder) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error) {
	r, size := rune(0), 0
loop:
	for ; nSrc < len(src); nSrc += size {
		switch c0 := src[nSrc]; {
		case c0 < utf8.RuneSelf:
			r, size = rune(c0), 1

		case c0 == 0x8e:
			if nSrc+1 >= len(src) {
				err = transform.ErrShortSrc
				break loop
			}
			c1 := src[nSrc+1]
			if c1 < 0xa1 || 0xdf < c1 {
				err = errInvalidEUCJP
				break loop
			}
			r, size = rune(c1)+(0xff61-0xa1), 2

		case c0 == 0x8f:
			if nSrc+2 >= len(src) {
				err = transform.ErrShortSrc
				break loop
			}
			c1 := src[nSrc+1]
			if c1 < 0xa1 || 0xfe < c1 {
				err = errInvalidEUCJP
				break loop
			}
			c2 := src[nSrc+2]
			if c2 < 0xa1 || 0xfe < c2 {
				err = errInvalidEUCJP
				break loop
			}
			r, size = '\ufffd', 3
			if i := int(c1-0xa1)*94 + int(c2-0xa1); i < len(jis0212Decode) {
				r = rune(jis0212Decode[i])
				if r == 0 {
					r = '\ufffd'
				}
			}

		case 0xa1 <= c0 && c0 <= 0xfe:
			if nSrc+1 >= len(src) {
				err = transform.ErrShortSrc
				break loop
			}
			c1 := src[nSrc+1]
			if c1 < 0xa1 || 0xfe < c1 {
				err = errInvalidEUCJP
				break loop
			}
			r, size = '\ufffd', 2
			if i := int(c0-0xa1)*94 + int(c1-0xa1); i < len(jis0208Decode) {
				r = rune(jis0208Decode[i])
				if r == 0 {
					r = '\ufffd'
				}
			}

		default:
			err = errInvalidEUCJP
			break loop
		}

		if nDst+utf8.RuneLen(r) > len(dst) {
			err = transform.ErrShortDst
			break loop
		}
		nDst += utf8.EncodeRune(dst[nDst:], r)
	}
	if atEOF && err == transform.ErrShortSrc {
		err = errInvalidEUCJP
	}
	return nDst, nSrc, err
}

type eucJPEncoder struct{ transform.NopResetter }

func (eucJPEncoder) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error) {
	r, size := rune(0), 0
	for ; nSrc < len(src); nSrc += size {
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

			// func init checks that the switch covers all tables.
			switch {
			case encode0Low <= r && r < encode0High:
				if r = rune(encode0[r-encode0Low]); r != 0 {
					goto write2or3
				}
			case encode1Low <= r && r < encode1High:
				if r = rune(encode1[r-encode1Low]); r != 0 {
					goto write2or3
				}
			case encode2Low <= r && r < encode2High:
				if r = rune(encode2[r-encode2Low]); r != 0 {
					goto write2or3
				}
			case encode3Low <= r && r < encode3High:
				if r = rune(encode3[r-encode3Low]); r != 0 {
					goto write2or3
				}
			case encode4Low <= r && r < encode4High:
				if r = rune(encode4[r-encode4Low]); r != 0 {
					goto write2or3
				}
			case encode5Low <= r && r < encode5High:
				if 0xff61 <= r && r < 0xffa0 {
					goto write2
				}
				if r = rune(encode5[r-encode5Low]); r != 0 {
					goto write2or3
				}
			}
			r = encoding.ASCIISub
		}

		if nDst >= len(dst) {
			err = transform.ErrShortDst
			break
		}
		dst[nDst] = uint8(r)
		nDst++
		continue

	write2or3:
		if r>>tableShift == jis0208 {
			if nDst+2 > len(dst) {
				err = transform.ErrShortDst
				break
			}
		} else {
			if nDst+3 > len(dst) {
				err = transform.ErrShortDst
				break
			}
			dst[nDst] = 0x8f
			nDst++
		}
		dst[nDst+0] = 0xa1 + uint8(r>>codeShift)&codeMask
		dst[nDst+1] = 0xa1 + uint8(r)&codeMask
		nDst += 2
		continue

	write2:
		if nDst+2 > len(dst) {
			err = transform.ErrShortDst
			break
		}
		dst[nDst+0] = 0x8e
		dst[nDst+1] = uint8(r - (0xff61 - 0xa1))
		nDst += 2
		continue
	}
	return nDst, nSrc, err
}

func init() {
	// Check that the hard-coded encode switch covers all tables.
	if numEncodeTables != 6 {
		panic("bad numEncodeTables")
	}
}
