// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package korean

import (
	"errors"
	"unicode/utf8"

	"golang.org/x/text/encoding"
	"golang.org/x/text/transform"
)

// EUCKR is the EUC-KR encoding, also known as Code Page 949.
var EUCKR encoding.Encoding = eucKR{}

type eucKR struct{}

func (eucKR) NewDecoder() transform.Transformer {
	return eucKRDecoder{}
}

func (eucKR) NewEncoder() transform.Transformer {
	return eucKREncoder{}
}

func (eucKR) String() string {
	return "EUC-KR"
}

var errInvalidEUCKR = errors.New("korean: invalid EUC-KR encoding")

type eucKRDecoder struct{ transform.NopResetter }

func (eucKRDecoder) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error) {
	r, size := rune(0), 0
loop:
	for ; nSrc < len(src); nSrc += size {
		switch c0 := src[nSrc]; {
		case c0 < utf8.RuneSelf:
			r, size = rune(c0), 1

		case 0x81 <= c0 && c0 < 0xff:
			if nSrc+1 >= len(src) {
				err = transform.ErrShortSrc
				break loop
			}
			c1 := src[nSrc+1]
			if c0 < 0xc7 {
				r = 178 * rune(c0-0x81)
				switch {
				case 0x41 <= c1 && c1 < 0x5b:
					r += rune(c1) - (0x41 - 0*26)
				case 0x61 <= c1 && c1 < 0x7b:
					r += rune(c1) - (0x61 - 1*26)
				case 0x81 <= c1 && c1 < 0xff:
					r += rune(c1) - (0x81 - 2*26)
				default:
					err = errInvalidEUCKR
					break loop
				}
			} else if 0xa1 <= c1 && c1 < 0xff {
				r = 178*(0xc7-0x81) + rune(c0-0xc7)*94 + rune(c1-0xa1)
			} else {
				err = errInvalidEUCKR
				break loop
			}
			if int(r) < len(decode) {
				r = rune(decode[r])
				if r == 0 {
					r = '\ufffd'
				}
			} else {
				r = '\ufffd'
			}
			size = 2

		default:
			err = errInvalidEUCKR
			break loop
		}

		if nDst+utf8.RuneLen(r) > len(dst) {
			err = transform.ErrShortDst
			break loop
		}
		nDst += utf8.EncodeRune(dst[nDst:], r)
	}
	if atEOF && err == transform.ErrShortSrc {
		err = errInvalidEUCKR
	}
	return nDst, nSrc, err
}

type eucKREncoder struct{ transform.NopResetter }

func (eucKREncoder) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error) {
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
					goto write2
				}
			case encode1Low <= r && r < encode1High:
				if r = rune(encode1[r-encode1Low]); r != 0 {
					goto write2
				}
			case encode2Low <= r && r < encode2High:
				if r = rune(encode2[r-encode2Low]); r != 0 {
					goto write2
				}
			case encode3Low <= r && r < encode3High:
				if r = rune(encode3[r-encode3Low]); r != 0 {
					goto write2
				}
			case encode4Low <= r && r < encode4High:
				if r = rune(encode4[r-encode4Low]); r != 0 {
					goto write2
				}
			case encode5Low <= r && r < encode5High:
				if r = rune(encode5[r-encode5Low]); r != 0 {
					goto write2
				}
			case encode6Low <= r && r < encode6High:
				if r = rune(encode6[r-encode6Low]); r != 0 {
					goto write2
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

	write2:
		if nDst+2 > len(dst) {
			err = transform.ErrShortDst
			break
		}
		dst[nDst+0] = uint8(r >> 8)
		dst[nDst+1] = uint8(r)
		nDst += 2
		continue
	}
	return nDst, nSrc, err
}

func init() {
	// Check that the hard-coded encode switch covers all tables.
	if numEncodeTables != 7 {
		panic("bad numEncodeTables")
	}
}
