// Copyright 2023-2024 PingCAP, Inc.
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

package charset

import (
	"encoding/binary"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

// Current implementation meets the requirement of GB18030-2022

// EncodingGB18030Impl is the instance of encodingGB18030
var EncodingGB18030Impl = &encodingGB18030{encodingGBK{encodingBase{enc: customGB18030{}}}}

func init() {
	EncodingGB18030Impl.self = EncodingGB18030Impl
}

// encodingGB18030 is GB18030 encoding.
type encodingGB18030 struct {
	encodingGBK
}

// Name implements Encoding interface.
func (*encodingGB18030) Name() string {
	return CharsetGB18030
}

// Tp implements Encoding interface.
func (*encodingGB18030) Tp() EncodingTp {
	return EncodingTpGB18030
}

// Peek implements Encoding interface.
func (*encodingGB18030) Peek(src []byte) []byte {
	return peek(src)
}

func peek(src []byte) []byte {
	length := len(src)
	if length == 0 {
		return src
	}

	// skip the first byte when encountering invalid byte(s)
	err := src[:1]
	switch {
	case src[0] == 0x80 || src[0] == 0xFF:
		return err
	case src[0] <= 0x7F:
		return src[:1]
	case 0x81 <= src[0] && src[0] <= 0xFE:
		if length < 2 {
			return err
		}
		if 0x40 <= src[1] && src[1] < 0x7F || 0x7F < src[1] && src[1] <= 0xFE {
			return src[:2]
		}
		if length < 4 {
			return err
		}
		if 0x30 <= src[1] && src[1] <= 0x39 && 0x81 <= src[2] && src[2] <= 0xfe && 0x30 <= src[3] && src[3] <= 0x39 {
			return src[:4]
		}
		return err
	}
	return err
}

func (*encodingGB18030) MbLen(bs string) int {
	if len(bs) < 2 {
		return 0
	}

	if 0x81 <= bs[0] && bs[0] <= 0xfe {
		if (0x40 <= bs[1] && bs[1] <= 0x7e) || (0x80 <= bs[1] && bs[1] <= 0xfe) {
			return 2
		}
		if 0x30 <= bs[1] && bs[1] <= 0x39 && 0x81 <= bs[2] && bs[2] <= 0xfe && 0x30 <= bs[3] && bs[3] <= 0x39 {
			return 4
		}
	}

	return 0
}

// ToUpper implements Encoding interface.
func (*encodingGB18030) ToUpper(d string) string {
	return strings.ToUpperSpecial(GB18030Case, d)
}

// ToLower implements Encoding interface.
func (*encodingGB18030) ToLower(d string) string {
	return strings.ToLowerSpecial(GB18030Case, d)
}

// customGB18030 is a simplifiedchinese.GB18030 wrapper.
type customGB18030 struct{}

// NewCustomGB18030Encoder return a custom GB18030 encoder.
func NewCustomGB18030Encoder() *encoding.Encoder {
	return customGB18030{}.NewEncoder()
}

// NewCustomGB18030Decoder return a custom GB18030 decoder.
func NewCustomGB18030Decoder() *encoding.Decoder {
	return customGB18030{}.NewDecoder()
}

// NewDecoder returns simplifiedchinese.GB18030.NewDecoder().
func (customGB18030) NewDecoder() *encoding.Decoder {
	return &encoding.Decoder{
		Transformer: customGB18030Decoder{
			gb18030Decoder: simplifiedchinese.GB18030.NewDecoder(),
		},
	}
}

// NewEncoder returns simplifiedchinese.gb18030.
func (customGB18030) NewEncoder() *encoding.Encoder {
	return &encoding.Encoder{
		Transformer: customGB18030Encoder{
			gb18030Encoder: simplifiedchinese.GB18030.NewEncoder(),
		},
	}
}

type customGB18030Decoder struct {
	gb18030Decoder *encoding.Decoder
}

// Transform special treatment for 0x80,
func (c customGB18030Decoder) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error) {
	if len(src) == 0 {
		return 0, 0, nil
	}
	for next := 0; nSrc < len(src); nSrc += next {
		if nDst >= len(dst) {
			return nDst, nSrc, transform.ErrShortDst
		}
		next = len(peek(src[nSrc:]))
		if nSrc+next > len(src) {
			return nDst, nSrc, transform.ErrShortSrc
		}

		if src[nSrc] == 0x80 {
			nDst += utf8.EncodeRune(dst[nDst:], utf8.RuneError)
		} else if r, ok := gb18030ToUnicode[convertBytesToUint32(src[nSrc:nSrc+next])]; ok {
			nDst += utf8.EncodeRune(dst[nDst:], r)
		} else {
			d, _, e := c.gb18030Decoder.Transform(dst[nDst:], src[nSrc:nSrc+next], atEOF)
			if e != nil {
				return nDst, nSrc, e
			}
			nDst += d
		}
	}
	return
}

// Reset is same as simplifiedchinese.GB18030.Reset().
func (c customGB18030Decoder) Reset() {
	c.gb18030Decoder.Reset()
}

type customGB18030Encoder struct {
	gb18030Encoder *encoding.Encoder
}

// Transform special treatment for `€`,
func (c customGB18030Encoder) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error) {
	if len(src) == 0 {
		return 0, 0, nil
	}
	for nSrc < len(src) {
		if nDst >= len(dst) {
			return nDst, nSrc, transform.ErrShortDst
		}
		r, size := utf8.DecodeRune(src[nSrc:])
		if v, ok := unicodeToGB18030[r]; ok {
			bytes := convertUint32ToBytes(v)
			if nDst+len(bytes) > len(dst) {
				return nDst, nSrc, transform.ErrShortDst
			}
			copy(dst[nDst:], bytes)
			nDst += len(bytes)
			nSrc += size
		} else {
			d, s, e := c.gb18030Encoder.Transform(dst[nDst:], src[nSrc:nSrc+size], atEOF)
			if e != nil {
				return nDst, nSrc, e
			}
			nDst += d
			nSrc += s
		}
	}
	return
}

// Reset is same as simplifiedchinese.gb18030.
func (c customGB18030Encoder) Reset() {
	c.gb18030Encoder.Reset()
}

func convertBytesToUint32(b []byte) uint32 {
	switch len(b) {
	case 4:
		return (uint32(b[0]) << 24) | (uint32(b[1]) << 16) | (uint32(b[2]) << 8) | uint32(b[3])
	case 3:
		return (uint32(b[0]) << 16) | (uint32(b[1]) << 8) | uint32(b[2])
	case 2:
		return (uint32(b[0]) << 8) | uint32(b[1])
	case 1:
		return uint32(b[0])
	}
	return 0
}

func convertUint32ToBytes(v uint32) []byte {
	var b []byte
	switch {
	case v&0xff000000 > 0:
		b = make([]byte, 4)
		binary.BigEndian.PutUint32(b, v)
	case v&0xff0000 > 0:
		b = make([]byte, 3)
		b[0] = byte(v >> 16)
		b[1] = byte(v >> 8)
		b[2] = byte(v)
	case v&0xff00 > 0:
		b = make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(v))
	default:
		b = make([]byte, 1)
		b[0] = byte(v)
	}
	return b
}
