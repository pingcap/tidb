// Copyright 2023 PingCAP, Inc.
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
	"bytes"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/simplifiedchinese"
	"unicode/utf8"
)

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
	length := len(src)
	if length == 0 {
		return src
	}
	charLen := 4
	if length > 0 && src[0] < 0x80 {
		charLen = 1
	} else if length >= 2 && src[1] >= 0x40 && src[1] <= 0xfe {
		charLen = 2
	}
	if charLen < length {
		return src[:charLen]
	}
	return src
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

// customGB18030 is a simplifiedchinese.GB18030 wrapper.
type customGB18030 struct{}

// NewCustomGB18030Encoder return a custom GB18030 encoding.
func NewCustomGB18030Encoder() *encoding.Encoder {
	return customGB18030{}.NewEncoder()
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
	if src[0] == 0x80 {
		return utf8.EncodeRune(dst[:], utf8.RuneError), 1, nil
	}
	return c.gb18030Decoder.Transform(dst, src, atEOF)
}

// Reset is same as simplifiedchinese.GB18030.Reset().
func (c customGB18030Decoder) Reset() {
	c.gb18030Decoder.Reset()
}

type customGB18030Encoder struct {
	gb18030Encoder *encoding.Encoder
}

// Transform special treatment for `€`,
// see https://github.com/pingcap/tidb/issues/30581 get details.
func (c customGB18030Encoder) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error) {
	if bytes.HasPrefix(src, []byte{0xe2, 0x82, 0xac} /* '€' */) {
		return 0, 0, ErrInvalidCharacterString
	}
	return c.gb18030Encoder.Transform(dst, src, atEOF)
}

// Reset is same as simplifiedchinese.gb18030.
func (c customGB18030Encoder) Reset() {
	c.gb18030Encoder.Reset()
}
