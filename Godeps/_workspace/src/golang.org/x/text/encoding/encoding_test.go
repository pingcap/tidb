// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package encoding_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

func trim(s string) string {
	if len(s) < 120 {
		return s
	}
	return s[:50] + "..." + s[len(s)-50:]
}

var basicTestCases = []struct {
	e         encoding.Encoding
	encPrefix string
	encoded   string
	utf8      string
}{
	// The encoded forms can be verified by the iconv program:
	// $ echo 月日は百代 | iconv -f UTF-8 -t SHIFT-JIS | xxd

	// Charmap tests.
	{
		e:       charmap.CodePage437,
		encoded: "H\x82ll\x93 \x9d\xa7\xf4\x9c\xbe",
		utf8:    "Héllô ¥º⌠£╛",
	},
	{
		e:       charmap.CodePage866,
		encoded: "H\xf3\xd3o \x98\xfd\x9f\xdd\xa1",
		utf8:    "Hє╙o Ш¤Я▌б",
	},
	{
		e:       charmap.ISO8859_2,
		encoded: "Hel\xe5\xf5",
		utf8:    "Helĺő",
	},
	{
		e:       charmap.ISO8859_3,
		encoded: "He\xbd\xd4",
		utf8:    "He½Ô",
	},
	{
		e:       charmap.ISO8859_4,
		encoded: "Hel\xb6\xf8",
		utf8:    "Helļø",
	},
	{
		e:       charmap.ISO8859_5,
		encoded: "H\xd7\xc6o",
		utf8:    "HзЦo",
	},
	{
		e:       charmap.ISO8859_6,
		encoded: "Hel\xc2\xc9",
		utf8:    "Helآة",
	},
	{
		e:       charmap.ISO8859_7,
		encoded: "H\xeel\xebo",
		utf8:    "Hξlλo",
	},
	{
		e:       charmap.ISO8859_8,
		encoded: "Hel\xf5\xed",
		utf8:    "Helץם",
	},
	{
		e:       charmap.ISO8859_10,
		encoded: "H\xea\xbfo",
		utf8:    "Hęŋo",
	},
	{
		e:       charmap.ISO8859_13,
		encoded: "H\xe6l\xf9o",
		utf8:    "Hęlło",
	},
	{
		e:       charmap.ISO8859_14,
		encoded: "He\xfe\xd0o",
		utf8:    "HeŷŴo",
	},
	{
		e:       charmap.ISO8859_15,
		encoded: "H\xa4ll\xd8",
		utf8:    "H€llØ",
	},
	{
		e:       charmap.ISO8859_16,
		encoded: "H\xe6ll\xbd",
		utf8:    "Hællœ",
	},
	{
		e:       charmap.KOI8R,
		encoded: "He\x93\xad\x9c",
		utf8:    "He⌠╜°",
	},
	{
		e:       charmap.KOI8U,
		encoded: "He\x93\xad\x9c",
		utf8:    "He⌠ґ°",
	},
	{
		e:       charmap.Macintosh,
		encoded: "He\xdf\xd7",
		utf8:    "Heﬂ◊",
	},
	{
		e:       charmap.MacintoshCyrillic,
		encoded: "He\xbe\x94",
		utf8:    "HeЊФ",
	},
	{
		e:       charmap.Windows874,
		encoded: "He\xb7\xf0",
		utf8:    "Heท๐",
	},
	{
		e:       charmap.Windows1250,
		encoded: "He\xe5\xe5o",
		utf8:    "Heĺĺo",
	},
	{
		e:       charmap.Windows1251,
		encoded: "H\xball\xfe",
		utf8:    "Hєllю",
	},
	{
		e:       charmap.Windows1252,
		encoded: "H\xe9ll\xf4 \xa5\xbA\xae\xa3\xd0",
		utf8:    "Héllô ¥º®£Ð",
	},
	{
		e:       charmap.Windows1253,
		encoded: "H\xe5ll\xd6",
		utf8:    "HεllΦ",
	},
	{
		e:       charmap.Windows1254,
		encoded: "\xd0ello",
		utf8:    "Ğello",
	},
	{
		e:       charmap.Windows1255,
		encoded: "He\xd4o",
		utf8:    "Heװo",
	},
	{
		e:       charmap.Windows1256,
		encoded: "H\xdbllo",
		utf8:    "Hغllo",
	},
	{
		e:       charmap.Windows1257,
		encoded: "He\xeflo",
		utf8:    "Heļlo",
	},
	{
		e:       charmap.Windows1258,
		encoded: "Hell\xf5",
		utf8:    "Hellơ",
	},
	{
		e:       charmap.XUserDefined,
		encoded: "\x00\x40\x7f\x80\xab\xff",
		utf8:    "\u0000\u0040\u007f\uf780\uf7ab\uf7ff",
	},

	// UTF-16 tests.
	{
		e:       unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM),
		encoded: "\x00\x57\x00\xe4\xd8\x35\xdd\x65",
		utf8:    "\x57\u00e4\U0001d565",
	},
	{
		e:         unicode.UTF16(unicode.BigEndian, unicode.ExpectBOM),
		encPrefix: "\xfe\xff",
		encoded:   "\x00\x57\x00\xe4\xd8\x35\xdd\x65",
		utf8:      "\x57\u00e4\U0001d565",
	},
	{
		e:       unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM),
		encoded: "\x57\x00\xe4\x00\x35\xd8\x65\xdd",
		utf8:    "\x57\u00e4\U0001d565",
	},
	{
		e:         unicode.UTF16(unicode.LittleEndian, unicode.ExpectBOM),
		encPrefix: "\xff\xfe",
		encoded:   "\x57\x00\xe4\x00\x35\xd8\x65\xdd",
		utf8:      "\x57\u00e4\U0001d565",
	},

	// Chinese tests.
	//
	// "\u0081\u00de\u00df\u00e0\u00e1\u00e2\u00e3\uffff\U00010000" is a
	// nonsense string that contains GB18030 encodable codepoints of which
	// only U+00E0 and U+00E1 are GBK encodable.
	//
	// "A\u3000\u554a\u4e02\u4e90\u72dc\u7349\u02ca\u2588Z€" is a nonsense
	// string that contains ASCII and GBK encodable codepoints from Levels
	// 1-5 as well as the Euro sign.
	//
	// "A\u43f0\u4c32\U00027267\u3000\U0002910d\u79d4Z€" is a nonsense string
	// that contains ASCII and Big5 encodable codepoints from the Basic
	// Multilingual Plane and the Supplementary Ideographic Plane as well as
	// the Euro sign.
	//
	// "花间一壶酒，独酌无相亲。" (simplified) and
	// "花間一壺酒，獨酌無相親。" (traditional)
	// are from the 8th century poem "Yuè Xià Dú Zhuó".
	{
		e: simplifiedchinese.GB18030,
		encoded: "\x81\x30\x81\x31\x81\x30\x89\x37\x81\x30\x89\x38\xa8\xa4\xa8\xa2" +
			"\x81\x30\x89\x39\x81\x30\x8a\x30\x84\x31\xa4\x39\x90\x30\x81\x30",
		utf8: "\u0081\u00de\u00df\u00e0\u00e1\u00e2\u00e3\uffff\U00010000",
	},
	{
		e: simplifiedchinese.GB18030,
		encoded: "\xbb\xa8\xbc\xe4\xd2\xbb\xba\xf8\xbe\xc6\xa3\xac\xb6\xc0\xd7\xc3" +
			"\xce\xde\xcf\xe0\xc7\xd7\xa1\xa3",
		utf8: "花间一壶酒，独酌无相亲。",
	},
	{
		e:       simplifiedchinese.GBK,
		encoded: "A\xa1\xa1\xb0\xa1\x81\x40\x81\x80\xaa\x40\xaa\x80\xa8\x40\xa8\x80Z\x80",
		utf8:    "A\u3000\u554a\u4e02\u4e90\u72dc\u7349\u02ca\u2588Z€",
	},
	{
		e: simplifiedchinese.GBK,
		encoded: "\xbb\xa8\xbc\xe4\xd2\xbb\xba\xf8\xbe\xc6\xa3\xac\xb6\xc0\xd7\xc3" +
			"\xce\xde\xcf\xe0\xc7\xd7\xa1\xa3",
		utf8: "花间一壶酒，独酌无相亲。",
	},
	{
		e:       simplifiedchinese.HZGB2312,
		encoded: "A~{\x21\x21~~\x30\x21~}Z~~",
		utf8:    "A\u3000~\u554aZ~",
	},
	{
		e:         simplifiedchinese.HZGB2312,
		encPrefix: "~{",
		encoded:   ";(<dR;:x>F#,6@WCN^O`GW!#",
		utf8:      "花间一壶酒，独酌无相亲。",
	},
	{
		e:       traditionalchinese.Big5,
		encoded: "A\x87\x40\x87\x41\x87\x45\xa1\x40\xfe\xfd\xfe\xfeZ\xa3\xe1",
		utf8:    "A\u43f0\u4c32\U00027267\u3000\U0002910d\u79d4Z€",
	},
	{
		e: traditionalchinese.Big5,
		encoded: "\xaa\xe1\xb6\xa1\xa4\x40\xb3\xfd\xb0\x73\xa1\x41\xbf\x57\xb0\x75" +
			"\xb5\x4c\xac\xdb\xbf\xcb\xa1\x43",
		utf8: "花間一壺酒，獨酌無相親。",
	},

	// Japanese tests.
	//
	// "A｡ｶﾟ 0208: etc 0212: etc" is a nonsense string that contains ASCII, half-width
	// kana, JIS X 0208 (including two near the kink in the Shift JIS second byte
	// encoding) and JIS X 0212 encodable codepoints.
	//
	// "月日は百代の過客にして、行かふ年も又旅人也。" is from the 17th century poem
	// "Oku no Hosomichi" and contains both hiragana and kanji.
	{
		e: japanese.EUCJP,
		encoded: "A\x8e\xa1\x8e\xb6\x8e\xdf " +
			"0208: \xa1\xa1\xa1\xa2\xa1\xdf\xa1\xe0\xa1\xfd\xa1\xfe\xa2\xa1\xa2\xa2\xf4\xa6 " +
			"0212: \x8f\xa2\xaf\x8f\xed\xe3",
		utf8: "A｡ｶﾟ " +
			"0208: \u3000\u3001\u00d7\u00f7\u25ce\u25c7\u25c6\u25a1\u7199 " +
			"0212: \u02d8\u9fa5",
	},
	{
		e: japanese.EUCJP,
		encoded: "\xb7\xee\xc6\xfc\xa4\xcf\xc9\xb4\xc2\xe5\xa4\xce\xb2\xe1\xb5\xd2" +
			"\xa4\xcb\xa4\xb7\xa4\xc6\xa1\xa2\xb9\xd4\xa4\xab\xa4\xd5\xc7\xaf" +
			"\xa4\xe2\xcb\xf4\xce\xb9\xbf\xcd\xcc\xe9\xa1\xa3",
		utf8: "月日は百代の過客にして、行かふ年も又旅人也。",
	},
	{
		e: japanese.ISO2022JP,
		encoded: "\x1b(I\x21\x36\x5f\x1b(B " +
			"0208: \x1b$B\x21\x21\x21\x22\x21\x5f\x21\x60\x21\x7d\x21\x7e\x22\x21\x22\x22\x74\x26",
		utf8: "｡ｶﾟ " +
			"0208: \u3000\u3001\u00d7\u00f7\u25ce\u25c7\u25c6\u25a1\u7199",
	},
	{
		e:         japanese.ISO2022JP,
		encPrefix: "\x1b\x24\x42",
		encoded: "\x37\x6e\x46\x7c\x24\x4f\x49\x34\x42\x65\x24\x4e\x32\x61\x35\x52" +
			"\x24\x4b\x24\x37\x24\x46\x21\x22\x39\x54\x24\x2b\x24\x55\x47\x2f" +
			"\x24\x62\x4b\x74\x4e\x39\x3f\x4d\x4c\x69\x21\x23",
		utf8: "月日は百代の過客にして、行かふ年も又旅人也。",
	},
	{
		e: japanese.ShiftJIS,
		encoded: "A\xa1\xb6\xdf " +
			"0208: \x81\x40\x81\x41\x81\x7e\x81\x80\x81\x9d\x81\x9e\x81\x9f\x81\xa0\xea\xa4",
		utf8: "A｡ｶﾟ " +
			"0208: \u3000\u3001\u00d7\u00f7\u25ce\u25c7\u25c6\u25a1\u7199",
	},
	{
		e: japanese.ShiftJIS,
		encoded: "\x8c\x8e\x93\xfa\x82\xcd\x95\x53\x91\xe3\x82\xcc\x89\xdf\x8b\x71" +
			"\x82\xc9\x82\xb5\x82\xc4\x81\x41\x8d\x73\x82\xa9\x82\xd3\x94\x4e" +
			"\x82\xe0\x96\x94\x97\xb7\x90\x6c\x96\xe7\x81\x42",
		utf8: "月日は百代の過客にして、行かふ年も又旅人也。",
	},

	// Korean tests.
	//
	// "A\uac02\uac35\uac56\ud401B\ud408\ud620\ud624C\u4f3d\u8a70D" is a
	// nonsense string that contains ASCII, Hangul and CJK ideographs.
	//
	// "세계야, 안녕" translates as "Hello, world".
	{
		e:       korean.EUCKR,
		encoded: "A\x81\x41\x81\x61\x81\x81\xc6\xfeB\xc7\xa1\xc7\xfe\xc8\xa1C\xca\xa1\xfd\xfeD",
		utf8:    "A\uac02\uac35\uac56\ud401B\ud408\ud620\ud624C\u4f3d\u8a70D",
	},
	{
		e:       korean.EUCKR,
		encoded: "\xbc\xbc\xb0\xe8\xbe\xdf\x2c\x20\xbe\xc8\xb3\xe7",
		utf8:    "세계야, 안녕",
	},
}

func TestBasics(t *testing.T) {
	for _, tc := range basicTestCases {
		for _, direction := range []string{"Decode", "Encode"} {
			newTransformer, want, src := (func() transform.Transformer)(nil), "", ""
			wPrefix, sPrefix := "", ""
			if direction == "Decode" {
				newTransformer, want, src = tc.e.NewDecoder, tc.utf8, tc.encoded
				wPrefix, sPrefix = "", tc.encPrefix
			} else {
				newTransformer, want, src = tc.e.NewEncoder, tc.encoded, tc.utf8
				wPrefix, sPrefix = tc.encPrefix, ""
			}

			dst := make([]byte, len(wPrefix)+len(want))
			nDst, nSrc, err := newTransformer().Transform(dst, []byte(sPrefix+src), true)
			if err != nil {
				t.Errorf("%v: %s: %v", tc.e, direction, err)
				continue
			}
			if nDst != len(wPrefix)+len(want) {
				t.Errorf("%v: %s: nDst got %d, want %d",
					tc.e, direction, nDst, len(wPrefix)+len(want))
				continue
			}
			if nSrc != len(sPrefix)+len(src) {
				t.Errorf("%v: %s: nSrc got %d, want %d",
					tc.e, direction, nSrc, len(sPrefix)+len(src))
				continue
			}
			if got := string(dst); got != wPrefix+want {
				t.Errorf("%v: %s:\ngot  %q\nwant %q",
					tc.e, direction, got, wPrefix+want)
				continue
			}

			for _, n := range []int{0, 1, 2, 10, 123, 4567} {
				input := sPrefix + strings.Repeat(src, n)
				sr := strings.NewReader(input)
				g, err := ioutil.ReadAll(transform.NewReader(sr, newTransformer()))
				if err != nil {
					t.Errorf("%v: %s: ReadAll: n=%d: %v", tc.e, direction, n, err)
					continue
				}
				if len(g) == 0 && len(input) == 0 {
					// If the input is empty then the output can be empty,
					// regardless of whatever wPrefix is.
					continue
				}
				got1, want1 := string(g), wPrefix+strings.Repeat(want, n)
				if got1 != want1 {
					t.Errorf("%v: %s: ReadAll: n=%d\ngot  %q\nwant %q",
						tc.e, direction, n, trim(got1), trim(want1))
					continue
				}
			}
		}
	}
}

// TestNonRepertoire tests that codes outside of an Encoding's repertoire are
// converted:
//   - to the Unicode replacement character '\ufffd' when decoding to UTF-8,
//   - to the ASCII substitute character '\x1a' when encoding from UTF-8.
func TestNonRepertoire(t *testing.T) {
	testCases := []struct {
		e          encoding.Encoding
		dSrc, eSrc string
	}{
		{charmap.Windows1252, "\x81", "갂"},
		{japanese.EUCJP, "\xfe\xfc", "갂"},
		{japanese.ISO2022JP, "\x1b$B\x7e\x7e", "갂"},
		{japanese.ShiftJIS, "\xef\xfc", "갂"},
		{korean.EUCKR, "\xfe\xfe", "א"},
		{simplifiedchinese.GBK, "\xfe\xfe", "갂"},
		{simplifiedchinese.HZGB2312, "~{z~", "갂"},
		{traditionalchinese.Big5, "\x81\x40", "갂"},
	}
	for _, tc := range testCases {
		for _, direction := range []string{"Decode", "Encode"} {
			enc, want, src := (transform.Transformer)(nil), "", ""
			if direction == "Decode" {
				enc, want, src = tc.e.NewDecoder(), "\ufffd", tc.dSrc
			} else {
				enc, want, src = tc.e.NewEncoder(), "\x1a", tc.eSrc
			}

			dst, err := ioutil.ReadAll(transform.NewReader(strings.NewReader(src), enc))
			if err != nil {
				t.Errorf("%s %v: %v", direction, tc.e, err)
				continue
			}
			if got := string(dst); got != want {
				t.Errorf("%s %v:\ngot  %q\nwant %q", direction, tc.e, got, want)
				continue
			}
		}
	}
}

// TestBig5CircumflexAndMacron tests the special cases listed in
// http://encoding.spec.whatwg.org/#big5
// Note that these special cases aren't preserved by round-tripping through
// decoding and encoding (since
// http://encoding.spec.whatwg.org/index-big5.txt does not have an entry for
// U+0304 or U+030C), so we can't test this in TestBasics.
func TestBig5CircumflexAndMacron(t *testing.T) {
	src := "\x88\x5f\x88\x60\x88\x61\x88\x62\x88\x63\x88\x64\x88\x65\x88\x66 " +
		"\x88\xa2\x88\xa3\x88\xa4\x88\xa5\x88\xa6"
	want := "ÓǑÒ\u00ca\u0304Ế\u00ca\u030cỀÊ " +
		"ü\u00ea\u0304ế\u00ea\u030cề"
	dst, err := ioutil.ReadAll(transform.NewReader(
		strings.NewReader(src), traditionalchinese.Big5.NewDecoder()))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(dst); got != want {
		t.Fatalf("\ngot  %q\nwant %q", got, want)
	}
}

func TestEncodeInvalidUTF8(t *testing.T) {
	inputs := []string{
		"hello.",
		"wo\ufffdld.",
		"ABC\xff\x80\x80", // Invalid UTF-8.
		"\x80\x80\x80\x80\x80",
		"\x80\x80D\x80\x80",          // Valid rune at "D".
		"E\xed\xa0\x80\xed\xbf\xbfF", // Two invalid UTF-8 runes (surrogates).
		"G",
		"H\xe2\x82",     // U+20AC in UTF-8 is "\xe2\x82\xac", which we split over two
		"\xacI\xe2\x82", // input lines. It maps to 0x80 in the Windows-1252 encoding.
	}
	// Each invalid source byte becomes '\x1a'.
	want := strings.Replace("hello.wo?ld.ABC??????????D??E??????FGH\x80I??", "?", "\x1a", -1)

	transformer := charmap.Windows1252.NewEncoder()
	gotBuf := make([]byte, 0, 1024)
	src := make([]byte, 0, 1024)
	for i, input := range inputs {
		dst := make([]byte, 1024)
		src = append(src, input...)
		atEOF := i == len(inputs)-1
		nDst, nSrc, err := transformer.Transform(dst, src, atEOF)
		gotBuf = append(gotBuf, dst[:nDst]...)
		src = src[nSrc:]
		if err != nil && err != transform.ErrShortSrc {
			t.Fatalf("i=%d: %v", i, err)
		}
		if atEOF && err != nil {
			t.Fatalf("i=%d: atEOF: %v", i, err)
		}
	}
	if got := string(gotBuf); got != want {
		t.Fatalf("\ngot  %+q\nwant %+q", got, want)
	}
}

func TestReplacement(t *testing.T) {
	for _, direction := range []string{"Decode", "Encode"} {
		enc, want := (transform.Transformer)(nil), ""
		if direction == "Decode" {
			enc = encoding.Replacement.NewDecoder()
			want = "\ufffd"
		} else {
			enc = encoding.Replacement.NewEncoder()
			want = "AB\x00CD\ufffdYZ"
		}
		sr := strings.NewReader("AB\x00CD\x80YZ")
		g, err := ioutil.ReadAll(transform.NewReader(sr, enc))
		if err != nil {
			t.Errorf("%s: ReadAll: %v", direction, err)
			continue
		}
		if got := string(g); got != want {
			t.Errorf("%s:\ngot  %q\nwant %q", direction, got, want)
			continue
		}
	}
}

func TestUTF8Validator(t *testing.T) {
	testCases := []struct {
		desc    string
		dstSize int
		src     string
		atEOF   bool
		want    string
		wantErr error
	}{
		{
			"empty input",
			100,
			"",
			false,
			"",
			nil,
		},
		{
			"valid 1-byte 1-rune input",
			100,
			"a",
			false,
			"a",
			nil,
		},
		{
			"valid 3-byte 1-rune input",
			100,
			"\u1234",
			false,
			"\u1234",
			nil,
		},
		{
			"valid 5-byte 3-rune input",
			100,
			"a\u0100\u0101",
			false,
			"a\u0100\u0101",
			nil,
		},
		{
			"perfectly sized dst (non-ASCII)",
			5,
			"a\u0100\u0101",
			false,
			"a\u0100\u0101",
			nil,
		},
		{
			"short dst (non-ASCII)",
			4,
			"a\u0100\u0101",
			false,
			"a\u0100",
			transform.ErrShortDst,
		},
		{
			"perfectly sized dst (ASCII)",
			5,
			"abcde",
			false,
			"abcde",
			nil,
		},
		{
			"short dst (ASCII)",
			4,
			"abcde",
			false,
			"abcd",
			transform.ErrShortDst,
		},
		{
			"partial input (!EOF)",
			100,
			"a\u0100\xf1",
			false,
			"a\u0100",
			transform.ErrShortSrc,
		},
		{
			"invalid input (EOF)",
			100,
			"a\u0100\xf1",
			true,
			"a\u0100",
			encoding.ErrInvalidUTF8,
		},
		{
			"invalid input (!EOF)",
			100,
			"a\u0100\x80",
			false,
			"a\u0100",
			encoding.ErrInvalidUTF8,
		},
		{
			"invalid input (above U+10FFFF)",
			100,
			"a\u0100\xf7\xbf\xbf\xbf",
			false,
			"a\u0100",
			encoding.ErrInvalidUTF8,
		},
		{
			"invalid input (surrogate half)",
			100,
			"a\u0100\xed\xa0\x80",
			false,
			"a\u0100",
			encoding.ErrInvalidUTF8,
		},
	}
	for _, tc := range testCases {
		dst := make([]byte, tc.dstSize)
		nDst, nSrc, err := encoding.UTF8Validator.Transform(dst, []byte(tc.src), tc.atEOF)
		if nDst < 0 || len(dst) < nDst {
			t.Errorf("%s: nDst=%d out of range", tc.desc, nDst)
			continue
		}
		got := string(dst[:nDst])
		if got != tc.want || nSrc != len(tc.want) || err != tc.wantErr {
			t.Errorf("%s:\ngot  %+q, %d, %v\nwant %+q, %d, %v",
				tc.desc, got, nSrc, err, tc.want, len(tc.want), tc.wantErr)
			continue
		}
	}
}

// TODO: UTF-16-specific tests:
// - inputs with multiple U+FEFF and U+FFFE runes. These should not be replaced
//   by U+FFFD.
// - malformed input: an odd number of bytes (and atEOF), or unmatched
//   surrogates. These should be replaced with U+FFFD.

var utf16LEIB = unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM)

// testdataFiles are files in testdata/*.txt.
var testdataFiles = []struct {
	enc           encoding.Encoding
	basename, ext string
}{
	{charmap.Windows1252, "candide", "windows-1252"},
	{japanese.EUCJP, "rashomon", "euc-jp"},
	{japanese.ISO2022JP, "rashomon", "iso-2022-jp"},
	{japanese.ShiftJIS, "rashomon", "shift-jis"},
	{korean.EUCKR, "unsu-joh-eun-nal", "euc-kr"},
	{simplifiedchinese.GBK, "sunzi-bingfa-simplified", "gbk"},
	{simplifiedchinese.HZGB2312, "sunzi-bingfa-gb-levels-1-and-2", "hz-gb2312"},
	{traditionalchinese.Big5, "sunzi-bingfa-traditional", "big5"},
	{utf16LEIB, "candide", "utf-16le"},

	// GB18030 is a superset of GBK and is nominally a Simplified Chinese
	// encoding, but it can also represent the entire Basic Multilingual
	// Plane, including codepoints like 'â' that aren't encodable by GBK.
	// GB18030 on Simplified Chinese should perform similarly to GBK on
	// Simplified Chinese. GB18030 on "candide" is more interesting.
	{simplifiedchinese.GB18030, "candide", "gb18030"},
}

func load(direction string, enc encoding.Encoding) ([]byte, []byte, func() transform.Transformer, error) {
	basename, ext, count := "", "", 0
	for _, tf := range testdataFiles {
		if tf.enc == enc {
			basename, ext = tf.basename, tf.ext
			count++
		}
	}
	if count != 1 {
		if count == 0 {
			return nil, nil, nil, fmt.Errorf("no testdataFiles for %s", enc)
		}
		return nil, nil, nil, fmt.Errorf("too many testdataFiles for %s", enc)
	}
	dstFile := fmt.Sprintf("testdata/%s-%s.txt", basename, ext)
	srcFile := fmt.Sprintf("testdata/%s-utf-8.txt", basename)
	newTransformer := enc.NewEncoder
	if direction == "Decode" {
		dstFile, srcFile = srcFile, dstFile
		newTransformer = enc.NewDecoder
	}
	dst, err := ioutil.ReadFile(dstFile)
	if err != nil {
		return nil, nil, nil, err
	}
	src, err := ioutil.ReadFile(srcFile)
	if err != nil {
		return nil, nil, nil, err
	}
	return dst, src, newTransformer, nil
}

func TestFiles(t *testing.T) {
	for _, dir := range []string{"Decode", "Encode"} {
		for _, tf := range testdataFiles {
			dst, src, newTransformer, err := load(dir, tf.enc)
			if err != nil {
				t.Errorf("%s, %s: load: %v", dir, tf.enc, err)
				continue
			}
			buf := bytes.NewBuffer(nil)
			r := transform.NewReader(bytes.NewReader(src), newTransformer())
			if _, err := io.Copy(buf, r); err != nil {
				t.Errorf("%s, %s: copy: %v", dir, tf.enc, err)
				continue
			}
			if !bytes.Equal(buf.Bytes(), dst) {
				t.Errorf("%s, %s: transformed bytes did not match golden file", dir, tf.enc)
				continue
			}
		}
	}
}

func benchmark(b *testing.B, direction string, enc encoding.Encoding) {
	_, src, newTransformer, err := load(direction, enc)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(src)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := transform.NewReader(bytes.NewReader(src), newTransformer())
		io.Copy(ioutil.Discard, r)
	}
}

func BenchmarkBig5Decoder(b *testing.B)      { benchmark(b, "Decode", traditionalchinese.Big5) }
func BenchmarkBig5Encoder(b *testing.B)      { benchmark(b, "Encode", traditionalchinese.Big5) }
func BenchmarkCharmapDecoder(b *testing.B)   { benchmark(b, "Decode", charmap.Windows1252) }
func BenchmarkCharmapEncoder(b *testing.B)   { benchmark(b, "Encode", charmap.Windows1252) }
func BenchmarkEUCJPDecoder(b *testing.B)     { benchmark(b, "Decode", japanese.EUCJP) }
func BenchmarkEUCJPEncoder(b *testing.B)     { benchmark(b, "Encode", japanese.EUCJP) }
func BenchmarkEUCKRDecoder(b *testing.B)     { benchmark(b, "Decode", korean.EUCKR) }
func BenchmarkEUCKREncoder(b *testing.B)     { benchmark(b, "Encode", korean.EUCKR) }
func BenchmarkGB18030Decoder(b *testing.B)   { benchmark(b, "Decode", simplifiedchinese.GB18030) }
func BenchmarkGB18030Encoder(b *testing.B)   { benchmark(b, "Encode", simplifiedchinese.GB18030) }
func BenchmarkGBKDecoder(b *testing.B)       { benchmark(b, "Decode", simplifiedchinese.GBK) }
func BenchmarkGBKEncoder(b *testing.B)       { benchmark(b, "Encode", simplifiedchinese.GBK) }
func BenchmarkHZGB2312Decoder(b *testing.B)  { benchmark(b, "Decode", simplifiedchinese.HZGB2312) }
func BenchmarkHZGB2312Encoder(b *testing.B)  { benchmark(b, "Encode", simplifiedchinese.HZGB2312) }
func BenchmarkISO2022JPDecoder(b *testing.B) { benchmark(b, "Decode", japanese.ISO2022JP) }
func BenchmarkISO2022JPEncoder(b *testing.B) { benchmark(b, "Encode", japanese.ISO2022JP) }
func BenchmarkShiftJISDecoder(b *testing.B)  { benchmark(b, "Decode", japanese.ShiftJIS) }
func BenchmarkShiftJISEncoder(b *testing.B)  { benchmark(b, "Encode", japanese.ShiftJIS) }
func BenchmarkUTF16Decoder(b *testing.B)     { benchmark(b, "Decode", utf16LEIB) }
func BenchmarkUTF16Encoder(b *testing.B)     { benchmark(b, "Encode", utf16LEIB) }
