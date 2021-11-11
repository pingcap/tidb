package charset

import (
	"golang.org/x/text/encoding"
	"unicode/utf8"
)

var UTF8Encoding = &Encoding{
	enc:  encoding.Nop,
	name: CharsetUTF8MB4,
	charLength: func(bs []byte) int {
		l := 0
		if bs[0] < 0x80 {
			l = 1
		} else if bs[0] < 0xe0 {
			l = 2
		} else if bs[0] < 0xf0 {
			l = 3
		} else {
			l = 4
		}

		if len(bs) < l || !utf8.Valid(bs[:l]) {
			return 1
		}
		return l
	},
	mbCharLength: func(bs []byte) int {
		l := 0
		if bs[0] < 0x80 {
			l = 1
		} else if bs[0] < 0xe0 {
			l = 2
		} else if bs[0] < 0xf0 {
			l = 3
		} else {
			l = 4
		}

		if len(bs) < l || !utf8.Valid(bs[:l]) {
			return 1
		}
		return l
	},
	specialCase: nil,
}
