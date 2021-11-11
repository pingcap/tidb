package charset

import (
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

var (
	LatinEncoding = &Encoding{
		enc:  charmap.Windows1252,
		name: CharsetLatin1,
		charLength: func(bytes []byte) int {
			return 1
		},
		mbCharLength: func(bytes []byte) int {
			return 1
		},
		specialCase: nil,
	}

	BinaryEncoding = &Encoding{
		enc:  encoding.Nop,
		name: CharsetBin,
		charLength: func(bytes []byte) int {
			return 1
		},
		mbCharLength: func(bytes []byte) int {
			return 1
		},
		specialCase: nil,
	}
)
