package charset

import "golang.org/x/text/encoding/simplifiedchinese"

var GBKEncoding = &Encoding{
	enc:  simplifiedchinese.GBK,
	name: "gbk",
	charLength: func(bs []byte) int {
		if len(bs) < 2 {
			return 1
		}
		if 0x81 <= bs[0] && bs[0] <= 0xf4 {
			return 2
		}

		return 1
	},
	mbCharLength: func(bs []byte) int {
		if len(bs) < 2 {
			return 0
		}

		if 0x81 <= bs[0] && bs[0] <= 0xf4 {
			if (0x40 <= bs[1] && bs[1] <= 0x7e) || (0x80 <= bs[1] && bs[1] <= 0xfe) {
				return 2
			}
		}

		return 0
	},
	specialCase: GBKCase,
}
