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
