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

import (
	"golang.org/x/text/encoding"
)

var UTF8Encoding = &Encoding{
	enc:  encoding.Nop,
	name: CharsetUTF8MB4,
	charLength: func(bs []byte) int {
		if len(bs) == 0 || bs[0] < 0x80 {
			return 1
		} else if bs[0] < 0xe0 {
			return 2
		} else if bs[0] < 0xf0 {
			return 3
		}
		return 4
	},
	specialCase: nil,
}
