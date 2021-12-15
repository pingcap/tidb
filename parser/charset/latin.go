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
	"golang.org/x/text/encoding/charmap"
)

var (
	LatinEncoding = &Encoding{
		enc:  charmap.Windows1252,
		name: CharsetLatin1,
		charLength: func(bytes []byte) int {
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
		specialCase: nil,
	}

	ASCIIEncoding = &Encoding{
		enc:  encoding.Nop,
		name: CharsetASCII,
		charLength: func(bytes []byte) int {
			return 1
		},
		specialCase: nil,
	}
)
