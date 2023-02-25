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

import "unicode"

// GB18030Case follows the cases from MySQL
var GB18030Case = unicode.SpecialCase{
	unicode.CaseRange{Lo: 0xB5, Hi: 0xB5, Delta: [unicode.MaxCase]rune{0, 775, 0}},
	unicode.CaseRange{Lo: 0x1C5, Hi: 0x1C5, Delta: [unicode.MaxCase]rune{0, 1, 0}},
	unicode.CaseRange{Lo: 0x1C8, Hi: 0x1C8, Delta: [unicode.MaxCase]rune{0, 1, 0}},
	unicode.CaseRange{Lo: 0x1CB, Hi: 0x1CB, Delta: [unicode.MaxCase]rune{0, 1, 0}},
	unicode.CaseRange{Lo: 0x1F2, Hi: 0x1F2, Delta: [unicode.MaxCase]rune{0, 1, 0}},
	unicode.CaseRange{Lo: 0x25C, Hi: 0x25C, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x261, Hi: 0x261, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x265, Hi: 0x266, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x26A, Hi: 0x26A, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x26C, Hi: 0x26C, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x282, Hi: 0x282, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x287, Hi: 0x287, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x29D, Hi: 0x29E, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x37F, Hi: 0x37F, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x3C2, Hi: 0x3C2, Delta: [unicode.MaxCase]rune{0, 1, 0}},
	unicode.CaseRange{Lo: 0x3D0, Hi: 0x3D0, Delta: [unicode.MaxCase]rune{0, -30, 0}},
	unicode.CaseRange{Lo: 0x3D1, Hi: 0x3D1, Delta: [unicode.MaxCase]rune{0, -25, 0}},
	unicode.CaseRange{Lo: 0x3D5, Hi: 0x3D5, Delta: [unicode.MaxCase]rune{0, -15, 0}},
	unicode.CaseRange{Lo: 0x3D6, Hi: 0x3D6, Delta: [unicode.MaxCase]rune{0, -22, 0}},
	unicode.CaseRange{Lo: 0x3F0, Hi: 0x3F0, Delta: [unicode.MaxCase]rune{0, -54, 0}},
	unicode.CaseRange{Lo: 0x3F1, Hi: 0x3F1, Delta: [unicode.MaxCase]rune{0, -48, 0}},
	unicode.CaseRange{Lo: 0x3F3, Hi: 0x3F3, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x3F5, Hi: 0x3F5, Delta: [unicode.MaxCase]rune{0, -64, 0}},
	unicode.CaseRange{Lo: 0x526, Hi: 0x52F, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x10C7, Hi: 0x10C7, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x10CD, Hi: 0x10CD, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x10D0, Hi: 0x10FA, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x10FD, Hi: 0x10FF, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x13A0, Hi: 0x13F5, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x13F8, Hi: 0x13FD, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x1C80, Hi: 0x1C88, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x1C90, Hi: 0x1CBA, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x1CBD, Hi: 0x1CBF, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x1D79, Hi: 0x1D79, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x1D7D, Hi: 0x1D7D, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x1D8E, Hi: 0x1D8E, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x1E9B, Hi: 0x1E9B, Delta: [unicode.MaxCase]rune{0, -58, 0}},
	unicode.CaseRange{Lo: 0x1FBE, Hi: 0x1FBE, Delta: [unicode.MaxCase]rune{0, -7173, 0}},
	unicode.CaseRange{Lo: 0x2CF2, Hi: 0x2CF3, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x2D27, Hi: 0x2D27, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x2D2D, Hi: 0x2D2D, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0xA660, Hi: 0xA661, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0xA698, Hi: 0xA69B, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0xA78D, Hi: 0xA78D, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0xA790, Hi: 0xA794, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0xA796, Hi: 0xA7AE, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0xA7B0, Hi: 0xA7BF, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0xA7C2, Hi: 0xA7CA, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0xA7F5, Hi: 0xA7F6, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0xAB53, Hi: 0xAB53, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0xAB70, Hi: 0xABBF, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x104B0, Hi: 0x104D3, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x104D8, Hi: 0x104FB, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x10C80, Hi: 0x10CB2, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x10CC0, Hi: 0x10CF2, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x118A0, Hi: 0x118DF, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x16E40, Hi: 0x16E7F, Delta: [unicode.MaxCase]rune{0, 0, 0}},
	unicode.CaseRange{Lo: 0x1E900, Hi: 0x1E943, Delta: [unicode.MaxCase]rune{0, 0, 0}},
}
