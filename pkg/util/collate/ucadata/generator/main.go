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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	_ "embed"
	"go/format"
	"math"
	"os"
	"text/template"
)

//go:embed allkeys-4.0.0.txt
var allkeys0400 string

//go:embed allkeys-9.0.0.txt
var allkeys0900 string

type cetEntry struct {
	// a cetEntry can actually contain several characters, which means map many to one or many
	// but MySQL's collation doesn't handle these contractions, so we only care the rule with a
	// single char.
	char rune

	// weights is the first level of each collation element
	// as we only implement 'ai_ci' collation, so other levels are ignored directly
	weights []uint16
}

func parseCETHex(input string) (bool, uint32, string) {
	hasValidHex := false
	currentRune := uint32(0)
	end := 0
	for idx, c := range input {
		if reverseHexTable[c] <= 0xf {
			hasValidHex = true

			currentRune <<= 4
			currentRune += uint32(reverseHexTable[c])
			continue
		}

		// invalid character
		end = idx
		break
	}
	return hasValidHex, currentRune, input[end:]
}

func parseCETWeights(input string) (bool, []uint16, string) {
	var left string
	ok := true

	weights := make([]uint16, 0, 1)
outer:
	for {
		left = input

		var (
			weight uint32
		)

		if input[0] != '[' {
			break
		}
		// ignore the dot or star
		if input[1] != '.' && input[1] != '*' {
			ok = false
			break
		}
		input = input[2:]

		ok, weight, input = parseCETHex(input)
		if !ok {
			ok = false
			break
		}
		if weight > math.MaxUint16 {
			ok = false
			break
		}

		// then ignore all weight with higher level
		for {
			if input[0] == '.' {
				ok, _, input = parseCETHex(input[1:])
				if !ok {
					ok = false
					break outer
				}
			} else if input[0] == ']' {
				input = input[1:]
				break
			} else {
				ok = false
				break outer
			}
		}

		weights = append(weights, uint16(weight))
	}

	return ok, weights, left
}

func parseCETEntry(input string) (bool, *cetEntry, string) {
	var (
		ok      bool
		char    uint32
		weights []uint16

		left string
	)
	left = input

	ok, char, input = parseCETHex(input)
	if !ok {
		return false, nil, left
	}
	// then ignore the space and ';'
outer:
	for {
		switch input[0] {
		case ' ':
			input = input[1:]
		case ';':
			input = input[1:]
		default:
			break outer
		}
	}
	// then parse the weights
	ok, weights, input = parseCETWeights(input)
	if !ok {
		return false, nil, left
	}
	return true, &cetEntry{
		char:    rune(char),
		weights: weights,
	}, input
}

type unicodeVersion int

const (
	unicode0400 unicodeVersion = 400
	unicode0900 unicodeVersion = 900
)

type cet struct {
	Name   string
	Length rune

	MapTable4   []uint64
	LongRuneMap map[rune][2]uint64

	URL string

	// explicitRune indicates whether this character is set in the `MapTable/LongRuneMap`.
	explicitRune map[rune]bool
	version      unicodeVersion
}

func (c *cet) insertWeights(char rune, weights []uint16) {
	if char == 0xFDFA {
		// this is a special case, MySQL doesn't handle this character in unicode 4.0.0
		switch c.version {
		case unicode0400:
			return
		case unicode0900:
			weights = weights[:8]
		}
	}

	c.explicitRune[char] = true

	nonZeroWeights := make([]uint16, 0, len(weights))
	for _, w := range weights {
		if w != 0 {
			nonZeroWeights = append(nonZeroWeights, w)
		}
	}

	if len(nonZeroWeights) <= 4 {
		idx := 0
		for _, w := range nonZeroWeights {
			if w != 0 {
				c.MapTable4[char] += uint64(w) << (idx * 16)
				idx++
			}
		}
	} else if len(nonZeroWeights) <= 8 {
		c.MapTable4[char] = LongRune8
		idx := 0
		weight0 := uint64(0)
		for _, w := range nonZeroWeights[:4] {
			if w != 0 {
				weight0 += uint64(w) << (idx * 16)
				idx++
			}
		}
		idx = 0
		weight1 := uint64(0)
		for _, w := range nonZeroWeights[4:] {
			if w != 0 {
				weight1 += uint64(w) << (idx * 16)
				idx++
			}
		}
		c.LongRuneMap[char] = [2]uint64{weight0, weight1}
	} else {
		panic("unreachable")
	}

	// a special value. The `MapTable4` is set to 0xFFFD automatically, but the implementation will get value from `LongRuneMap`
	// and gets 0.
	if c.version == unicode0900 && char == 0xFFFD {
		c.LongRuneMap[char] = [2]uint64{0xFFFD, 0}
	}
}

// parseAllKeys dumps the `MapTable0900` and `LongRuneTable0900` from allkeys0900
// this function actually has the potential to become a generic parser for both allkeys0900
// and the data in `unicode_ci`. TODO: migrate the `unicode_ci_data` to use this parser
func parseAllKeys(input string, length rune, version unicodeVersion) cet {
	cet := cet{
		Length:       length,
		MapTable4:    make([]uint64, length),
		LongRuneMap:  make(map[rune][2]uint64),
		explicitRune: make(map[rune]bool),
		version:      version,
	}

	for {
		var (
			entry *cetEntry
		)
		_, entry, input = parseCETEntry(input)
		if len(input) == 0 {
			break
		}
		if entry != nil && entry.char < length {
			cet.insertWeights(entry.char, entry.weights)
		}
		// just go to the next line
		for {
			if input[0] != '\n' {
				input = input[1:]
				continue
			}

			input = input[1:]
			break
		}
	}

	return cet
}

func (c *cet) calcImplicitWeight() {
	for i := rune(1); i < c.Length; i++ {
		if c.explicitRune[i] {
			continue
		}

		var first, second uint64

		if c.version == unicode0400 {
			first, second = c.getImplicitWeight0400(i)
		} else {
			first, second = c.getImplicitWeight0900(i)
		}
		if second == 0 {
			c.MapTable4[i] = first
		} else {
			c.MapTable4[i] = LongRune8
			c.LongRuneMap[i] = [2]uint64{first, second}
		}
	}
}

func (*cet) getImplicitWeight0400(r rune) (first uint64, second uint64) {
	// Han and other unsigned cases
	first = uint64(r >> 15)
	if r >= 0x3400 && r <= 0x4DB5 {
		first += 0xFB80
	} else if (r >= 0x4E00 && r <= 0x9FA5) || (r >= 0xFA0E && r <= 0xFA0F) {
		first += 0xFB40
	} else {
		first += 0xFBC0
	}

	return first + (uint64((r&0x7FFF)|0x8000) << 16), 0
}

func (c *cet) getImplicitWeight0900(r rune) (first uint64, second uint64) {
	// invalid characters, they are surrogate pair in utf-16, so removed in unicode
	if (r >= 0xD800 && r <= 0xDFFF) || r == 0xFFFD {
		return 0xFFFD, 0
	}

	// handle hangul syllable
	if r >= 0xAC00 && r <= 0xD7AF {
		jamo := decomposeHangulSyllable(r)
		// the length of jamo is 2 or 3, so it will only use a single uint64
		first = uint64(0)
		for idx, j := range jamo {
			// `ucadata.DUCET0900Table.MapTable[j]` should have only one weight
			// test has ensured the jamo has only one weight
			first += (c.MapTable4[j] & 0xFFFF) << (idx * 16)
		}
		return first, 0
	}

	// The implicit weight is always [.AAAA.0020.0002][.BBBB.0000.0000]
	// The calculation process of AAAA and BBBB is according to the UCA
	if r >= 0x17000 && r <= 0x18AFF {
		// Tangut characters
		return 0xFB00 + (uint64((r-0x17000)|0x8000) << 16), 0
	}

	// Nushu and Khitan Small Script were added into unicode in 10.0 and 13.0, so they don't need to be handled
	// specially.

	// Han and other unsigned cases
	first = uint64(r >> 15)
	if (r >= 0x3400 && r <= 0x4DB5) || (r >= 0x20000 && r <= 0x2A6D6) ||
		(r >= 0x2A700 && r <= 0x2B734) || (r >= 0x2B740 && r <= 0x2B81D) ||
		(r >= 0x2B820 && r <= 0x2CEA1) {
		first += 0xFB80
	} else if (r >= 0x4E00 && r <= 0x9FD5) || (r >= 0xFA0E && r <= 0xFA29) {
		first += 0xFB40
	} else {
		first += 0xFBC0
	}

	return first + (uint64((r&0x7FFF)|0x8000) << 16), 0
}

func decomposeHangulSyllable(r rune) []rune {
	const (
		syllableBase     rune = 0xAC00
		leadingJamoBase  rune = 0x1100
		vowelJamoBase    rune = 0x1161
		trailingJamoBase rune = 0x11A7
		vowelJamoCnt     rune = 21
		trailingJamoCnt  rune = 28
	)

	syllableIndex := r - syllableBase
	vtCombination := vowelJamoCnt * trailingJamoCnt
	leadingJamoIndex := syllableIndex / vtCombination
	vowelJamoIndex := (syllableIndex % vtCombination) / trailingJamoCnt
	trailingJamoIndex := syllableIndex % trailingJamoCnt

	result := []rune{leadingJamoBase + leadingJamoIndex, vowelJamoBase + vowelJamoIndex}
	if trailingJamoIndex > 0 {
		result = append(result, trailingJamoBase+trailingJamoIndex)
	}

	return result
}

//go:embed data.go.tpl
var unicodeDataTemplate string

func generateFile(filename string, d *cet) {
	tpl, err := template.New("unicode_template").
		Funcs(template.FuncMap{"mod": func(i, j int) bool { return i%j == 0 }}).
		Parse(unicodeDataTemplate)
	if err != nil {
		panic(err)
	}

	output := bytes.Buffer{}
	err = tpl.Execute(&output, d)
	if err != nil {
		panic(err)
	}
	formattedSource, err := format.Source(output.Bytes())
	if err != nil {
		panic(err)
	}
	err = os.WriteFile(filename, formattedSource, 0666)
	if err != nil {
		panic(err)
	}
}

func main() {
	switch os.Args[len(os.Args)-1] {
	case "unicode_0900_ai_ci_data_generated.go":
		ducet0900Table := parseAllKeys(allkeys0900, 0x2CEA1, unicode0900)
		ducet0900Table.Name = "DUCET0900Table"
		ducet0900Table.URL = "https://www.unicode.org/Public/UCA/9.0.0/allkeys.txt"
		ducet0900Table.calcImplicitWeight()
		generateFile("unicode_0900_ai_ci_data_generated.go", &ducet0900Table)
	case "unicode_ci_data_generated.go":
		// in 4.0.0, only cares the character between 0 and 0xFFFF
		ducet0400Table := parseAllKeys(allkeys0400, 0x10000, unicode0400)
		ducet0400Table.Name = "DUCET0400Table"
		ducet0400Table.URL = "https://www.unicode.org/Public/UCA/4.0.0/allkeys-4.0.0.txt"
		ducet0400Table.calcImplicitWeight()
		generateFile("unicode_ci_data_generated.go", &ducet0400Table)
	default:
		panic("unreachable")
	}
}
