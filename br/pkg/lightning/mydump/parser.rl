// Copyright 2019 PingCAP, Inc.
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

// Please edit `parser.rl` if you want to modify this file. To generate
// `parser_generated.go`, please execute
//
// ```sh
// make data_parsers
// ```

package mydump

import (
	"io"

	"github.com/pingcap/errors"
)

%%{
#`

# This is a ragel parser to quickly scan through a data source file consisting
# of INSERT statements only. You may find detailed syntax explanation on its
# website <https://www.colm.net/open-source/ragel/>.

machine chunk_parser;

# We treat all unimportant patterns as "comments". This include:
#  - Real SQL comments `/* ... */` and `-- ...`
#  - Whitespace
#  - Separators `,` and `;`
#  - The keyword `INTO` (suffix `i` means case-insensitive).
#  - The parts of the function `CONVERT(` and `USING UTF8MB4)`
#    (to strip the unnecessary detail from mydumper JSON output)
block_comment = '/*' any* :>> '*/';
line_comment = /--[^\r\n]*/;
comment =
	block_comment |
	line_comment |
	space |
	[,;] |
	'convert('i |
	'using utf8mb4)'i;

# The patterns parse quoted strings.
bs = '\\' when { parser.escFlavor != backslashEscapeFlavorNone };

single_quoted = "'" (^"'" | bs any | "''")** "'";
double_quoted = '"' (^'"' | bs any | '""')** '"';
back_quoted = '`' (^'`' | '``')* '`';
unquoted = ^([,;()'"`/*] | space)+;

integer = '-'? [0-9]+;
hex_string = '0x' [0-9a-fA-F]+ | "x'"i [0-9a-fA-F]* "'";
bin_string = '0b' [01]+ | "b'"i [01]* "'";

main := |*
	comment;

	'(' => {
		consumedToken = tokRowBegin
		fbreak;
	};

	')' => {
		consumedToken = tokRowEnd
		fbreak;
	};

	'values'i => {
		consumedToken = tokValues
		fbreak;
	};

	'null'i => {
		consumedToken = tokNull
		fbreak;
	};

	'true'i => {
		consumedToken = tokTrue
		fbreak;
	};

	'false'i => {
		consumedToken = tokFalse
		fbreak;
	};

	integer => {
		consumedToken = tokInteger
		fbreak;
	};

	hex_string => {
		consumedToken = tokHexString
		fbreak;
	};

	bin_string => {
		consumedToken = tokBinString
		fbreak;
	};

	single_quoted => {
		consumedToken = tokSingleQuoted
		fbreak;
	};

	double_quoted => {
		consumedToken = tokDoubleQuoted
		fbreak;
	};

	back_quoted => {
		consumedToken = tokBackQuoted
		fbreak;
	};

	unquoted => {
		consumedToken = tokUnquoted
		fbreak;
	};
*|;

#`
}%%

%% write data;

func (parser *ChunkParser) lex() (token, []byte, error) {
	var cs, ts, te, act, p int
	%% write init;

	for {
		data := parser.buf
		consumedToken := tokNil
		pe := len(data)
		eof := -1
		if parser.isLastChunk {
			eof = pe
		}

		%% write exec;

		if cs == %%{ write error; }%% {
			parser.logSyntaxError()
			return tokNil, nil, errors.New("syntax error")
		}

		if consumedToken != tokNil {
			result := data[ts:te]
			parser.buf = data[te:]
			parser.pos += int64(te)
			return consumedToken, result, nil
		}

		if parser.isLastChunk {
			if te == eof {
				return tokNil, nil, io.EOF
			} else {
				return tokNil, nil, errors.New("syntax error: unexpected EOF")
			}
		}

		parser.buf = parser.buf[ts:]
		parser.pos += int64(ts)
		p -= ts
		te -= ts
		ts = 0
		if err := parser.readBlock(); err != nil {
			return tokNil, nil, errors.Trace(err)
		}
	}

	return tokNil, nil, nil
}
