// Copyright 2026 PingCAP, Inc.
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

package mydump

import (
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/stretchr/testify/require"
)

// TestBlockParserReadBlockKeepsRemainder guards the buffer handling in
// readBlock. The unparsed remainder (parser.buf) is a sub-slice of appendBuf's
// backing array, so readBlock compacts it to the front and then appends the
// newly read block into the same buffer. bytes.Buffer does not document
// behavior when the written slice overlaps the buffer's own storage, so pin
// the result here to keep a later change from silently corrupting a row that
// spans several blocks.
func TestBlockParserReadBlockKeepsRemainder(t *testing.T) {
	const content = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const blockBufSize = 8 // the actual block buffer is blockBufSize*config.BufferSizeScale

	parser := makeBlockParser(NewStringReader(content), blockBufSize, nil, nil, log.L(), 0)
	n := len(parser.blockBuf)
	require.Greater(t, len(content), n, "content must span more than one read")

	require.NoError(t, parser.readBlock())
	require.Equal(t, content[:n], string(parser.buf))

	// Simulate the lexer consuming the first 10 bytes: parser.buf now aliases the
	// middle of appendBuf's backing array, so the next readBlock has to move the
	// remainder back to the front before appending the new block.
	parser.buf = parser.buf[10:]
	require.NoError(t, parser.readBlock())
	require.Equal(t, content[10:], string(parser.buf))
}
