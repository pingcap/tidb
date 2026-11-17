// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package simplesst

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileHeaderV1Zstd(t *testing.T) {
	require.Equal(t, 6, len(fileHeaderV1Zstd))
	require.Equal(t, []byte("TGSC"), fileHeaderV1Zstd[:4])
	require.Equal(t, byte(FileFormatV1), fileHeaderV1Zstd[4])
	require.Equal(t, byte(compressionAlgoZstd), fileHeaderV1Zstd[5])
}

func TestParseFileHeader_V1Zstd(t *testing.T) {
	version, algo, ok := parseFileHeader(fileHeaderV1Zstd)
	require.True(t, ok)
	require.EqualValues(t, FileFormatV1, version)
	require.EqualValues(t, compressionAlgoZstd, algo)
}

func TestParseFileHeader_V0(t *testing.T) {
	// First 6 bytes of a raw v0 data file: high bytes of an 8-byte big-endian
	// keyLen. For any realistic key length (< 6 PB) the first 4 bytes are zero.
	v0 := []byte{0, 0, 0, 0, 0, 0}
	_, _, ok := parseFileHeader(v0)
	require.False(t, ok, "v0 raw data must not be misidentified as a v1 header")
}

func TestParseFileHeader_TooShort(t *testing.T) {
	_, _, ok := parseFileHeader([]byte("TGSC"))
	require.False(t, ok)
}

func TestParseFileHeader_BadAlgo(t *testing.T) {
	bad := []byte{'T', 'G', 'S', 'C', byte(FileFormatV1), 99}
	_, _, ok := parseFileHeader(bad)
	require.False(t, ok)
}
