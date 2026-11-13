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

// File-level format markers for v1 (compressed) global-sort intermediates.
//
// A v1 data file and a v1 stat file both begin with a 6-byte header:
//
//	offset 0..3 :  magic "TGSC"  (TiDB Global Sort Compressed)
//	offset 4    :  format version (uint8)
//	offset 5    :  compression algorithm (uint8)
//
// Files without this header are treated as v0 (raw, uncompressed) for
// backward compatibility. The first 8 bytes of a raw file are a big-endian
// uint64 key length, so any realistic key length leaves the first 4 bytes
// as 0x00 0x00 0x00 0x00, which can never collide with the ASCII magic.
const (
	FileFormatV0 uint8 = 0
	FileFormatV1 uint8 = 1

	compressionAlgoNone uint8 = 0
	compressionAlgoZstd uint8 = 1

	FileHeaderLen = 6
)

var fileMagic = [4]byte{'T', 'G', 'S', 'C'}

// fileHeaderV1Zstd is the canonical header for v1 zstd-compressed files.
// It is allocated once at package init so writers can write it as a slice
// without re-allocating.
var fileHeaderV1Zstd = []byte{
	'T', 'G', 'S', 'C',
	FileFormatV1,
	compressionAlgoZstd,
}

// parseFileHeader inspects the first FileHeaderLen bytes of a file. It
// returns (version, algo, true) if the bytes are a valid v1 header for a
// known algorithm, and (_, _, false) otherwise. Callers must distinguish absent magic from unsupported
// compressed headers before falling back to v0.
func parseFileHeader(b []byte) (version, algo uint8, ok bool) {
	if len(b) < FileHeaderLen {
		return 0, 0, false
	}
	if b[0] != fileMagic[0] || b[1] != fileMagic[1] || b[2] != fileMagic[2] || b[3] != fileMagic[3] {
		return 0, 0, false
	}
	version = b[4]
	algo = b[5]
	switch algo {
	case compressionAlgoZstd:
		// known algo
	default:
		return 0, 0, false
	}
	if version != FileFormatV1 {
		return 0, 0, false
	}
	return version, algo, true
}
