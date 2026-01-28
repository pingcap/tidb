// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compressedio

import "github.com/pingcap/errors"

// CompressType represents the type of compression.
type CompressType uint8

const (
	// NoCompression won't compress given bytes.
	NoCompression CompressType = iota
	// Gzip will compress given bytes in gzip format.
	Gzip
	// Snappy will compress given bytes in snappy format.
	Snappy
	// Zstd will compress given bytes in zstd format.
	Zstd
)

// FileSuffix returns the file suffix for the compress type.
func (ct CompressType) FileSuffix() string {
	switch ct {
	case NoCompression:
		return ""
	case Gzip:
		return ".gz"
	case Snappy:
		return ".snappy"
	case Zstd:
		return ".zst"
	default:
		return ""
	}
}

// Flusher flush interface.
type Flusher interface {
	Flush() error
}

// DecompressConfig is the config used for decompression.
type DecompressConfig struct {
	// ZStdDecodeConcurrency only used for ZStd decompress, see WithDecoderConcurrency.
	// if not 1, ZStd will decode file asynchronously.
	ZStdDecodeConcurrency int
}

// ParseCompressType parses compressType string to storage.CompressType
func ParseCompressType(compressType string) (CompressType, error) {
	switch compressType {
	case "", "no-compression":
		return NoCompression, nil
	case "gzip", "gz":
		return Gzip, nil
	case "snappy":
		return Snappy, nil
	case "zstd", "zst":
		return Zstd, nil
	default:
		return NoCompression, errors.Errorf("unknown compress type %s", compressType)
	}
}
