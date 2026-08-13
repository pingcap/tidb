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

import (
	"io"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

// NewReader read compressed data.
// only for test now.
func NewReader(compressType CompressType, cfg DecompressConfig, r io.Reader) (io.Reader, error) {
	switch compressType {
	case Gzip:
		return gzip.NewReader(r)
	case Snappy:
		return snappy.NewReader(r), nil
	case Zstd:
		options := []zstd.DOption{}
		if cfg.ZStdDecodeConcurrency > 0 {
			options = append(options, zstd.WithDecoderConcurrency(cfg.ZStdDecodeConcurrency))
		}
		return zstd.NewReader(r, options...)
	default:
		return nil, nil
	}
}
