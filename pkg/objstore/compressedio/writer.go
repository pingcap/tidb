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
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Writer a compressed writer interface.
type Writer interface {
	io.WriteCloser
	Flusher
}

// NewWriter creates a compress writer
func NewWriter(compressType CompressType, w io.Writer) Writer {
	switch compressType {
	case Gzip:
		return gzip.NewWriter(w)
	case Snappy:
		return snappy.NewBufferedWriter(w)
	case Zstd:
		newWriter, err := zstd.NewWriter(w)
		if err != nil {
			log.Warn("Met error when creating new writer for Zstd type file", zap.Error(err))
		}
		return newWriter
	default:
		return nil
	}
}
