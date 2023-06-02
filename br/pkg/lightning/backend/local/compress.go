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

package local

import (
	"io"
	"sync"

	"github.com/klauspost/compress/gzip" // faster than stdlib
	"google.golang.org/grpc"
)

var (
	_ grpc.Compressor   = (*gzipCompressor)(nil)
	_ grpc.Decompressor = (*gzipDecompressor)(nil)
)

type gzipCompressor struct{}

var gzipWriterPool = sync.Pool{
	New: func() any {
		return gzip.NewWriter(io.Discard)
	},
}

func (*gzipCompressor) Do(w io.Writer, p []byte) error {
	z := gzipWriterPool.Get().(*gzip.Writer)
	defer gzipWriterPool.Put(z)
	z.Reset(w)
	if _, err := z.Write(p); err != nil {
		return err
	}
	return z.Close()
}

func (*gzipCompressor) Type() string {
	return "gzip"
}

type gzipDecompressor struct{}

var gzipReaderPool = sync.Pool{
	New: func() any {
		return &gzip.Reader{}
	},
}

func (*gzipDecompressor) Do(r io.Reader) ([]byte, error) {
	z := gzipReaderPool.Get().(*gzip.Reader)
	if err := z.Reset(r); err != nil {
		gzipReaderPool.Put(z)
		return nil, err
	}

	defer func() {
		_ = z.Close()
		gzipReaderPool.Put(z)
	}()
	return io.ReadAll(z)
}

func (*gzipDecompressor) Type() string {
	return "gzip"
}
