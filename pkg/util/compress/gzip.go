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

package compress

import (
	"io"
	"sync"

	"github.com/klauspost/compress/gzip"
)

// GzipWriterPool is a sync.Pool of gzip.Writer.
var GzipWriterPool = sync.Pool{
	New: func() any {
		return gzip.NewWriter(io.Discard)
	},
}

// GzipReaderPool is a sync.Pool of gzip.Reader.
var GzipReaderPool = sync.Pool{
	New: func() any {
		return &gzip.Reader{}
	},
}
