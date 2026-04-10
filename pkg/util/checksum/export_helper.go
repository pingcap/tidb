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

package checksum

import (
	"io"
)

// Exported types for testing

// ExportedWriter exports Writer for testing
type ExportedWriter = Writer

// ExportedReader exports Reader for testing
type ExportedReader = Reader

// ExportedNewWriter creates a new Writer for testing
func ExportedNewWriter(w io.WriteCloser) *Writer {
	return NewWriter(w)
}

// ExportedNewReader creates a new Reader for testing
func ExportedNewReader(r io.ReaderAt) *Reader {
	return NewReader(r)
}

// ExportedErrChecksumFail gets the errChecksumFail for testing
func ExportedErrChecksumFail() error {
	return errChecksumFail
}

// ExportedSetErrChecksumFail sets the errChecksumFail for testing
func ExportedSetErrChecksumFail(err error) {
	errChecksumFail = err
}
