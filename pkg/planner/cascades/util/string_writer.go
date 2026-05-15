// Copyright 2024 PingCAP, Inc.
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

package util

import (
	"bufio"
	"io"

	"github.com/pingcap/tidb/pkg/util/intest"
)

// StrBufferWriter is interface facilitate quick writing string regardless of error handling and written len.
type StrBufferWriter interface {
	WriteString(s string)
	Flush()
}

// StrBuffer is a basic wrapper bufio.Writer while override its WriteString func.
type StrBuffer struct {
	bio *bufio.Writer
}

// NewStrBuffer new a defined buffed string writer with passed io.writer.
func NewStrBuffer(w io.Writer) StrBufferWriter {
	return &StrBuffer{
		bio: bufio.NewWriter(w),
	}
}

// WriteString implements IBufStrWriter
func (sw *StrBuffer) WriteString(s string) {
	_, err := sw.bio.WriteString(s)
	intest.Assert(err == nil, "buffer-io WriteString should be no error in test")
}

// Flush implements IBufStrWriter
func (sw *StrBuffer) Flush() {
	err := sw.bio.Flush()
	intest.Assert(err == nil, "buffer-io Flush should be no error in test")
}
