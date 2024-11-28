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

type IBufStrWriter interface {
	WriteString(s string)
	Flush()
}

type BufStrWriter struct {
	bio *bufio.Writer
}

// NewBufStrWriter new a defined buffed string writer with passed io.writer.
func NewBufStrWriter(w io.Writer) IBufStrWriter {
	return &BufStrWriter{
		bio: bufio.NewWriter(w),
	}
}

func (sw *BufStrWriter) WriteString(s string) {
	_, err := sw.bio.WriteString(s)
	intest.Assert(err == nil, "WriteString should be no error in test")
}

func (sw *BufStrWriter) Flush() {
	err := sw.bio.Flush()
	intest.Assert(err == nil, "buffer-io flush should be no error in test")
}
