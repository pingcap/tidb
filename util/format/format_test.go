// Copyright 2015 PingCAP, Inc.
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

package format

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func checkFormat(t *testing.T, f Formatter, buf *bytes.Buffer, str, expect string) {
	_, err := f.Format(str, 3)
	assert.Nil(t, err)
	b, err := io.ReadAll(buf)
	assert.Nil(t, err)
	assert.Equal(t, string(b), expect)
}

func TestFormat(t *testing.T) {
	str := "abc%d%%e%i\nx\ny\n%uz\n"
	buf := &bytes.Buffer{}
	f := IndentFormatter(buf, "\t")
	expect := `abc3%e
	x
	y
z
`
	checkFormat(t, f, buf, str, expect)

	str = "abc%d%%e%i\nx\ny\n%uz\n%i\n"
	buf = &bytes.Buffer{}
	f = FlatFormatter(buf)
	expect = "abc3%e x y z\n "
	checkFormat(t, f, buf, str, expect)

	str1 := fmt.Sprintf("%c%c%s%c%c%s", '\'', '\000', "abc", '\n', '\r', "def")
	str2 := OutputFormat(str1)
	assert.Equal(t, str2, "''\\0abc\\n\\rdef")
}
