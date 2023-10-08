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

package column

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResultEncoder(t *testing.T) {
	// Encode bytes to utf-8.
	d := NewResultEncoder("utf-8")
	src := []byte("test_string")
	result := d.EncodeMeta(src)
	require.Equal(t, src, result)

	// Encode bytes to GBK.
	d = NewResultEncoder("gbk")
	result = d.EncodeMeta([]byte("一"))
	require.Equal(t, []byte{0xd2, 0xbb}, result)

	// Encode bytes to binary.
	d = NewResultEncoder("binary")
	result = d.EncodeMeta([]byte("一"))
	require.Equal(t, "一", string(result))
}
