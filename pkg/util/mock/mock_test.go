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

package mock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type contextKeyType int

func (k contextKeyType) String() string {
	return "mock_key"
}

const contextKey contextKeyType = 0

func TestContext(t *testing.T) {
	ctx := NewContext()

	ctx.SetValue(contextKey, 1)
	v := ctx.Value(contextKey)
	assert.Equal(t, 1, v)

	ctx.ClearValue(contextKey)
	v = ctx.Value(contextKey)
	assert.Nil(t, v)
}

func BenchmarkNewContext(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		NewContext()
	}
}
