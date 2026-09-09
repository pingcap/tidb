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

package serialization

import (
	"testing"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestVectorFloat32SerializationRoundTrip(t *testing.T) {
	values := []types.VectorFloat32{
		types.ZeroVectorFloat32,
		types.MustCreateVectorFloat32([]float32{1.25, -2.5, 0}),
	}
	for _, expected := range values {
		encoded := SerializeVectorFloat32(expected, nil)
		posAndBuf := &PosAndBuf{Buf: encoded}
		actual := DeserializeVectorFloat32(posAndBuf)

		require.Equal(t, expected.Len(), actual.Len())
		require.Equal(t, expected.Elements(), actual.Elements())
		require.Equal(t, expected.ZeroCopySerialize(), actual.ZeroCopySerialize())
		require.Equal(t, int64(len(encoded)), posAndBuf.Pos)
	}
}
