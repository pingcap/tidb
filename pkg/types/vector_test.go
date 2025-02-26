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

package types_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestVectorEndianess(t *testing.T) {
	// Note: This test fails in BigEndian machines.
	v := types.InitVectorFloat32(2)
	vv := v.Elements()
	vv[0] = 1.1
	vv[1] = 2.2
	require.Equal(t, []byte{
		/* Length = 0x02 */
		0x02, 0x00, 0x00, 0x00,
		/* Element 1 = 0x3f8ccccd */
		0xcd, 0xcc, 0x8c, 0x3f,
		/* Element 2 = 0x400ccccd */
		0xcd, 0xcc, 0x0c, 0x40,
	}, v.SerializeTo(nil))
}

func TestZeroVector(t *testing.T) {
	require.True(t, types.ZeroVectorFloat32.IsZeroValue())
	require.Equal(t, 0, types.ZeroVectorFloat32.Compare(types.ZeroVectorFloat32))

	require.Equal(t, []byte{0, 0, 0, 0}, types.ZeroVectorFloat32.ZeroCopySerialize())
	require.Equal(t, 4, types.ZeroVectorFloat32.SerializedSize())
	require.Equal(t, []byte{0, 0, 0, 0}, types.ZeroVectorFloat32.SerializeTo(nil))
	require.Equal(t, []byte{1, 2, 3, 0, 0, 0, 0}, types.ZeroVectorFloat32.SerializeTo([]byte{1, 2, 3}))

	v, remaining, err := types.ZeroCopyDeserializeVectorFloat32([]byte{0, 0, 0, 0})
	require.Nil(t, err)
	require.Len(t, remaining, 0)
	require.Equal(t, 0, v.Len())
	require.Equal(t, "[]", v.String())
	require.True(t, v.IsZeroValue())
	require.Equal(t, 0, v.Compare(types.ZeroVectorFloat32))
	require.Equal(t, 0, types.ZeroVectorFloat32.Compare(v))
}

func TestVectorParse(t *testing.T) {
	v, err := types.ParseVectorFloat32(`abc`)
	require.NotNil(t, err)
	require.True(t, v.IsZeroValue())

	// Note: Currently we will parse "null" into [].
	v, err = types.ParseVectorFloat32(`null`)
	require.Nil(t, err)
	require.True(t, v.IsZeroValue())

	v, err = types.ParseVectorFloat32(`"json_str"`)
	require.NotNil(t, err)
	require.True(t, v.IsZeroValue())

	v, err = types.ParseVectorFloat32(`123`)
	require.NotNil(t, err)
	require.True(t, v.IsZeroValue())

	v, err = types.ParseVectorFloat32(`[123`)
	require.NotNil(t, err)
	require.True(t, v.IsZeroValue())

	v, err = types.ParseVectorFloat32(`123]`)
	require.NotNil(t, err)
	require.True(t, v.IsZeroValue())

	v, err = types.ParseVectorFloat32(`[123,]`)
	require.NotNil(t, err)
	require.True(t, v.IsZeroValue())

	v, err = types.ParseVectorFloat32(`[]`)
	require.Nil(t, err)
	require.Equal(t, 0, v.Len())
	require.Equal(t, "[]", v.String())
	require.True(t, v.IsZeroValue())
	require.Equal(t, 0, v.Compare(types.ZeroVectorFloat32))
	require.Equal(t, 0, types.ZeroVectorFloat32.Compare(v))

	v, err = types.ParseVectorFloat32(`[1.1, 2.2, 3.3]`)
	require.Nil(t, err)
	require.Equal(t, 3, v.Len())
	require.Equal(t, "[1.1,2.2,3.3]", v.String())
	require.False(t, v.IsZeroValue())
	require.Equal(t, 1, v.Compare(types.ZeroVectorFloat32))
	require.Equal(t, -1, types.ZeroVectorFloat32.Compare(v))

	v, err = types.ParseVectorFloat32(`[-1e39, 1e39]`)
	require.EqualError(t, err, "value -1e+39 out of range for float32")
}

func TestVectorDatum(t *testing.T) {
	d := types.NewDatum(nil)
	d.SetVectorFloat32(types.ZeroVectorFloat32)
	v := d.GetVectorFloat32()
	require.Equal(t, 0, v.Len())
	require.Equal(t, "[]", v.String())
	require.True(t, v.IsZeroValue())
	require.Equal(t, 0, v.Compare(types.ZeroVectorFloat32))
	require.Equal(t, 0, types.ZeroVectorFloat32.Compare(v))
}

func TestVectorCompare(t *testing.T) {
	v1, err := types.ParseVectorFloat32(`[1.1, 2.2, 3.3]`)
	require.NoError(t, err)
	v2, err := types.ParseVectorFloat32(`[-1.1, 4.2]`)
	require.NoError(t, err)

	require.Equal(t, 1, v1.Compare(v2))
	require.Equal(t, -1, v2.Compare(v1))

	v1, err = types.ParseVectorFloat32(`[1.1, 2.2, 3.3]`)
	require.NoError(t, err)
	v2, err = types.ParseVectorFloat32(`[1.1, 4.2]`)
	require.NoError(t, err)

	require.Equal(t, -1, v1.Compare(v2))
	require.Equal(t, 1, v2.Compare(v1))
}

func TestVectorSerialize(t *testing.T) {
	v1, err := types.ParseVectorFloat32(`[1.1, 2.2, 3.3]`)
	require.NoError(t, err)

	serialized := v1.SerializeTo(nil)
	serialized = append(serialized, 0x01, 0x02, 0x03, 0x04)

	v2, remaining, err := types.ZeroCopyDeserializeVectorFloat32(serialized)
	require.NoError(t, err)
	require.Len(t, remaining, 4)
	require.Equal(t, []byte{
		0x01, 0x02, 0x03, 0x04,
	}, remaining)

	require.Equal(t, "[1.1,2.2,3.3]", v2.String())

	v3, remaining, err := types.ZeroCopyDeserializeVectorFloat32([]byte{0xF1, 0xFC})
	require.Error(t, err)
	require.Len(t, remaining, 2)
	require.Equal(t, []byte{
		0xF1, 0xFC,
	}, remaining)
	require.True(t, v3.IsZeroValue())
}
