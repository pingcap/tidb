//go:build cgo

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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
)

func TestUDFArgBufferReuse(t *testing.T) {
	tracker := memory.NewTracker(-1, -1)
	buf := udfArgBuffer{tracker: tracker}
	t.Cleanup(buf.free)

	ptr, length, err := buf.writeDatum(nil, types.NewIntDatum(42), types.ETInt)
	require.NoError(t, err)
	require.NotNil(t, ptr)
	require.Zero(t, int(length))
	firstPtr := ptr
	firstCapacity := buf.capacity

	ptr, _, err = buf.writeDatum(nil, types.NewIntDatum(43), types.ETInt)
	require.NoError(t, err)
	require.Equal(t, firstPtr, ptr)
	require.Equal(t, firstCapacity, buf.capacity)
	require.Equal(t, int64(firstCapacity), tracker.BytesConsumed())

	ptr, length, err = buf.writeDatum(nil, types.NewStringDatum("abc"), types.ETString)
	require.NoError(t, err)
	require.Equal(t, firstPtr, ptr)
	require.Equal(t, 3, int(length))
	require.Equal(t, []byte{'a', 'b', 'c', 0}, unsafe.Slice((*byte)(ptr), 4))

	longValue := "a string that forces the reusable C buffer to grow"
	ptr, length, err = buf.writeDatum(nil, types.NewStringDatum(longValue), types.ETString)
	require.NoError(t, err)
	require.Equal(t, len(longValue), int(length))
	require.GreaterOrEqual(t, buf.capacity, len(longValue)+1)
	require.Equal(t, append([]byte(longValue), 0), unsafe.Slice((*byte)(ptr), len(longValue)+1))

	retainedPtr := buf.ptr
	ptr, length, err = buf.writeDatum(nil, types.Datum{}, types.ETString)
	require.NoError(t, err)
	require.Nil(t, ptr)
	require.Zero(t, int(length))
	require.Equal(t, retainedPtr, buf.ptr)

	largeValue := make([]byte, maxReusableUDFArgBufferSize)
	ptr, length, err = buf.writeDatum(nil, types.NewBytesDatum(largeValue), types.ETString)
	require.NoError(t, err)
	require.Equal(t, len(largeValue), int(length))
	require.NotEqual(t, retainedPtr, ptr)
	require.Equal(t, retainedPtr, buf.ptr)
	require.Equal(t, ptr, buf.temporary)
	require.Equal(t, int64(buf.capacity+len(largeValue)+1), tracker.BytesConsumed())

	buf.releaseTemporary()
	require.Nil(t, buf.temporary)
	require.Equal(t, retainedPtr, buf.ptr)
	require.Equal(t, int64(buf.capacity), tracker.BytesConsumed())

	buf.free()
	require.Zero(t, tracker.BytesConsumed())
}
