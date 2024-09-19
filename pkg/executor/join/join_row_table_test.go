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

package join

import (
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestHeapObjectCanMove(t *testing.T) {
	require.Equal(t, false, heapObjectsCanMove())
}

func TestFixedOffsetInRowLayout(t *testing.T) {
	require.Equal(t, 8, sizeOfNextPtr)
	require.Equal(t, 8, sizeOfLengthField)
}

func TestBitMaskInUint32(t *testing.T) {
	testData := make([]byte, 4)
	for i := 0; i < 32; i++ {
		testData[i/8] = 1 << (7 - i%8)
		testUint32 := atomic.LoadUint32((*uint32)(unsafe.Pointer(&testData[0])))
		ref := testUint32 & bitMaskInUint32[i]
		require.Equal(t, true, ref != 0)
		testData[i/8] = 0
	}
}

func TestUintptrCanHoldPointer(t *testing.T) {
	require.Equal(t, true, sizeOfUintptr >= sizeOfUnsafePointer)
}
