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

package pkdbspfreshbulk

import (
	"encoding/binary"
	"unsafe"
)

func init() {
	// This is the same platform contract as types.VectorFloat32. Never
	// silently encode native big-endian bytes as the artifact's little endian.
	if binary.NativeEndian.Uint16([]byte{1, 0}) != 1 {
		panic("SPFresh vectors only support little endian")
	}
}

// float32LittleEndianBytes borrows the exact f32 bits without encoding them a
// second time. Callers must consume the view synchronously and never mutate it;
// the hash writer and fragment copy do not retain this slice.
func float32LittleEndianBytes(vector []float32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(vector))), len(vector)*4)
}
