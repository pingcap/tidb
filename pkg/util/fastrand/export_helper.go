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

package fastrand

// ExportUint32N exports Uint32N function for testing
func ExportUint32N(n uint32) uint32 {
	return Uint32N(n)
}

// ExportUint64N exports Uint64N function for testing
func ExportUint64N(n uint64) uint64 {
	return Uint64N(n)
}

// ExportBuf exports Buf function for testing
func ExportBuf(size int) []byte {
	return Buf(size)
}

// ExportUint32 exports Uint32 function for testing
func ExportUint32() uint32 {
	return Uint32()
}
