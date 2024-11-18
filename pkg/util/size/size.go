// Copyright 2022 PingCAP, Inc.
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

package size

import "unsafe"

const (
	// KB is the kilobytes.
	KB = uint64(1024)
	// MB is the megabytes.
	MB = KB * 1024
	// GB is the gigabytes.
	GB = MB * 1024
	// TB is the terabytes.
	TB = GB * 1024
	// PB is the petabytes.
	PB = TB * 1024
)

// below constants are the size of commonly used types, for memory trace
const (
	// SizeOfSlice is the memory itself used, excludes the elements' memory
	SizeOfSlice = int64(unsafe.Sizeof(*new([]int)))

	// SizeOfByte is the memory each byte occupied
	SizeOfByte = int64(unsafe.Sizeof(*new(byte)))

	// SizeOfString is the memory string itself occupied
	SizeOfString = int64(unsafe.Sizeof(*new(string)))

	// SizeOfBool is the memory each bool occupied
	SizeOfBool = int64(unsafe.Sizeof(*new(bool)))

	// SizeOfPointer is the memory each pointer occupied
	SizeOfPointer = int64(unsafe.Sizeof(new(int)))

	// SizeOfInterface is the memory each interface occupied, exclude the real type memory usage
	SizeOfInterface = int64(unsafe.Sizeof(*new(any)))

	// SizeOfFloat64 is the memory each float64 occupied
	SizeOfFloat64 = int64(unsafe.Sizeof(*new(float64)))

	// SizeOfUint64 is the memory each uint64 occupied
	SizeOfUint64 = int64(unsafe.Sizeof(*new(uint64)))

	// SizeOfInt32 is the memory each int32 occupied
	SizeOfInt32 = int64(unsafe.Sizeof(*new(int32)))

	// SizeOfInt is the memory each int occupied
	SizeOfInt = int64(unsafe.Sizeof(*new(int)))

	// SizeOfUint8 is the memory each uint8 occupied
	SizeOfUint8 = int64(unsafe.Sizeof(*new(uint8)))

	// SizeOfUint is the memory each uint occupied
	SizeOfUint = int64(unsafe.Sizeof(*new(uint)))

	// SizeOfFunc is the memory each function occupied
	SizeOfFunc = int64(unsafe.Sizeof(*new(func())))

	// SizeOfInt64 is the memory each int64 occupied
	SizeOfInt64 = int64(unsafe.Sizeof(*new(int64)))

	// SizeOfMap is the memory each map itself occupied
	SizeOfMap = int64(unsafe.Sizeof(*new(map[int]int)))
)
