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

package hack

import (
	"runtime"
	"strings"
	"unsafe"
)

// MutableString can be used as string via string(MutableString) without performance loss.
type MutableString string

// String converts slice to MutableString without copy.
// The MutableString can be converts to string without copy.
// Use it at your own risk.
func String(b []byte) MutableString {
	if len(b) == 0 {
		return ""
	}
	return MutableString(unsafe.String(unsafe.SliceData(b), len(b)))
}

// Slice converts string to slice without copy.
// Use at your own risk.
func Slice(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// LoadFactor is the maximum average load of a bucket that triggers growth is 6.5 in Golang Map.
// Represent as LoadFactorNum/LoadFactorDen, to allow integer math.
// They are from the golang definition. ref: https://github.com/golang/go/blob/go1.13.15/src/runtime/map.go#L68-L71
const (
	// LoadFactorDen is the denominator of load factor
	LoadFactorDen = 2
)

// LoadFactorNum is the numerator of load factor
var LoadFactorNum = 13

func init() {
	checkMapABI()
	// In go1.21, the load factor num becomes 12 and go team has decided not to backport the fix to 1.21.
	// See more details in https://github.com/golang/go/issues/63438
	if strings.Contains(runtime.Version(), `go1.21`) || strings.Contains(runtime.Version(), `go1.22`) {
		LoadFactorNum = 12
	}
}

// GetBytesFromPtr return a bytes array from the given ptr and length
func GetBytesFromPtr(ptr unsafe.Pointer, length int) []byte {
	return unsafe.Slice((*byte)(ptr), length)
}

// Memory usage constants for swiss map
const (
	DefBucketMemoryUsageForMapStringToAny = 312
	DefBucketMemoryUsageForSetString      = 248
	DefBucketMemoryUsageForSetFloat64     = 184
	DefBucketMemoryUsageForSetInt64       = 184
)
