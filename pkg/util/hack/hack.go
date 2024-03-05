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
	// In go1.21, the load factor num becomes 12 and go team has decided not to backport the fix to 1.21.
	// See more details in https://github.com/golang/go/issues/63438
	if strings.Contains(runtime.Version(), `go1.21`) {
		LoadFactorNum = 12
	}
}

const (
	// DefBucketMemoryUsageForMapStrToSlice = bucketSize*(1+unsafe.Sizeof(string) + unsafe.Sizeof(slice))+2*ptrSize
	// ref https://github.com/golang/go/blob/go1.15.6/src/reflect/type.go#L2162.
	// The bucket size may be changed by golang implement in the future.
	// Golang Map needs to acquire double the memory when expanding,
	// and the old buckets will be released after the data is migrated.
	// Considering the worst case, the data in the old bucket cannot be migrated in time, and the old bucket cannot
	// be GCed, we expand the bucket size to 1.5 times to estimate the memory usage of Golang Map.
	DefBucketMemoryUsageForMapStrToSlice = (8*(1+16+24) + 16) / 2 * 3
	// DefBucketMemoryUsageForMapIntToPtr = bucketSize*(1+unsafe.Sizeof(uint64) + unsafe.Sizeof(pointer))+2*ptrSize
	DefBucketMemoryUsageForMapIntToPtr = (8*(1+8+8) + 16) / 2 * 3
	// DefBucketMemoryUsageForMapStringToAny = bucketSize*(1+unsafe.Sizeof(string) + unsafe.Sizeof(interface{}))+2*ptrSize
	DefBucketMemoryUsageForMapStringToAny = (8*(1+16+16) + 16) / 2 * 3
	// DefBucketMemoryUsageForSetString = bucketSize*(1+unsafe.Sizeof(string) + unsafe.Sizeof(struct{}))+2*ptrSize
	DefBucketMemoryUsageForSetString = (8*(1+16+0) + 16) / 2 * 3
	// DefBucketMemoryUsageForSetFloat64 = bucketSize*(1+unsafe.Sizeof(float64) + unsafe.Sizeof(struct{}))+2*ptrSize
	DefBucketMemoryUsageForSetFloat64 = (8*(1+8+0) + 16) / 2 * 3
	// DefBucketMemoryUsageForSetInt64 = bucketSize*(1+unsafe.Sizeof(int64) + unsafe.Sizeof(struct{}))+2*ptrSize
	DefBucketMemoryUsageForSetInt64 = (8*(1+8+0) + 16) / 2 * 3
)

// EstimateBucketMemoryUsage returns the estimated memory usage of a bucket in a map.
func EstimateBucketMemoryUsage[K comparable, V any]() uint64 {
	return (8*(1+uint64(unsafe.Sizeof(*new(K))+unsafe.Sizeof(*new(V)))) + 16) / 2 * 3
}
