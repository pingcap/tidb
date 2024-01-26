// Copyright 2023 PingCAP, Inc.
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
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/types"
)

// These types are used for serializing or deserializing interface type
const (
	BoolType = iota
	Int64Type
	Uint64Type
	FloatType
	StringType
	BinaryJSONType
	OpaqueType
	TimeType
	DurationType

	InterfaceTypeCodeLen = int64(1)
	JSONTypeCodeLen      = int64(types.JSONTypeCode(1))
	BoolLen              = int64(unsafe.Sizeof(true))
	ByteLen              = int64(unsafe.Sizeof(byte(0)))
	Int8Len              = int64(unsafe.Sizeof(int8(0)))
	Uint8Len             = int64(unsafe.Sizeof(uint8(0)))
	IntLen               = int64(unsafe.Sizeof(int(0)))
	Int32Len             = int64(unsafe.Sizeof(int32(0)))
	Uint32Len            = int64(unsafe.Sizeof(uint32(0)))
	Int64Len             = int64(unsafe.Sizeof(int64(0)))
	Uint64Len            = int64(unsafe.Sizeof(uint64(0)))
	Float32Len           = int64(unsafe.Sizeof(float32(0)))
	Float64Len           = int64(unsafe.Sizeof(float64(0)))
	TimeLen              = int64(unsafe.Sizeof(types.Time{}))
	TimeDurationLen      = int64(unsafe.Sizeof(time.Duration(0)))
)
