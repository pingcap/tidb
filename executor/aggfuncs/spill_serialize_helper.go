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

package aggfuncs

import (
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/spill"
)

type SpillSerializeHelper struct {
	// tmpBuf is an auxiliary data struct that used for encoding bytes.
	// 1024 is large enough for all fixed length data struct.
	tmpBuf [1024]byte
}

func (s *SpillSerializeHelper) serializeBool(value bool) []byte {
	return spill.SerializeBool(value, s.tmpBuf[0:boolLen])
}

func (s *SpillSerializeHelper) serializeInt8(value int8) []byte {
	return spill.SerializeInt8(value, s.tmpBuf[0:int8Len])
}

func (s *SpillSerializeHelper) serializeInt32(value int32) []byte {
	return spill.SerializeInt32(value, s.tmpBuf[0:int32Len])
}

func (s *SpillSerializeHelper) serializeUint32(value uint32) []byte {
	return spill.SerializeUint32(value, s.tmpBuf[0:uint32Len])
}

func (s *SpillSerializeHelper) serializeUint64(value uint64) []byte {
	return spill.SerializeUint64(value, s.tmpBuf[0:uint64Len])
}

func (s *SpillSerializeHelper) serializeInt64(value int64) []byte {
	return spill.SerializeInt64(value, s.tmpBuf[0:int64Len])
}

func (s *SpillSerializeHelper) serializeFloat32(value float32) []byte {
	return spill.SerializeFloat32(value, s.tmpBuf[0:float32Len])
}

func (s *SpillSerializeHelper) serializeFloat64(value float64) []byte {
	return spill.SerializeFloat64(value, s.tmpBuf[0:float64Len])
}

// TODO if DefRowSize and DefInterfaceSize need to be serialized?
