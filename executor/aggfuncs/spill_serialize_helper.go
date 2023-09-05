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

import "github.com/pingcap/tidb/util/spill"

type SpillSerializeHelper struct {
	tmpBuf []byte
}

func newSpillSerializeHelper(typeLen int) SpillSerializeHelper {
	if typeLen == varLenFlag {
		return SpillSerializeHelper{}
	}
	return SpillSerializeHelper{
		tmpBuf: make([]byte, typeLen),
	}
}

func (s *SpillSerializeHelper) serializeBool(value bool) []byte {
	return spill.SerializeBool(value, s.tmpBuf)
}

func (s *SpillSerializeHelper) serializeInt8(value int8) []byte {
	return spill.SerializeInt8(value, s.tmpBuf)
}

func (s *SpillSerializeHelper) serializeInt32(value int32) []byte {
	return spill.SerializeInt32(value, s.tmpBuf)
}

func (s *SpillSerializeHelper) serializeUint32(value uint32) []byte {
	return spill.SerializeUint32(value, s.tmpBuf)
}

func (s *SpillSerializeHelper) serializeUint64(value uint64) []byte {
	return spill.SerializeUint64(value, s.tmpBuf)
}

func (s *SpillSerializeHelper) serializeInt64(value int64) []byte {
	return spill.SerializeInt64(value, s.tmpBuf)
}

func (s *SpillSerializeHelper) serializeFloat64(value float64) []byte {
	return spill.SerializeFloat64(value, s.tmpBuf)
}

// TODO if DefRowSize and DefInterfaceSize need to be serialized?
