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

type spillSerializeHelper struct {
	tmpBuf []byte
}

func newSpillSerializeHelper(typeLen int) spillSerializeHelper {
	if typeLen == varLenFlag {
		return spillSerializeHelper{}
	}
	return spillSerializeHelper{
		tmpBuf: make([]byte, typeLen),
	}
}

func (s *spillSerializeHelper) serializeBool(value bool, buf []byte) []byte {
	return spill.SerializeBool(value, buf, s.tmpBuf)
}

func (s *spillSerializeHelper) serializeInt8(value int8, buf []byte) []byte {
	return spill.SerializeInt8(value, buf, s.tmpBuf)
}

func (s *spillSerializeHelper) serializeInt32(value int32, buf []byte) []byte {
	return spill.SerializeInt32(value, buf, s.tmpBuf)
}

func (s *spillSerializeHelper) serializeUint32(value uint32, buf []byte) []byte {
	return spill.SerializeUint32(value, buf, s.tmpBuf)
}

func (s *spillSerializeHelper) serializeUint64(value uint64, buf []byte) []byte {
	return spill.SerializeUint64(value, buf, s.tmpBuf)
}

func (s *spillSerializeHelper) serializeInt64(value int64, buf []byte) []byte {
	return spill.SerializeInt64(value, buf, s.tmpBuf)
}

func (s *spillSerializeHelper) serializeFloat64(value float64, buf []byte) []byte {
	return spill.SerializeFloat64(value, buf, s.tmpBuf)
}

// TODO if DefRowSize and DefInterfaceSize need to be serialized?
