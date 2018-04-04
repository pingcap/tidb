// Copyright 2017 PingCAP, Inc.
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

package protocol

// MySQLX Column Bytes Content Types
const (
	ContentTypeGeometry uint32 = 0x0001 // GEOMETRY (WKB encoding)
	ContentTypeJSON     uint32 = 0x0002 // JSON (text encoding)
	ContentTypeXML      uint32 = 0x0003 // XML (text encoding)
)

// MySQLX Column Flags
const (
	FlagUintZeroFill      uint32 = 0x0001 // UINT zerofill
	FlagDoubleUnsigned    uint32 = 0x0001 // DOUBLE 0x0001 unsigned
	FlagFloatUnsigned     uint32 = 0x0001 // FLOAT  0x0001 unsigned
	FlagDecimalUnsigned   uint32 = 0x0001 // DECIMAL 0x0001 unsigned
	FlagBytesRightpad     uint32 = 0x0001 // BYTES  0x0001 rightpad
	FlagDatetimeTimestamp uint32 = 0x0001 // DATETIME 0x0001 timestamp
	FlagNotNull           uint32 = 0x0010
	FlagPrimaryKey        uint32 = 0x0020
	FlagUniqueKey         uint32 = 0x0040
	FlagMultipleKey       uint32 = 0x0080
	FlagAutoIncrement     uint32 = 0x0100
)
