// Go driver for MySQL X Protocol
//
// Copyright 2016 Simon J Mudd.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.
//
// MySQL X protocol authentication using MYSQL41 method

package mysql

import (
	"encoding/binary"
	"fmt"
	"math"

	"database/sql/driver"

	"github.com/golang/protobuf/proto"

	"github.com/pingcap/tipb/go-mysqlx/Resultset"
)

// decodeZigzag64 reads a zigzag-encoded 64-bit integer
// from the Buffer.
// This is the format used for the sint64 protocol buffer type.
func decodeZigzag64(x uint64) int64 {
	x = (x >> 1) ^ uint64((int64(x&1)<<63)>>63)
	return int64(x)
}

// MySQL signed int to int64
func mysqlSintToInt(data []byte) (signed int64, e error) {
	myint, num := proto.DecodeVarint(data)
	if num == 0 {
		e = fmt.Errorf("Unable to decode '% x' as varint...", data)
	}

	signed = decodeZigzag64(myint)

	return signed, e
}

// MySQL unsigned signed int to uint64
func mysqlUintToUint(data []byte) (unsigned uint64, e error) {
	unsigned, num := proto.DecodeVarint(data)
	if num == 0 {
		e = fmt.Errorf("Unable to decode '% x' as varint. unsigned: %+v", data, unsigned)
	}

	return unsigned, e
}

// MySQL float to float32
func mysqlFloatToFloat32(data []byte) (float32, error) {
	f := math.Float32frombits(binary.LittleEndian.Uint32(data[:]))

	return f, nil
}

// MySQL double to float64
func mysqlDoubleToFloat64(data []byte) (float64, error) {
	f := math.Float64frombits(binary.LittleEndian.Uint64(data[:]))

	return f, nil
}

// MySQL char/binary. The trailing \x00 needs to be removed.
func mysqlBytesToBytes(data []byte) ([]byte, error) {
	// 012345
	// ABCDEF\x00 (len 7)
	dest := data[0 : len(data)-1] // not copying to avoid overhead (is this ok?), just using a shorter slice

	return dest, nil
}

// for handling stuff we haven't done yet. Should become obsolete as I finish the code...
func noConversion(typeName string, data []byte) ([]byte, error) {
	dest := data

	//	if len(data) == 0 {
	//		return nil, nil // this is a NULL result
	//	}

	return dest[0 : len(data)-1], nil // chop off last character
}

// Handle the conversion from the MysQL type to the driver type
func convertColumnData(column *Mysqlx_Resultset.ColumnMetaData, data []byte) (dest driver.Value, e error) {

	// We don't expect data to be nil. Probably a bug?
	if data == nil {
		return nil, fmt.Errorf("convertColumnData: data == nil. Unexpected. Returning dest = nil")
	}
	// An empty slice implies NULL
	if len(data) == 0 {
		return nil, nil
	}

	// If we get this far we have non NULL values.
	switch column.GetType() {
	case Mysqlx_Resultset.ColumnMetaData_SINT:
		return mysqlSintToInt(data)
	case Mysqlx_Resultset.ColumnMetaData_UINT:
		return mysqlUintToUint(data)
	case Mysqlx_Resultset.ColumnMetaData_DOUBLE:
		return mysqlDoubleToFloat64(data)
	case Mysqlx_Resultset.ColumnMetaData_BYTES:
		return mysqlBytesToBytes(data)
	case Mysqlx_Resultset.ColumnMetaData_FLOAT:
		return mysqlFloatToFloat32(data)
	//        ColumnMetaData_BYTES    ColumnMetaData_FieldType = 7
	//        ColumnMetaData_TIME     ColumnMetaData_FieldType = 10
	//        ColumnMetaData_DATETIME ColumnMetaData_FieldType = 12
	//        ColumnMetaData_SET      ColumnMetaData_FieldType = 15
	//        ColumnMetaData_ENUM     ColumnMetaData_FieldType = 16
	//        ColumnMetaData_BIT      ColumnMetaData_FieldType = 17
	//        ColumnMetaData_DECIMAL  ColumnMetaData_FieldType = 18
	default:
		return noConversion("BYTES", data)
	}

	return dest, nil
}
