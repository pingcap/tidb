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

package types

import "github.com/pingcap/tidb/types/json"

// Row is an interface to read columns values.
type Row interface {

	// GetInt64 returns the int64 value and isNull with the colIdx.
	GetInt64(colIdx int) (val int64, isNull bool)

	// GetUint64 returns the uint64 value and isNull with the colIdx.
	GetUint64(colIdx int) (val uint64, isNull bool)

	// GetFloat32 returns the float32 value and isNull with the colIdx.
	GetFloat32(colIdx int) (float32, bool)

	// GetFloat64 returns the float64 value and isNull with the colIdx.
	GetFloat64(colIdx int) (float64, bool)

	// GetString returns the string value and isNull with the colIdx.
	GetString(colIdx int) (string, bool)

	// GetBytes returns the bytes value and isNull with the colIdx.
	GetBytes(colIdx int) ([]byte, bool)

	// GetTime returns the Time value and is isNull with the colIdx.
	GetTime(colIdx int) (Time, bool)

	// GetDuration returns the Duration value and isNull with the colIdx.
	GetDuration(colIdx int) (Duration, bool)

	// GetEnum returns the Enum value and isNull with the colIdx.
	GetEnum(colIdx int) (Enum, bool)

	// GetSet returns the Set value and isNull with the colIdx.
	GetSet(colIdx int) (Set, bool)

	// GetMyDecimal returns the MyDecimal value and isNull with the colIdx.
	GetMyDecimal(colIdx int) (*MyDecimal, bool)

	// GetJSON returns the JSON value and isNull with the colIdx.
	GetJSON(colIdx int) (json.JSON, bool)
}

var _ Row = DatumRow{}

// DatumRow is a slice of Datum, implements Row interface.
type DatumRow []Datum

// GetInt64 implements Row interface.
func (dr DatumRow) GetInt64(colIdx int) (int64, bool) {
	datum := dr[colIdx]
	if datum.IsNull() {
		return 0, true
	}
	return dr[colIdx].GetInt64(), false
}

// GetUint64 implements Row interface.
func (dr DatumRow) GetUint64(colIdx int) (uint64, bool) {
	datum := dr[colIdx]
	if datum.IsNull() {
		return 0, true
	}
	return dr[colIdx].GetUint64(), false
}

// GetFloat32 implements Row interface.
func (dr DatumRow) GetFloat32(colIdx int) (float32, bool) {
	datum := dr[colIdx]
	if datum.IsNull() {
		return 0, true
	}
	return dr[colIdx].GetFloat32(), false
}

// GetFloat64 implements Row interface.
func (dr DatumRow) GetFloat64(colIdx int) (float64, bool) {
	datum := dr[colIdx]
	if datum.IsNull() {
		return 0, true
	}
	return dr[colIdx].GetFloat64(), false
}

// GetString implements Row interface.
func (dr DatumRow) GetString(colIdx int) (string, bool) {
	datum := dr[colIdx]
	if datum.IsNull() {
		return "", true
	}
	return dr[colIdx].GetString(), false
}

// GetBytes implements Row interface.
func (dr DatumRow) GetBytes(colIdx int) ([]byte, bool) {
	datum := dr[colIdx]
	if datum.IsNull() {
		return nil, true
	}
	return dr[colIdx].GetBytes(), false
}

// GetTime implements Row interface.
func (dr DatumRow) GetTime(colIdx int) (Time, bool) {
	datum := dr[colIdx]
	if datum.IsNull() {
		return Time{}, true
	}
	return dr[colIdx].GetMysqlTime(), false
}

// GetDuration implements Row interface.
func (dr DatumRow) GetDuration(colIdx int) (Duration, bool) {
	datum := dr[colIdx]
	if datum.IsNull() {
		return Duration{}, true
	}
	return dr[colIdx].GetMysqlDuration(), false
}

// GetEnum implements Row interface.
func (dr DatumRow) GetEnum(colIdx int) (Enum, bool) {
	datum := dr[colIdx]
	if datum.IsNull() {
		return Enum{}, true
	}
	return dr[colIdx].GetMysqlEnum(), false
}

// GetSet implements Row interface.
func (dr DatumRow) GetSet(colIdx int) (Set, bool) {
	datum := dr[colIdx]
	if datum.IsNull() {
		return Set{}, true
	}
	return dr[colIdx].GetMysqlSet(), false
}

// GetMyDecimal implements Row interface.
func (dr DatumRow) GetMyDecimal(colIdx int) (*MyDecimal, bool) {
	datum := dr[colIdx]
	if datum.IsNull() {
		return nil, true
	}
	return dr[colIdx].GetMysqlDecimal(), false
}

// GetJSON implements Row interface.
func (dr DatumRow) GetJSON(colIdx int) (json.JSON, bool) {
	datum := dr[colIdx]
	if datum.IsNull() {
		return json.JSON{}, true
	}
	return dr[colIdx].GetMysqlJSON(), false
}
