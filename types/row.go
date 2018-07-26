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
	// Len returns the number of values in the row.
	Len() int

	// GetInt64 returns the int64 value with the colIdx.
	GetInt64(colIdx int) int64

	// GetUint64 returns the uint64 value with the colIdx.
	GetUint64(colIdx int) uint64

	// GetFloat32 returns the float32 value with the colIdx.
	GetFloat32(colIdx int) float32

	// GetFloat64 returns the float64 value with the colIdx.
	GetFloat64(colIdx int) float64

	// GetString returns the string value with the colIdx.
	GetString(colIdx int) string

	// GetBytes returns the bytes value with the colIdx.
	GetBytes(colIdx int) []byte

	// GetTime returns the Time value with the colIdx.
	GetTime(colIdx int) Time

	// GetDuration returns the Duration value with the colIdx.
	// fillFsp is needed for refill fsp info if duration came from chunk.Row which is no longer store fsp info.
	// If caller make sure that data from Datum or only use Duration.Duration properties can pass 0 as fillFsp.
	GetDuration(colIdx int, fillFsp int) Duration

	// GetEnum returns the Enum value with the colIdx.
	GetEnum(colIdx int) Enum

	// GetSet returns the Set value with the colIdx.
	GetSet(colIdx int) Set

	// GetMyDecimal returns the MyDecimal value with the colIdx.
	GetMyDecimal(colIdx int) *MyDecimal

	// GetJSON returns the JSON value with the colIdx.
	GetJSON(colIdx int) json.BinaryJSON

	// GetDatum returns a Datum with the colIdx and field type.
	// This method is provided for convenience, direct type methods are preferred for better performance.
	GetDatum(colIdx int, tp *FieldType) Datum

	// IsNull returns if the value is null with the colIdx.
	IsNull(colIdx int) bool
}
