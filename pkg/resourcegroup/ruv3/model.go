// Copyright 2026 PingCAP, Inc.
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

// Package ruv3 defines the raw units and weighting model used to calculate RU v3.
package ruv3

import "math"

// StmtUnits contains the raw work measured for one RU v3 calculation.
type StmtUnits struct {
	// WriteStatement is one for a write DML, including one affecting no rows.
	WriteStatement float64
	// OperatorNum counts final plan occurrences, including pushed operators.
	OperatorNum float64
	// WriteKeys and WriteBytes describe committed TiKV payload. Explicit
	// transactions contribute these only on COMMIT, not on each DML.
	WriteKeys  float64
	WriteBytes float64
	// CPUWork is the sum of occurrence-local operator work from the supported
	// root and coprocessor operators in the flat plan.
	CPUWork float64
	// ScanBytes is the sum of physical-byte estimates from supported Reader
	// request components. Each contribution is collected once per request.
	ScanBytes float64
	// NetBytes is statement transport evidence, not operator attribution. It is
	// the finalized TiKV coprocessor response-body byte count.
	NetBytes float64
	// FrontendCompileBytes is the UTF-8 byte length of the normalized SQL text.
	FrontendCompileBytes float64
	// HashStateRows counts entries admitted to completed, operator-owned hash
	// lookup or group-state structures.
	HashStateRows float64
	// JoinOutputRows counts rows produced by supported Join occurrences after
	// their join conditions and join-type semantics are applied.
	JoinOutputRows float64
}

// StmtWeights contains the coefficient for each RU v3 raw unit.
type StmtWeights struct {
	CPUWork             float64
	ScanByte            float64
	NetByte             float64
	FrontendCompileByte float64
	HashStateRow        float64
	JoinOutputRow       float64
	WriteStatement      float64
	OperatorNum         float64
	WriteKey            float64
	WriteByte           float64
}

// StmtResult contains the weighted RU v3 total.
type StmtResult struct {
	TotalRU float64
}

// DefaultWeights returns the deliberately uncalibrated weights used by the
// current RU v3 model. They are placeholders, not billing values.
func DefaultWeights() StmtWeights {
	return StmtWeights{
		CPUWork:             1,
		ScanByte:            1,
		NetByte:             1,
		FrontendCompileByte: 1,
		HashStateRow:        1,
		JoinOutputRow:       1,
		WriteStatement:      1,
		OperatorNum:         1,
		WriteKey:            1,
		WriteByte:           1,
	}
}

// Valid reports whether every raw unit is finite and nonnegative.
func (units StmtUnits) Valid() bool {
	return validValues(
		units.CPUWork,
		units.ScanBytes,
		units.NetBytes,
		units.FrontendCompileBytes,
		units.HashStateRows,
		units.JoinOutputRows,
		units.WriteStatement,
		units.OperatorNum,
		units.WriteKeys,
		units.WriteBytes,
	)
}

func (weights StmtWeights) valid() bool {
	return validValues(
		weights.CPUWork,
		weights.ScanByte,
		weights.NetByte,
		weights.FrontendCompileByte,
		weights.HashStateRow,
		weights.JoinOutputRow,
		weights.WriteStatement,
		weights.OperatorNum,
		weights.WriteKey,
		weights.WriteByte,
	)
}

func validValues(values ...float64) bool {
	for _, value := range values {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return true
}

// Add returns the field-by-field sum of units and other. Validation is
// intentionally left to Valid or Calculate.
func (units StmtUnits) Add(other StmtUnits) StmtUnits {
	units.CPUWork += other.CPUWork
	units.ScanBytes += other.ScanBytes
	units.NetBytes += other.NetBytes
	units.FrontendCompileBytes += other.FrontendCompileBytes
	units.HashStateRows += other.HashStateRows
	units.JoinOutputRows += other.JoinOutputRows
	units.WriteStatement += other.WriteStatement
	units.OperatorNum += other.OperatorNum
	units.WriteKeys += other.WriteKeys
	units.WriteBytes += other.WriteBytes
	return units
}

// Sub subtracts other from units field by field. Validation is intentionally
// left to Valid or Calculate.
func (units StmtUnits) Sub(other StmtUnits) StmtUnits {
	units.CPUWork -= other.CPUWork
	units.ScanBytes -= other.ScanBytes
	units.NetBytes -= other.NetBytes
	units.FrontendCompileBytes -= other.FrontendCompileBytes
	units.HashStateRows -= other.HashStateRows
	units.JoinOutputRows -= other.JoinOutputRows
	units.WriteStatement -= other.WriteStatement
	units.OperatorNum -= other.OperatorNum
	units.WriteKeys -= other.WriteKeys
	units.WriteBytes -= other.WriteBytes
	return units
}

// Calculate applies weights to units. It returns false for invalid input or an
// invalid weighted result.
func Calculate(units StmtUnits, weights StmtWeights) (StmtResult, bool) {
	if !units.Valid() || !weights.valid() {
		return StmtResult{}, false
	}
	totalRU := weights.CPUWork*units.CPUWork +
		weights.ScanByte*units.ScanBytes +
		weights.NetByte*units.NetBytes +
		weights.FrontendCompileByte*units.FrontendCompileBytes +
		weights.HashStateRow*units.HashStateRows +
		weights.JoinOutputRow*units.JoinOutputRows +
		weights.WriteStatement*units.WriteStatement +
		weights.OperatorNum*units.OperatorNum +
		weights.WriteKey*units.WriteKeys +
		weights.WriteByte*units.WriteBytes
	if !validValues(totalRU) {
		return StmtResult{}, false
	}
	return StmtResult{TotalRU: totalRU}, true
}
