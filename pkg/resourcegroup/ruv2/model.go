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

// Package ruv2 defines the raw units and weighting model used to calculate RU v3.
package ruv2

import (
	"fmt"
	"math"
)

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
	// ScanBytes combines TiKV physical-byte estimates with TiFlash user_read_bytes,
	// preserving each producer's byte definition. Reader boundaries count each
	// request component's contribution once.
	ScanBytes float64
	// NetBytes is statement transport evidence, not operator attribution. It is
	// the finalized TiKV response-body and TiFlash remote connection byte counts.
	NetBytes float64
	// CrossAZNetBytes is the subset of NetBytes identified as cross-AZ TiFlash traffic.
	CrossAZNetBytes float64
	// FrontendCompileBytes is the UTF-8 byte length of the normalized SQL text,
	// or zero when the statement hits the plan cache.
	FrontendCompileBytes float64
	// HashStateRows measures constructed hash lookup or group state. TiFlash
	// contributions retain their producer's size definition: distinct keys,
	// build rows, or aggregation map entries, summed without normalization.
	HashStateRows float64
	// JoinOutputRows counts rows produced by supported Join occurrences after
	// their join conditions and join-type semantics are applied.
	JoinOutputRows float64
}

// StmtWeights contains the coefficient for each RU v3 raw unit.
type StmtWeights struct {
	// CrossAZNetByte is reserved for an additional cross-AZ charge. Cross-AZ
	// traffic is already included in NetBytes at the ordinary NetByte weight;
	// current accounting does not price it differently, so this defaults to zero.
	// The "-" tags deliberately exclude it from TOML/JSON until separate pricing
	// is supported; raw CrossAZNetBytes remain available for observation.
	CrossAZNetByte      float64 `toml:"-" json:"-"`
	CPUWork             float64 `toml:"cpu-work" json:"cpu-work"`
	ScanByte            float64 `toml:"scan-byte" json:"scan-byte"`
	NetByte             float64 `toml:"net-byte" json:"net-byte"`
	FrontendCompileByte float64 `toml:"frontend-compile-byte" json:"frontend-compile-byte"`
	HashStateRow        float64 `toml:"hash-state-row" json:"hash-state-row"`
	JoinOutputRow       float64 `toml:"join-output-row" json:"join-output-row"`
	WriteStatement      float64 `toml:"write-statement" json:"write-statement"`
	OperatorNum         float64 `toml:"operator-num" json:"operator-num"`
	WriteKey            float64 `toml:"write-key" json:"write-key"`
	WriteByte           float64 `toml:"write-byte" json:"write-byte"`
}

// DDLWeights contains the coefficient for each DDL RU v2 byte unit.
type DDLWeights struct {
	TxnKVBytes    float64 `toml:"txn-kv-bytes" json:"txn-kv-bytes"`
	IngestKVBytes float64 `toml:"ingest-kv-bytes" json:"ingest-kv-bytes"`
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

// DefaultDDLWeights returns the deliberately uncalibrated DDL weights used by
// the current RU v2 model. They are placeholders, not billing values.
func DefaultDDLWeights() DDLWeights {
	return DDLWeights{
		TxnKVBytes:    1,
		IngestKVBytes: 1,
	}
}

// Validate checks that every DDL weight is finite and nonnegative.
func (weights DDLWeights) Validate() error {
	for _, weight := range []struct {
		name  string
		value float64
	}{
		{"txn-kv-bytes", weights.TxnKVBytes},
		{"ingest-kv-bytes", weights.IngestKVBytes},
	} {
		if !validValues(weight.value) {
			return fmt.Errorf("%s must be finite and non-negative, got %v", weight.name, weight.value)
		}
	}
	return nil
}

// Valid reports whether every raw unit is finite and nonnegative.
func (units StmtUnits) Valid() bool {
	return units.CrossAZNetBytes <= units.NetBytes && validValues(
		units.CrossAZNetBytes,
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
		weights.CrossAZNetByte,
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

// Validate checks that every weight is finite and nonnegative.
func (weights StmtWeights) Validate() error {
	for _, weight := range []struct {
		name  string
		value float64
	}{
		{"cpu-work", weights.CPUWork},
		{"scan-byte", weights.ScanByte},
		{"net-byte", weights.NetByte},
		{"cross-az-net-byte", weights.CrossAZNetByte},
		{"frontend-compile-byte", weights.FrontendCompileByte},
		{"hash-state-row", weights.HashStateRow},
		{"join-output-row", weights.JoinOutputRow},
		{"write-statement", weights.WriteStatement},
		{"operator-num", weights.OperatorNum},
		{"write-key", weights.WriteKey},
		{"write-byte", weights.WriteByte},
	} {
		if !validValues(weight.value) {
			return fmt.Errorf("%s must be finite and non-negative, got %v", weight.name, weight.value)
		}
	}
	return nil
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
	units.CrossAZNetBytes += other.CrossAZNetBytes
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
	units.CrossAZNetBytes -= other.CrossAZNetBytes
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
		weights.CrossAZNetByte*units.CrossAZNetBytes +
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
