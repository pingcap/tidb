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

package ruv3

import (
	"math"
	"reflect"
	"testing"
)

func TestDefaultWeights(t *testing.T) {
	want := StmtWeights{
		CPUWork: 1, ScanByte: 1, NetByte: 1, FrontendCompileByte: 1,
		HashStateRow: 1, JoinOutputRow: 1, WriteStatement: 1,
		OperatorNum: 1, WriteKey: 1, WriteByte: 1,
	}
	if got := DefaultWeights(); !reflect.DeepEqual(got, want) {
		t.Fatalf("DefaultWeights() = %+v, want %+v", got, want)
	}
}

func TestCalculate(t *testing.T) {
	units := StmtUnits{
		CPUWork: 1, ScanBytes: 2, NetBytes: 3, FrontendCompileBytes: 4,
		HashStateRows: 5, JoinOutputRows: 6, WriteStatement: 7,
		OperatorNum: 8, WriteKeys: 9, WriteBytes: 10,
	}
	weights := StmtWeights{
		CPUWork: 2, ScanByte: 3, NetByte: 4, FrontendCompileByte: 5,
		HashStateRow: 6, JoinOutputRow: 7, WriteStatement: 8,
		OperatorNum: 9, WriteKey: 10, WriteByte: 11,
	}

	got, ok := Calculate(units, weights)
	if !ok {
		t.Fatal("Calculate() rejected valid units and weights")
	}
	if want := (StmtResult{TotalRU: 440}); got != want {
		t.Fatalf("Calculate() = %+v, want %+v", got, want)
	}
}

func TestCalculateRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		units   StmtUnits
		weights StmtWeights
	}{
		{name: "negative unit", units: StmtUnits{CPUWork: -1}, weights: DefaultWeights()},
		{name: "NaN unit", units: StmtUnits{ScanBytes: math.NaN()}, weights: DefaultWeights()},
		{name: "infinite unit", units: StmtUnits{WriteBytes: math.Inf(1)}, weights: DefaultWeights()},
		{name: "negative weight", weights: StmtWeights{CPUWork: -1}},
		{name: "NaN weight", weights: StmtWeights{ScanByte: math.NaN()}},
		{name: "infinite weight", weights: StmtWeights{WriteByte: math.Inf(1)}},
		{name: "infinite result", units: StmtUnits{CPUWork: math.MaxFloat64}, weights: StmtWeights{CPUWork: 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := Calculate(tt.units, tt.weights)
			if ok {
				t.Fatalf("Calculate() = %+v, true, want zero result and false", got)
			}
			if got != (StmtResult{}) {
				t.Fatalf("Calculate() result = %+v, want zero result", got)
			}
		})
	}
}

func TestStmtUnitsValid(t *testing.T) {
	if !(StmtUnits{}).Valid() {
		t.Fatal("zero StmtUnits should be valid")
	}
	for name, units := range map[string]StmtUnits{
		"negative": {OperatorNum: -1},
		"NaN":      {HashStateRows: math.NaN()},
		"infinite": {JoinOutputRows: math.Inf(-1)},
	} {
		t.Run(name, func(t *testing.T) {
			if units.Valid() {
				t.Fatalf("StmtUnits.Valid() = true for %+v", units)
			}
		})
	}
}

func TestStmtUnitsArithmetic(t *testing.T) {
	left := StmtUnits{
		CPUWork: 1, ScanBytes: 2, NetBytes: 3, FrontendCompileBytes: 4,
		HashStateRows: 5, JoinOutputRows: 6, WriteStatement: 7,
		OperatorNum: 8, WriteKeys: 9, WriteBytes: 10,
	}
	right := StmtUnits{
		CPUWork: 10, ScanBytes: 9, NetBytes: 8, FrontendCompileBytes: 7,
		HashStateRows: 6, JoinOutputRows: 5, WriteStatement: 4,
		OperatorNum: 3, WriteKeys: 2, WriteBytes: 1,
	}

	leftBefore := left
	got := left.Add(right)
	if left != leftBefore {
		t.Fatalf("StmtUnits.Add() mutated its receiver: got %+v, want %+v", left, leftBefore)
	}

	want := StmtUnits{
		CPUWork: 11, ScanBytes: 11, NetBytes: 11, FrontendCompileBytes: 11,
		HashStateRows: 11, JoinOutputRows: 11, WriteStatement: 11,
		OperatorNum: 11, WriteKeys: 11, WriteBytes: 11,
	}
	if got != want {
		t.Fatalf("StmtUnits.Add() = %+v, want %+v", got, want)
	}
	if got := got.Sub(right); got != left {
		t.Fatalf("StmtUnits.Sub() = %+v, want %+v", got, left)
	}
}
