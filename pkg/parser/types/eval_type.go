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

import "fmt"

// EvalType indicates the specified types that arguments and result of a built-in function should be.
type EvalType byte

const (
	// ETInt represents type INT in evaluation.
	ETInt EvalType = iota
	// ETReal represents type REAL in evaluation.
	ETReal
	// ETDecimal represents type DECIMAL in evaluation.
	ETDecimal
	// ETString represents type STRING in evaluation.
	ETString
	// ETDatetime represents type DATETIME in evaluation.
	ETDatetime
	// ETTimestamp represents type TIMESTAMP in evaluation.
	ETTimestamp
	// ETDuration represents type DURATION in evaluation.
	ETDuration
	// ETJson represents type JSON in evaluation.
	ETJson
	// ETVectorFloat32 represents type VectorFloat32 in evaluation.
	ETVectorFloat32
)

// IsStringKind returns true for ETString, ETDatetime, ETTimestamp, ETDuration, ETJson EvalTypes.
func (et EvalType) IsStringKind() bool {
	return et == ETString || et == ETDatetime ||
		et == ETTimestamp || et == ETDuration || et == ETJson || et == ETVectorFloat32
}

// IsVectorKind returns true for ETVectorXxx EvalTypes.
func (et EvalType) IsVectorKind() bool {
	return et == ETVectorFloat32
}

// String implements fmt.Stringer interface.
func (et EvalType) String() string {
	switch et {
	case ETInt:
		return "Int"
	case ETReal:
		return "Real"
	case ETDecimal:
		return "Decimal"
	case ETString:
		return "String"
	case ETDatetime:
		return "Datetime"
	case ETTimestamp:
		return "Timestamp"
	case ETDuration:
		return "Time"
	case ETJson:
		return "Json"
	case ETVectorFloat32:
		return "VectorFloat32"
	default:
		panic(fmt.Sprintf("invalid EvalType %d", et))
	}
}
