// Copyright 2019-present PingCAP, Inc.
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
//
// +build darwin linux windows

package mathutil

// Reexport functions and variables from mathutil
import (
	"github.com/cznic/mathutil"
)

const (
	// MaxInt presents the maximum number of Int
	MaxInt = mathutil.MaxInt
	// MinInt presents the minimum number of Int
	MinInt = mathutil.MinInt
)

// MaxUint64 returns the larger of a and b.
var MaxUint64 = mathutil.MaxUint64

// MinUint64 returns the smaller of a and b.
var MinUint64 = mathutil.MinUint64

// MaxUint32 returns the larger of a and b.
var MaxUint32 = mathutil.MaxUint32

// MinUint32 returns the smaller of a and b.
var MinUint32 = mathutil.MinUint32

// MaxInt64 returns the larger of a and b.
var MaxInt64 = mathutil.MaxInt64

// MinInt64 returns the smaller of a and b.
var MinInt64 = mathutil.MinInt64

// MaxInt8 returns the larger of a and b.
var MaxInt8 = mathutil.MaxInt8

// MinInt8 returns the smaller of a and b.
var MinInt8 = mathutil.MinInt8

// Max returns the larger of a and b.
var Max = mathutil.Max

// Min returns the smaller of a and b.
var Min = mathutil.Min
