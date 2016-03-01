// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
)

// CompareInt64 returns an integer comparing the int64 x to y.
func CompareInt64(x, y int64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// CompareUint64 returns an integer comparing the uint64 x to y.
func CompareUint64(x, y uint64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// CompareFloat64 returns an integer comparing the float64 x to y.
func CompareFloat64(x, y float64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// CompareInteger returns an integer comparing the int64 x to the uint64 y.
func CompareInteger(x int64, y uint64) int {
	if x < 0 {
		return -1
	}
	return CompareUint64(uint64(x), y)
}

// CompareString returns an integer comparing the string x to y.
func CompareString(x, y string) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// compareFloatString compares float a with string s.
// compareFloatString first parses s to a float value, if failed, returns error.
func compareFloatString(a float64, s string) (int, error) {
	// MySQL will convert string to a float point value.
	// MySQL uses a very loose conversation, e.g, 123.abc -> 123
	// We should do a trade off whether supporting this feature or using a strict mode.
	// Now we use a strict mode.
	b, err := StrToFloat(s)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return CompareFloat64(a, b), nil
}

// compareStringFloat compares string s with float a.
func compareStringFloat(s string, a float64) (int, error) {
	n, err := compareFloatString(a, s)
	return -n, errors.Trace(err)
}

func coerceCompare(a, b interface{}) (x interface{}, y interface{}, err error) {
	rowTypeNum := 0
	x, y = Coerce(a, b)
	// change []byte to string for later compare
	switch v := a.(type) {
	case []byte:
		x = string(v)
	case []interface{}:
		rowTypeNum++
	}

	switch v := b.(type) {
	case []byte:
		y = string(v)
	case []interface{}:
		rowTypeNum++
	}

	if rowTypeNum == 1 {
		// a and b must be all row type or not
		err = errors.Errorf("invalid comapre type %T cmp %T", a, b)
	}

	return x, y, errors.Trace(err)
}

func compareRow(a, b []interface{}) (int, error) {
	if len(a) != len(b) {
		return 0, errors.Errorf("mismatch columns for row %v cmp %v", a, b)
	}

	for i := range a {
		n, err := Compare(a[i], b[i])
		if err != nil {
			return 0, errors.Trace(err)
		} else if n != 0 {
			return n, nil
		}
	}
	return 0, nil
}

// Compare returns an integer comparing the interface a with b.
// a > b -> 1
// a = b -> 0
// a < b -> -1
func Compare(a, b interface{}) (int, error) {
	var coerceErr error
	a, b, coerceErr = coerceCompare(a, b)
	if coerceErr != nil {
		return 0, errors.Trace(coerceErr)
	}

	if va, ok := a.([]interface{}); ok {
		// we guarantee in coerceCompare that a and b are both []interface{}
		vb := b.([]interface{})
		return compareRow(va, vb)
	}

	if a == nil || b == nil {
		// Check ni first, nil is always less than none nil value.
		if a == nil && b != nil {
			return -1, nil
		} else if a != nil && b == nil {
			return 1, nil
		} else {
			// here a and b are all nil
			return 0, nil
		}
	}

	// TODO: support compare time type with other int, float, decimal types.
	switch x := a.(type) {
	case float64:
		switch y := b.(type) {
		case float64:
			return CompareFloat64(x, y), nil
		case string:
			return compareFloatString(x, y)
		}
	case int64:
		switch y := b.(type) {
		case int64:
			return CompareInt64(x, y), nil
		case uint64:
			return CompareInteger(x, y), nil
		case string:
			return compareFloatString(float64(x), y)
		case mysql.Hex:
			return CompareFloat64(float64(x), y.ToNumber()), nil
		case mysql.Bit:
			return CompareFloat64(float64(x), y.ToNumber()), nil
		case mysql.Enum:
			return CompareFloat64(float64(x), y.ToNumber()), nil
		case mysql.Set:
			return CompareFloat64(float64(x), y.ToNumber()), nil
		}
	case uint64:
		switch y := b.(type) {
		case uint64:
			return CompareUint64(x, y), nil
		case int64:
			return -CompareInteger(y, x), nil
		case string:
			return compareFloatString(float64(x), y)
		case mysql.Hex:
			return CompareFloat64(float64(x), y.ToNumber()), nil
		case mysql.Bit:
			return CompareFloat64(float64(x), y.ToNumber()), nil
		case mysql.Enum:
			return CompareFloat64(float64(x), y.ToNumber()), nil
		case mysql.Set:
			return CompareFloat64(float64(x), y.ToNumber()), nil
		}
	case mysql.Decimal:
		switch y := b.(type) {
		case mysql.Decimal:
			return x.Cmp(y), nil
		case string:
			f, err := mysql.ConvertToDecimal(y)
			if err != nil {
				return 0, errors.Trace(err)
			}
			return x.Cmp(f), nil
		}
	case string:
		switch y := b.(type) {
		case string:
			return CompareString(x, y), nil
		case int64:
			return compareStringFloat(x, float64(y))
		case uint64:
			return compareStringFloat(x, float64(y))
		case float64:
			return compareStringFloat(x, y)
		case mysql.Decimal:
			f, err := mysql.ConvertToDecimal(x)
			if err != nil {
				return 0, errors.Trace(err)
			}
			return f.Cmp(y), nil
		case mysql.Time:
			n, err := y.CompareString(x)
			return -n, errors.Trace(err)
		case mysql.Duration:
			n, err := y.CompareString(x)
			return -n, errors.Trace(err)
		case mysql.Hex:
			return CompareString(x, y.ToString()), nil
		case mysql.Bit:
			return CompareString(x, y.ToString()), nil
		case mysql.Enum:
			return CompareString(x, y.String()), nil
		case mysql.Set:
			return CompareString(x, y.String()), nil
		}
	case mysql.Time:
		switch y := b.(type) {
		case mysql.Time:
			return x.Compare(y), nil
		case string:
			return x.CompareString(y)
		}
	case mysql.Duration:
		switch y := b.(type) {
		case mysql.Duration:
			return x.Compare(y), nil
		case string:
			return x.CompareString(y)
		}
	case mysql.Hex:
		switch y := b.(type) {
		case int64:
			return CompareFloat64(x.ToNumber(), float64(y)), nil
		case uint64:
			return CompareFloat64(x.ToNumber(), float64(y)), nil
		case mysql.Bit:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		case string:
			return CompareString(x.ToString(), y), nil
		case mysql.Enum:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		case mysql.Set:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		case mysql.Hex:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		}
	case mysql.Bit:
		switch y := b.(type) {
		case int64:
			return CompareFloat64(x.ToNumber(), float64(y)), nil
		case uint64:
			return CompareFloat64(x.ToNumber(), float64(y)), nil
		case mysql.Hex:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		case string:
			return CompareString(x.ToString(), y), nil
		case mysql.Enum:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		case mysql.Set:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		case mysql.Bit:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		}
	case mysql.Enum:
		switch y := b.(type) {
		case int64:
			return CompareFloat64(x.ToNumber(), float64(y)), nil
		case uint64:
			return CompareFloat64(x.ToNumber(), float64(y)), nil
		case mysql.Hex:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		case string:
			return CompareString(x.String(), y), nil
		case mysql.Bit:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		case mysql.Set:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		case mysql.Enum:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		}
	case mysql.Set:
		switch y := b.(type) {
		case int64:
			return CompareFloat64(x.ToNumber(), float64(y)), nil
		case uint64:
			return CompareFloat64(x.ToNumber(), float64(y)), nil
		case mysql.Hex:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		case string:
			return CompareString(x.String(), y), nil
		case mysql.Bit:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		case mysql.Enum:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		case mysql.Set:
			return CompareFloat64(x.ToNumber(), y.ToNumber()), nil
		}
	}

	return 0, errors.Errorf("invalid comapre type %T cmp %T", a, b)
}
