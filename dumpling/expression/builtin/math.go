// Copyright 2013 The ql Authors. All rights reserved.
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

package builtin

import (
	"math"
	"math/rand"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/types"
)

// see https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html

func builtinAbs(args []interface{}, ctx map[interface{}]interface{}) (v interface{}, err error) {
	d := types.RawData(args[0])
	switch x := d.(type) {
	case nil:
		return nil, nil
	case uint, uint8, uint16, uint32, uint64:
		return x, nil
	case int, int8, int16, int32, int64:
		// we don't need to handle error here, it must be success
		v, _ := types.ToInt64(d)
		if v >= 0 {
			return x, nil
		}

		// TODO: handle overflow if x is MinInt64
		return -v, nil
	default:
		// we will try to convert other types to float
		// TODO: if time has no precision, it will be a integer
		f, err := types.ToFloat64(d)
		return math.Abs(f), errors.Trace(err)
	}
}

func builtinRand(args []interface{}, ctx map[interface{}]interface{}) (v interface{}, err error) {
	if len(args) == 1 {
		seed, err := types.ToInt64(args[0])
		if err != nil {
			return nil, errors.Trace(err)
		}

		rand.Seed(seed)
	}

	return rand.Float64(), nil
}

func builtinPow(args []interface{}, ctx map[interface{}]interface{}) (v interface{}, err error) {
	x, err := types.ToFloat64(args[0])
	if err != nil {
		return nil, errors.Trace(err)
	}

	y, err := types.ToFloat64(args[1])
	if err != nil {
		return nil, errors.Trace(err)
	}

	return math.Pow(x, y), nil

}
