// Copyright 2023 PingCAP, Inc.
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

package statistics

import (
	"time"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
)

type datumMapCache struct {
	datumMap map[hack.MutableString]types.Datum
}

func newDatumMapCache() *datumMapCache {
	return &datumMapCache{
		datumMap: make(map[hack.MutableString]types.Datum),
	}
}

func (d *datumMapCache) Get(key hack.MutableString) (val types.Datum, ok bool) {
	val, ok = d.datumMap[key]
	return
}

func (d *datumMapCache) Put(val TopNMeta, encodedVal hack.MutableString,
	tp byte, isIndex bool, loc *time.Location) (dat types.Datum, err error) {
	dat, err = topNMetaToDatum(val, tp, isIndex, loc)
	if err != nil {
		return dat, err
	}
	d.datumMap[encodedVal] = dat
	return dat, nil
}

func topNMetaToDatum(val TopNMeta,
	tp byte, isIndex bool, loc *time.Location) (dat types.Datum, err error) {
	if isIndex {
		dat.SetBytes(val.Encoded)
	} else {
		var err error
		if types.IsTypeTime(tp) {
			// Handle date time values specially since they are encoded to int and we'll get int values if using DecodeOne.
			_, dat, err = codec.DecodeAsDateTime(val.Encoded, tp, loc)
		} else if types.IsTypeFloat(tp) {
			_, dat, err = codec.DecodeAsFloat32(val.Encoded, tp)
		} else {
			_, dat, err = codec.DecodeOne(val.Encoded)
		}
		if err != nil {
			return dat, err
		}
	}
	return dat, err
}
