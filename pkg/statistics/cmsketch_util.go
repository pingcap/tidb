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

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/hack"
)

// DatumMapCache is used to store the mapping from the string type to datum type.
// The datum is used to find the value in the histogram.
type DatumMapCache struct {
	datumMap map[hack.MutableString]types.Datum
}

// NewDatumMapCache creates a new DatumMapCache.
func NewDatumMapCache() *DatumMapCache {
	return &DatumMapCache{
		datumMap: make(map[hack.MutableString]types.Datum),
	}
}

// Get gets the datum from the cache.
func (d *DatumMapCache) Get(key hack.MutableString) (val types.Datum, ok bool) {
	val, ok = d.datumMap[key]
	return
}

// Put puts the datum into the cache.
func (d *DatumMapCache) Put(val TopNMeta, encodedVal hack.MutableString,
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
