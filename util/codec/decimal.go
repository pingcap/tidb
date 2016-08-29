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

package codec

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

// EncodeDecimal encodes a decimal d into a byte slice which can be sorted lexicographically later.
func EncodeDecimal(b []byte, d types.Datum) []byte {
	dec := d.GetMysqlDecimal()
	precision := d.Length()
	frac := d.Frac()
	if precision == 0 {
		precision, frac = dec.PrecisionAndFrac()
	}
	b = append(b, byte(precision), byte(frac))
	bin, err := dec.ToBin(precision, frac)
	if err != nil {
		log.Errorf("should not happen, precision %d, frac %d %v", precision, frac, err)
		return b
	}
	b = append(b, bin...)
	return b
}

// DecodeDecimal decodes bytes to decimal.
func DecodeDecimal(b []byte) ([]byte, types.Datum, error) {
	var d types.Datum
	if len(b) < 3 {
		return b, d, errors.New("insufficient bytes to decode value")
	}
	precision := int(b[0])
	frac := int(b[1])
	b = b[2:]
	dec := new(mysql.MyDecimal)
	binSize, err := dec.FromBin(b, precision, frac)
	b = b[binSize:]
	if err != nil {
		return b, d, errors.Trace(err)
	}
	d.SetLength(precision)
	d.SetFrac(frac)
	d.SetMysqlDecimal(dec)
	return b, d, nil
}
