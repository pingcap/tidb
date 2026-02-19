// Copyright 2016 PingCAP, Inc.
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

package types

import (
	"cmp"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
)

// Equals implements base.HashEquals.<1st> interface.
func (d *Datum) Equals(other any) bool {
	if other == nil {
		return false
	}
	var d2 *Datum
	switch x := other.(type) {
	case *Datum:
		d2 = x
	case Datum:
		d2 = &x
	default:
		return false
	}
	ok := d.k == d2.k &&
		d.decimal == d2.decimal &&
		d.length == d2.length &&
		d.i == d2.i &&
		d.collation == d2.collation &&
		string(d.b) == string(d2.b)
	if !ok {
		return false
	}
	// compare x
	switch d.k {
	case KindMysqlDecimal:
		return d.GetMysqlDecimal().Compare(d2.GetMysqlDecimal()) == 0
	case KindMysqlTime:
		return d.GetMysqlTime().Compare(d2.GetMysqlTime()) == 0
	default:
		return true
	}
}

// Compare compares datum to another datum.
// Notes: don't rely on datum.collation to get the collator, it's tend to buggy.
func (d *Datum) Compare(ctx Context, ad *Datum, comparer collate.Collator) (int, error) {
	if d.k == KindMysqlJSON && ad.k != KindMysqlJSON {
		cmp, err := ad.Compare(ctx, d, comparer)
		return cmp * -1, errors.Trace(err)
	}
	switch ad.k {
	case KindNull:
		if d.k == KindNull {
			return 0, nil
		}
		return 1, nil
	case KindMinNotNull:
		if d.k == KindNull {
			return -1, nil
		} else if d.k == KindMinNotNull {
			return 0, nil
		}
		return 1, nil
	case KindMaxValue:
		if d.k == KindMaxValue {
			return 0, nil
		}
		return -1, nil
	case KindInt64:
		return d.compareInt64(ctx, ad.GetInt64())
	case KindUint64:
		return d.compareUint64(ctx, ad.GetUint64())
	case KindFloat32, KindFloat64:
		return d.compareFloat64(ctx, ad.GetFloat64())
	case KindString:
		return d.compareString(ctx, ad.GetString(), comparer)
	case KindBytes:
		return d.compareString(ctx, ad.GetString(), comparer)
	case KindMysqlDecimal:
		return d.compareMysqlDecimal(ctx, ad.GetMysqlDecimal())
	case KindMysqlDuration:
		return d.compareMysqlDuration(ctx, ad.GetMysqlDuration())
	case KindMysqlEnum:
		return d.compareMysqlEnum(ctx, ad.GetMysqlEnum(), comparer)
	case KindBinaryLiteral, KindMysqlBit:
		return d.compareBinaryLiteral(ctx, ad.GetBinaryLiteral4Cmp(), comparer)
	case KindMysqlSet:
		return d.compareMysqlSet(ctx, ad.GetMysqlSet(), comparer)
	case KindMysqlJSON:
		return d.compareMysqlJSON(ad.GetMysqlJSON())
	case KindMysqlTime:
		return d.compareMysqlTime(ctx, ad.GetMysqlTime())
	case KindVectorFloat32:
		return d.compareVectorFloat32(ctx, ad.GetVectorFloat32())
	default:
		return 0, nil
	}
}

func (d *Datum) compareInt64(ctx Context, i int64) (int, error) {
	switch d.k {
	case KindMaxValue:
		return 1, nil
	case KindInt64:
		return cmp.Compare(d.i, i), nil
	case KindUint64:
		if i < 0 || d.GetUint64() > math.MaxInt64 {
			return 1, nil
		}
		return cmp.Compare(d.i, i), nil
	default:
		return d.compareFloat64(ctx, float64(i))
	}
}

func (d *Datum) compareUint64(ctx Context, u uint64) (int, error) {
	switch d.k {
	case KindMaxValue:
		return 1, nil
	case KindInt64:
		if d.i < 0 || u > math.MaxInt64 {
			return -1, nil
		}
		return cmp.Compare(d.i, int64(u)), nil
	case KindUint64:
		return cmp.Compare(d.GetUint64(), u), nil
	default:
		return d.compareFloat64(ctx, float64(u))
	}
}

func (d *Datum) compareFloat64(ctx Context, f float64) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindInt64:
		return cmp.Compare(float64(d.i), f), nil
	case KindUint64:
		return cmp.Compare(float64(d.GetUint64()), f), nil
	case KindFloat32, KindFloat64:
		return cmp.Compare(d.GetFloat64(), f), nil
	case KindString, KindBytes:
		fVal, err := StrToFloat(ctx, d.GetString(), false)
		return cmp.Compare(fVal, f), errors.Trace(err)
	case KindMysqlDecimal:
		fVal, err := d.GetMysqlDecimal().ToFloat64()
		return cmp.Compare(fVal, f), errors.Trace(err)
	case KindMysqlDuration:
		fVal := d.GetMysqlDuration().Seconds()
		return cmp.Compare(fVal, f), nil
	case KindMysqlEnum:
		fVal := d.GetMysqlEnum().ToNumber()
		return cmp.Compare(fVal, f), nil
	case KindBinaryLiteral, KindMysqlBit:
		val, err := d.GetBinaryLiteral4Cmp().ToInt(ctx)
		fVal := float64(val)
		return cmp.Compare(fVal, f), errors.Trace(err)
	case KindMysqlSet:
		fVal := d.GetMysqlSet().ToNumber()
		return cmp.Compare(fVal, f), nil
	case KindMysqlTime:
		fVal, err := d.GetMysqlTime().ToNumber().ToFloat64()
		return cmp.Compare(fVal, f), errors.Trace(err)
	default:
		return -1, nil
	}
}

func (d *Datum) compareString(ctx Context, s string, comparer collate.Collator) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindString, KindBytes:
		return comparer.Compare(d.GetString(), s), nil
	case KindMysqlDecimal:
		dec := new(MyDecimal)
		err := ctx.HandleTruncate(dec.FromString(hack.Slice(s)))
		return d.GetMysqlDecimal().Compare(dec), errors.Trace(err)
	case KindMysqlTime:
		dt, err := ParseDatetime(ctx, s)
		return d.GetMysqlTime().Compare(dt), errors.Trace(err)
	case KindMysqlDuration:
		dur, _, err := ParseDuration(ctx, s, MaxFsp)
		return d.GetMysqlDuration().Compare(dur), errors.Trace(err)
	case KindMysqlSet:
		return comparer.Compare(d.GetMysqlSet().String(), s), nil
	case KindMysqlEnum:
		return comparer.Compare(d.GetMysqlEnum().String(), s), nil
	case KindBinaryLiteral, KindMysqlBit:
		return comparer.Compare(d.GetBinaryLiteral4Cmp().ToString(), s), nil
	default:
		fVal, err := StrToFloat(ctx, s, false)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return d.compareFloat64(ctx, fVal)
	}
}

func (d *Datum) compareMysqlDecimal(ctx Context, dec *MyDecimal) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindMysqlDecimal:
		return d.GetMysqlDecimal().Compare(dec), nil
	case KindString, KindBytes:
		dDec := new(MyDecimal)
		err := ctx.HandleTruncate(dDec.FromString(d.GetBytes()))
		return dDec.Compare(dec), errors.Trace(err)
	default:
		dVal, err := d.ConvertTo(ctx, NewFieldType(mysql.TypeNewDecimal))
		if err != nil {
			return 0, errors.Trace(err)
		}
		return dVal.GetMysqlDecimal().Compare(dec), nil
	}
}

func (d *Datum) compareMysqlDuration(ctx Context, dur Duration) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindMysqlDuration:
		return d.GetMysqlDuration().Compare(dur), nil
	case KindString, KindBytes:
		dDur, _, err := ParseDuration(ctx, d.GetString(), MaxFsp)
		return dDur.Compare(dur), errors.Trace(err)
	default:
		return d.compareFloat64(ctx, dur.Seconds())
	}
}

func (d *Datum) compareMysqlEnum(sc Context, enum Enum, comparer collate.Collator) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindString, KindBytes, KindMysqlEnum, KindMysqlSet:
		return comparer.Compare(d.GetString(), enum.String()), nil
	default:
		return d.compareFloat64(sc, enum.ToNumber())
	}
}

func (d *Datum) compareBinaryLiteral(ctx Context, b BinaryLiteral, comparer collate.Collator) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindString, KindBytes:
		fallthrough // in this case, d is converted to Binary and then compared with b
	case KindBinaryLiteral, KindMysqlBit:
		return comparer.Compare(d.GetBinaryLiteral4Cmp().ToString(), b.ToString()), nil
	default:
		val, err := b.ToInt(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		result, err := d.compareFloat64(ctx, float64(val))
		return result, errors.Trace(err)
	}
}

func (d *Datum) compareMysqlSet(ctx Context, set Set, comparer collate.Collator) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindString, KindBytes, KindMysqlEnum, KindMysqlSet:
		return comparer.Compare(d.GetString(), set.String()), nil
	default:
		return d.compareFloat64(ctx, set.ToNumber())
	}
}

func (d *Datum) compareMysqlJSON(target BinaryJSON) (int, error) {
	// json is not equal with NULL
	if d.k == KindNull {
		return 1, nil
	}

	origin, err := d.ToMysqlJSON()
	if err != nil {
		return 0, errors.Trace(err)
	}
	return CompareBinaryJSON(origin, target), nil
}

func (d *Datum) compareMysqlTime(ctx Context, time Time) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindString, KindBytes:
		dt, err := ParseDatetime(ctx, d.GetString())
		return dt.Compare(time), errors.Trace(err)
	case KindMysqlTime:
		return d.GetMysqlTime().Compare(time), nil
	default:
		fVal, err := time.ToNumber().ToFloat64()
		if err != nil {
			return 0, errors.Trace(err)
		}
		return d.compareFloat64(ctx, fVal)
	}
}

func (d *Datum) compareVectorFloat32(ctx Context, vec VectorFloat32) (int, error) {
	switch d.k {
	case KindNull, KindMinNotNull:
		return -1, nil
	case KindMaxValue:
		return 1, nil
	case KindVectorFloat32:
		return d.GetVectorFloat32().Compare(vec), nil
	// Note: We expect cast is applied before compare, when comparing with String and other vector types.
	default:
		return 0, errors.New("cannot compare vector and non-vector, cast is required")
	}
}
