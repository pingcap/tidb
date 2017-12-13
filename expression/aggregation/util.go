package aggregation

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mvmap"
)

// distinctChecker stores existing keys and checks if given data is distinct.
type distinctChecker struct {
	existingKeys *mvmap.MVMap
	buf          []byte
}

// createDistinctChecker creates a new distinct checker.
func createDistinctChecker() *distinctChecker {
	return &distinctChecker{
		existingKeys: mvmap.NewMVMap(),
	}
}

// Check checks if values is distinct.
func (d *distinctChecker) Check(values []types.Datum) (bool, error) {
	d.buf = d.buf[:0]
	var err error
	d.buf, err = codec.EncodeValue(d.buf, values...)
	if err != nil {
		return false, errors.Trace(err)
	}
	v := d.existingKeys.Get(d.buf)
	if v != nil {
		return false, nil
	}
	d.existingKeys.Put(d.buf, []byte{})
	return true, nil
}

// calculateSum adds v to sum.
func calculateSum(sc *stmtctx.StatementContext, sum, v types.Datum) (data types.Datum, err error) {
	// for avg and sum calculation
	// avg and sum use decimal for integer and decimal type, use float for others
	// see https://dev.mysql.com/doc/refman/5.7/en/group-by-functions.html

	switch v.Kind() {
	case types.KindNull:
	case types.KindInt64, types.KindUint64:
		var d *types.MyDecimal
		d, err = v.ToDecimal(sc)
		if err == nil {
			data = types.NewDecimalDatum(d)
		}
	case types.KindMysqlDecimal:
		data = v
	default:
		var f float64
		f, err = v.ToFloat64(sc)
		if err == nil {
			data = types.NewFloat64Datum(f)
		}
	}

	if err != nil {
		return data, errors.Trace(err)
	}
	if data.IsNull() {
		return sum, nil
	}
	switch sum.Kind() {
	case types.KindNull:
		return data, nil
	case types.KindFloat64, types.KindMysqlDecimal:
		return types.ComputePlus(sum, data)
	default:
		return data, errors.Errorf("invalid value %v for aggregate", sum.Kind())
	}
}
