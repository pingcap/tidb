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

package plans

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/kv"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/types"
)

// TODO: split into multi files

// Note: All plan.Plans must have a pointer receiver. Enables plan.PlanA == plan.PlanB operation.
var (
	_ plan.Plan = (*NullPlan)(nil)
	_ plan.Plan = (*selectIndexDefaultPlan)(nil)
)

func isTableOrIndex(p plan.Plan) bool {
	switch p.(type) {
	case
		*indexPlan,
		*TableDefaultPlan:
		return true
	default:
		return false
	}
}

func getIdentValue(name string, fields []*field.ResultField, row []interface{}, flag uint32) (interface{}, error) {
	indices := field.GetResultFieldIndex(name, fields, flag)
	if len(indices) == 0 {
		return nil, errors.Errorf("unknown field %s", name)
	}
	index := indices[0]
	return row[index], nil
}

// This is not used, should be removed???
type selectIndexDefaultPlan struct {
	nm string
	x  interface{}
}

// Explain implements plan.Plan Explain interface.
func (r *selectIndexDefaultPlan) Explain(w format.Formatter) {
	w.Format("┌Iterate all values of index %q\n└Output field names N/A\n", r.nm)
}

// Filter implements plan.Plan Filter interface.
func (r *selectIndexDefaultPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return r, false, nil
}

// Do implements plan.Plan Do interface.
func (r *selectIndexDefaultPlan) Do(ctx context.Context, f plan.RowIterFunc) (err error) {
	var x kv.Index
	switch ix := r.x.(type) {
	case *column.IndexedCol:
		x = ix.X
	default:
		panic("should never happen")
	}

	txn, err := ctx.GetTxn(false)
	if err != nil {
		return err
	}
	en, err := x.SeekFirst(txn)
	if err != nil {
		return types.EOFAsNil(err)
	}
	defer en.Close()

	var id int64
	for {
		k, _, err := en.Next()
		if err != nil {
			return types.EOFAsNil(err)
		}

		id++
		if more, err := f(id, k); !more || err != nil {
			return err
		}
	}
}

// GetFields implements plan.Plan GetFields interface.
func (r *selectIndexDefaultPlan) GetFields() []*field.ResultField {
	return []*field.ResultField{&field.ResultField{Name: r.nm}}
}

// NullPlan is empty plan, if we can affirm that the resultset is empty, we
// could just return a NullPlan directly, no need to scan any table. e.g.
// SELECT * FROM t WHERE i > 0 and i < 0;
type NullPlan struct {
	Fields []*field.ResultField
}

// GetFields implements plan.Plan GetFields interface.
func (r *NullPlan) GetFields() []*field.ResultField { return r.Fields }

// Explain implements plan.Plan Explain interface.
func (r *NullPlan) Explain(w format.Formatter) {
	w.Format("┌Iterate no rows\n└Output field names %v\n", field.RFQNames(r.Fields))
}

// Do implements plan.Plan Do interface. Do nothing.
func (r *NullPlan) Do(context.Context, plan.RowIterFunc) error {
	return nil
}

// Filter implements plan.Plan Filter interface.
func (r *NullPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	return r, false, nil
}

// Set ResultField info according to values
// This is used for inferring calculated fields type/Flen/charset
// For example "select count(*) from t;" will return a ResultField with type TypeLonglong, charset binary and Flen 21.
func setResultFieldInfo(fields []*field.ResultField, values []interface{}) error {
	if len(fields) != len(values) {
		return errors.Errorf("Fields and Values length unmatch %d VS %d", len(fields), len(values))
	}
	for i, rf := range fields {
		if rf.Col.Tp == 0 {
			// 0 == TypeDecimal, Tp maybe uninitialized
			rf.Col.Charset = charset.CharsetBin
			rf.Col.Collate = charset.CharsetBin
			c := values[i]
			switch v := c.(type) {
			case int8, int16, int, int32, int64:
				rf.Col.Tp = mysql.TypeLonglong
			case uint8, uint16, uint, uint32, uint64:
				rf.Col.Tp = mysql.TypeLonglong
				rf.Col.Flag |= mysql.UnsignedFlag
			case float32, float64:
				rf.Col.Tp = mysql.TypeFloat
			case string:
				rf.Col.Tp = mysql.TypeVarchar
				rf.Col.Flen = len(v)
				rf.Col.Charset = mysql.DefaultCharset
				rf.Col.Collate = mysql.DefaultCollationName
			case []byte:
				rf.Col.Tp = mysql.TypeBlob
			case mysql.Time:
				rf.Col.Tp = v.Type
			case mysql.Duration:
				rf.Col.Tp = mysql.TypeDuration
			case mysql.Decimal:
				rf.Col.Tp = mysql.TypeDecimal
			default:
				return errors.Errorf("Unknown type %T", c)
			}
			if rf.Col.Flen == 0 {
				rf.Col.Flen = mysql.GetDefaultFieldLength(rf.Col.Tp)
			}
			// TODO: set flags
		}
	}
	return nil
}

// RowKeyEntry is designed for Delete statement in multi-table mode,
// we should know which table this row comes from
type RowKeyEntry struct {
	// The table which this row come from
	Tbl table.Table
	// Row handle
	Key string
}

// RowKeyList is list of RowKeyEntry
type RowKeyList struct {
	Keys []*RowKeyEntry
}

func (rks *RowKeyList) appendKeys(keys ...*RowKeyEntry) {
	for _, key := range keys {
		rks.Keys = append(rks.Keys, key)
	}
}
