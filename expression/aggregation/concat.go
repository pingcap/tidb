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

package aggregation

import (
	"bytes"
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

type concatFunction struct {
	aggFunction
}

// Clone implements Aggregation interface.
func (cf *concatFunction) Clone() Aggregation {
	nf := *cf
	for i, arg := range cf.Args {
		nf.Args[i] = arg.Clone()
	}
	nf.resultMapper = make(aggCtxMapper)
	return &nf
}

// GetType implements Aggregation interface.
func (cf *concatFunction) GetType() *types.FieldType {
	return types.NewFieldType(mysql.TypeVarString)
}

func (cf *concatFunction) writeValue(ctx *aggEvaluateContext, val types.Datum) {
	if val.Kind() == types.KindBytes {
		ctx.Buffer.Write(val.GetBytes())
	} else {
		ctx.Buffer.WriteString(fmt.Sprintf("%v", val.GetValue()))
	}
}

// Update implements Aggregation interface.
func (cf *concatFunction) Update(row []types.Datum, groupKey []byte, sc *variable.StatementContext) error {
	ctx := cf.getContext(groupKey)
	cf.datumBuf = cf.datumBuf[:0]
	for _, a := range cf.Args {
		value, err := a.Eval(row)
		if err != nil {
			return errors.Trace(err)
		}
		if value.GetValue() == nil {
			return nil
		}
		cf.datumBuf = append(cf.datumBuf, value)
	}
	if cf.Distinct {
		d, err := ctx.DistinctChecker.Check(cf.datumBuf)
		if err != nil {
			return errors.Trace(err)
		}
		if !d {
			return nil
		}
	}
	if ctx.Buffer == nil {
		ctx.Buffer = &bytes.Buffer{}
	} else {
		// now use comma separator
		ctx.Buffer.WriteString(",")
	}
	for _, val := range cf.datumBuf {
		cf.writeValue(ctx, val)
	}
	// TODO: if total length is greater than global var group_concat_max_len, truncate it.
	return nil
}

// StreamUpdate implements Aggregation interface.
func (cf *concatFunction) StreamUpdate(row []types.Datum, sc *variable.StatementContext) error {
	ctx := cf.getStreamedContext()
	cf.datumBuf = cf.datumBuf[:0]
	for _, a := range cf.Args {
		value, err := a.Eval(row)
		if err != nil {
			return errors.Trace(err)
		}
		if value.GetValue() == nil {
			return nil
		}
		cf.datumBuf = append(cf.datumBuf, value)
	}
	if cf.Distinct {
		d, err := ctx.DistinctChecker.Check(cf.datumBuf)
		if err != nil {
			return errors.Trace(err)
		}
		if !d {
			return nil
		}
	}
	if ctx.Buffer == nil {
		ctx.Buffer = &bytes.Buffer{}
	} else {
		// now use comma separator
		ctx.Buffer.WriteString(",")
	}
	for _, val := range cf.datumBuf {
		cf.writeValue(ctx, val)
	}
	// TODO: if total length is greater than global var group_concat_max_len, truncate it.
	return nil
}

// GetGroupResult implements Aggregation interface.
func (cf *concatFunction) GetGroupResult(groupKey []byte) (d types.Datum) {
	ctx := cf.getContext(groupKey)
	if ctx.Buffer != nil {
		d.SetString(ctx.Buffer.String())
	} else {
		d.SetNull()
	}
	return d
}

// GetPartialResult implements Aggregation interface.
func (cf *concatFunction) GetPartialResult(groupKey []byte) []types.Datum {
	return []types.Datum{cf.GetGroupResult(groupKey)}
}

// GetStreamResult implements Aggregation interface.
func (cf *concatFunction) GetStreamResult() (d types.Datum) {
	if cf.streamCtx == nil {
		return
	}
	if cf.streamCtx.Buffer != nil {
		d.SetString(cf.streamCtx.Buffer.String())
	} else {
		d.SetNull()
	}
	cf.streamCtx = nil
	return
}
