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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

type concatFunction struct {
	aggFunction
	separator string
	sepInited bool
}

// Clone implements Aggregation interface.
func (cf *concatFunction) Clone() Aggregation {
	nf := *cf
	for i, arg := range cf.Args {
		nf.Args[i] = arg.Clone()
	}
	return &nf
}

// GetType implements Aggregation interface.
func (cf *concatFunction) GetType() *types.FieldType {
	return types.NewFieldType(mysql.TypeVarString)
}

func (cf *concatFunction) writeValue(ctx *AggEvaluateContext, val types.Datum) {
	if val.Kind() == types.KindBytes {
		ctx.Buffer.Write(val.GetBytes())
	} else {
		ctx.Buffer.WriteString(fmt.Sprintf("%v", val.GetValue()))
	}
}

func (cf *concatFunction) initSeparator(sc *stmtctx.StatementContext, row types.Row) error {
	sepArg := cf.Args[len(cf.Args)-1]
	sep, isNull, err := sepArg.EvalString(row, sc)
	if err != nil {
		return errors.Trace(err)
	}
	if isNull {
		return errors.Errorf("Invalid separator argument.")
	}
	cf.separator = sep
	cf.Args = cf.Args[:len(cf.Args)-1]
	return nil
}

// Update implements Aggregation interface.
func (cf *concatFunction) Update(ctx *AggEvaluateContext, sc *stmtctx.StatementContext, row types.Row) error {
	datumBuf := make([]types.Datum, 0, len(cf.Args))
	if !cf.sepInited {
		err := cf.initSeparator(sc, row)
		if err != nil {
			return errors.Trace(err)
		}
		cf.sepInited = true
	}
	for _, a := range cf.Args {
		value, err := a.Eval(row)
		if err != nil {
			return errors.Trace(err)
		}
		if value.GetValue() == nil {
			return nil
		}
		datumBuf = append(datumBuf, value)
	}
	if cf.Distinct {
		d, err := ctx.DistinctChecker.Check(datumBuf)
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
		ctx.Buffer.WriteString(cf.separator)
	}
	for _, val := range datumBuf {
		cf.writeValue(ctx, val)
	}
	// TODO: if total length is greater than global var group_concat_max_len, truncate it.
	return nil
}

// GetResult implements Aggregation interface.
func (cf *concatFunction) GetResult(ctx *AggEvaluateContext) (d types.Datum) {
	if ctx.Buffer != nil {
		d.SetString(ctx.Buffer.String())
	} else {
		d.SetNull()
	}
	return d
}

// GetPartialResult implements Aggregation interface.
func (cf *concatFunction) GetPartialResult(ctx *AggEvaluateContext) []types.Datum {
	return []types.Datum{cf.GetResult(ctx)}
}
