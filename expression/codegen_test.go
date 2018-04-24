// Copyright 2018 PingCAP, Inc.
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

package expression

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tiancaiamao/shen-go/runtime"
)

var _ = Suite(&testCodeGenSuite{})

type testCodeGenSuite struct{}

func (t *testCodeGenSuite) SetUpSuite(c *C) {}

func (t *testCodeGenSuite) TearDownSuite(c *C) {}

func SexpToPB(o runtime.Obj) (*tipb.Expr, error) {
	var c convert
	expr := c.sexpToPB(o)
	return expr, c.err
}

type convert struct {
	err error
}

// sexpToPB converts a sexp representation to tipb.Expr.
func (c *convert) sexpToPB(o runtime.Obj) *tipb.Expr {
	if c.err != nil {
		return nil
	}

	if !runtime.IsPair(o) {
		c.err = errors.New("all expr should be pair")
		return nil
	}

	sym := runtime.Car(o)
	if !runtime.IsSymbol(sym) {
		c.err = errors.Errorf("car should be symbol, but get %s", runtime.ObjString(sym))
		return nil
	}

	str := runtime.GetSymbol(sym)
	switch str {
	case "int64":
		val := runtime.Cadr(o)
		return &tipb.Expr{
			Tp:  tipb.ExprType_Int64,
			Val: codec.EncodeInt(nil, int64(runtime.GetInteger(val))),
		}
	case "col":
		val := runtime.Cadr(o)
		// tp := runtime.Cadr(runtime.Cdr(o))
		return &tipb.Expr{
			Tp:  tipb.ExprType_ColumnRef,
			Val: codec.EncodeInt(nil, int64(runtime.GetInteger(val))),
		}
	}

	// Scalar Function.
	args := runtime.ListToSlice(runtime.Cdr(o))
	children := make([]*tipb.Expr, 0, len(args))
	for _, arg := range args {
		tmp := c.sexpToPB(arg)
		if c.err != nil {
			return nil
		}
		children = append(children, tmp)
	}
	sig, ok := tipb.ScalarFuncSig_value[str]
	if !ok {
		c.err = errors.Errorf("invalid function %s", str)
		return nil
	}
	return &tipb.Expr{
		Tp:       tipb.ExprType_ScalarFunc,
		Sig:      tipb.ScalarFuncSig(sig),
		Children: children,
	}
}

func PBToGo(expr string) string {
	return fmt.Sprintf(`package main
import "github.com/pingcap/tidb/types"

func LogicalAnd(a, b bool) bool {
	return a && b
}

func LTInt(a, b int64) bool {
	return a < b
}

func F(row types.Row) bool {
	return %s
}`, expr)
}

func (t *testCodeGenSuite) TestAll(c *C) {
	str := "(LogicalAnd (LTInt (col 2 int32) (int64 3)) (LTInt (col 2 int32) (int64 42)))"
	r := runtime.NewSexpReader(strings.NewReader(str))
	o, err := r.Read()
	c.Assert(err, IsNil)

	pb, err := SexpToPB(o)
	c.Assert(err, IsNil)

	expr, err := PBToGoExpr(pb)
	c.Assert(err, IsNil)

	goCode := PBToGo(expr)
	p, err := CompileAndLoad(goCode)
	c.Assert(err, IsNil)

	sym, err := p.Lookup("F")
	c.Assert(err, IsNil)
	f := sym.(func(row types.Row) bool)

	chunk := chunk.NewChunk([]*types.FieldType{
		types.NewFieldType(mysql.TypeLong),
		types.NewFieldType(mysql.TypeLong),
		types.NewFieldType(mysql.TypeLong),
	})
	chunk.AppendInt64(2, 42)
	row := chunk.GetRow(0)
	row.GetInt64(2)
	c.Assert(f(row), Equals, false)
}
