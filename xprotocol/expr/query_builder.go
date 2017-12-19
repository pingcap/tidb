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

package expr

import (
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tipb/go-mysqlx/Expr"
	log "github.com/sirupsen/logrus"
)

type queryBuilder struct {
	str          string
	inQuoted     bool
	inIdentifier bool
}

func (qb *queryBuilder) Bquote() *queryBuilder {
	qb.str += "'"
	qb.inQuoted = true
	return qb
}

func (qb *queryBuilder) Equote() *queryBuilder {
	qb.str += "'"
	qb.inQuoted = true
	return qb
}

func (qb *queryBuilder) Bident() *queryBuilder {
	qb.str += "`"
	qb.inIdentifier = true
	return qb
}

func (qb *queryBuilder) Eident() *queryBuilder {
	qb.str += "`"
	qb.inIdentifier = true
	return qb
}

func (qb *queryBuilder) dot() *queryBuilder {
	return qb.put(".")
}

func (qb *queryBuilder) put(i interface{}) *queryBuilder {
	switch v := i.(type) {
	case int64:
		qb.str += strconv.FormatInt(v, 10)
	case uint64:
		qb.str += strconv.FormatUint(v, 10)
	case uint32:
		qb.str += strconv.FormatUint(uint64(v), 10)
	case float64:
		qb.str += strconv.FormatFloat(v, 'g', -1, 64)
	case float32:
		qb.str += strconv.FormatFloat(float64(v), 'g', -1, 64)
	case string:
		qb.str += v
	case []byte:
		if qb.inQuoted {
			v = escapeString(v)
		} else if qb.inIdentifier {
			v = escapeIdentifier(v)
		}
		str := string(v)
		qb.str += str
	default:
		log.Panicf("can not put this value")
	}
	return qb
}

func escapeString(b []byte) []byte {
	for i := range b {
		b[i] = escapeChar(b[i])
	}
	return b
}

func escapeIdentifier(b []byte) []byte {
	return b
}

func escapeChar(c byte) byte {
	switch c {
	case 0:
		return '0'
	case '\n':
		return 'n'
	case '\r':
		return 'r'
	case '\032':
		return 'Z'
	}
	return c
}

func (qb *queryBuilder) QuoteString(str string) *queryBuilder {
	return qb.put(util.QuoteString(str))
}

// ConcatExpr contains expressions which needed to be concat together.
type ConcatExpr struct {
	expr interface{}
	*GeneratorInfo
}

// NewConcatExpr returns a new ConcatExpr pointer.
func NewConcatExpr(expr interface{}, i *GeneratorInfo) *ConcatExpr {
	return &ConcatExpr{
		expr: expr,
		GeneratorInfo: &GeneratorInfo{
			args:          i.args,
			defaultSchema: i.defaultSchema,
			isRelation:    i.isRelation,
		},
	}
}

// AddExpr executes add operation.
func AddExpr(e interface{}) (*string, error) {
	var g generator
	c := e.(*ConcatExpr)

	switch v := c.expr.(type) {
	case *Mysqlx_Expr.Expr:
		g = &expr{c.GeneratorInfo, v}
	case *Mysqlx_Expr.Identifier:
		g = &ident{c.GeneratorInfo, v, false}
	case []*Mysqlx_Expr.DocumentPathItem:
		g = &docPathArray{c.GeneratorInfo, v}
	case *Mysqlx_Expr.Object_ObjectField:
		g = &objectField{c.GeneratorInfo, v}
	case *Mysqlx_Datatypes.Any:
		g = &any{c.GeneratorInfo, v}
	case *Mysqlx_Datatypes.Scalar:
		g = &scalar{c.GeneratorInfo, v}
	case *Mysqlx_Datatypes.Scalar_Octets:
		g = &scalarOctets{c.GeneratorInfo, v}
	case *Mysqlx_Expr.ColumnIdentifier:
		g = &columnIdent{c.GeneratorInfo, v}
	case int64, uint64, uint32, float64, float32, string, []byte:
		baseQB := &queryBuilder{"", false, false}
		baseQB.put(v)
		return &(baseQB.str), nil
	default:
		log.Panicf("not supported type")
	}

	qb, err := g.generate(&queryBuilder{"", false, false})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &qb.str, nil
}

func addUnquoteExpr(c interface{}) (*string, error) {
	str := ""
	gen, err := AddExpr(c)
	if err != nil {
		return nil, errors.Trace(err)
	}
	concat := c.(*ConcatExpr)
	e := concat.expr.(*Mysqlx_Expr.Expr)
	if e.GetType() == Mysqlx_Expr.Expr_IDENT && len(e.Identifier.GetDocumentPath()) > 0 {
		str = "JSON_UNQUOTE(" + *gen + ")"
	} else {
		str = *gen
	}
	return &str, nil
}

// AddForEach concats each expression.
func AddForEach(ca interface{}, f func(c interface{}) (*string, error), seq string) (*string, error) {
	cs, ok := ca.([]interface{})
	str := ""
	if !ok || len(cs) == 0 {
		return &str, nil
	}
	for _, c := range cs[:len(cs)-1] {
		gen, err := f(c)
		if err != nil {
			return nil, errors.Trace(err)
		}
		str += *gen + seq
	}
	gen, err := f(cs[len(cs)-1])
	if err != nil {
		return nil, errors.Trace(err)
	}
	str += *gen
	return &str, nil
}
