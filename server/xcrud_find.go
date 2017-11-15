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

package server

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/xprotocol/expr"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx/Crud"
	"github.com/pingcap/tipb/go-mysqlx/Expr"
)

const derivedTableName = "`_DERIVED_TABLE_`"

type findBuilder struct {
	baseBuilder
}

func (b *findBuilder) build(payload []byte) (*string, error) {
	var msg Mysqlx_Crud.Find
	if err := msg.Unmarshal(payload); err != nil {
		return nil, util.ErrXBadMessage
	}
	b.GeneratorInfo = expr.NewGenerator(msg.GetArgs(), msg.GetCollection().GetSchema(), msg.GetDataModel() == Mysqlx_Crud.DataModel_TABLE)

	target := ""
	if msg.GetDataModel() != Mysqlx_Crud.DataModel_TABLE && len(msg.GetGrouping()) > 0 {
		gen, err := b.addDocStmtWithGrouping(&msg)
		if err != nil {
			return nil, errors.Trace(err)
		}
		target += *gen
	} else {
		gen, err := b.addStmtCommon(&msg)
		if err != nil {
			return nil, errors.Trace(err)
		}
		target += *gen
	}
	return &target, nil
}

func (b *findBuilder) concatSQL(msg *Mysqlx_Crud.Find) (*string, error) {
	target := " FROM " + *(b.addCollection(msg.GetCollection()))

	if c := msg.GetCriteria(); c != nil {
		gen, err := b.addFilter(c)
		if err != nil {
			return nil, errors.Trace(err)
		}
		target += *gen
	}

	if g := msg.GetGrouping(); g != nil {
		gen, err := b.addGrouping(g)
		if err != nil {
			return nil, errors.Trace(err)
		}
		target += *gen
	}

	if gc := msg.GetGroupingCriteria(); gc != nil {
		gen, err := b.addGroupingCriteria(gc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		target += *gen
	}

	if o := msg.GetOrder(); o != nil {
		gen, err := b.addOrder(o)
		if err != nil {
			return nil, errors.Trace(err)
		}
		target += *gen
	}

	if l := msg.GetLimit(); l != nil {
		gen, err := b.addLimit(l, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		target += *gen
	}
	return &target, nil
}

func (b *findBuilder) addDocStmtWithGrouping(msg *Mysqlx_Crud.Find) (*string, error) {
	if len(msg.GetProjection()) == 0 {
		return nil, util.ErrXBadProjection.Gen("Invalid empty projection list for grouping")
	}
	target := "SELECT "
	gen, err := b.addDocObject(msg.GetProjection(), b.addDocPrimaryProjectionItem)
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen + " FROM (" + "SELECT "
	gen, err = b.addTableProjection(msg.GetProjection())
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen

	gen, err = b.concatSQL(msg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen + ") AS " + derivedTableName
	return &target, nil
}

func (b *findBuilder) addStmtCommon(msg *Mysqlx_Crud.Find) (*string, error) {
	target := "SELECT "
	if msg.GetDataModel() == Mysqlx_Crud.DataModel_TABLE {
		gen, err := b.addTableProjection(msg.GetProjection())
		if err != nil {
			return nil, errors.Trace(err)
		}
		target += *gen
	} else {
		gen, err := b.addDocProjection(msg.GetProjection())
		if err != nil {
			return nil, errors.Trace(err)
		}
		target += *gen
	}
	gen, err := b.concatSQL(msg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen
	return &target, nil
}

func (b *findBuilder) addTableProjection(pl []*Mysqlx_Crud.Projection) (*string, error) {
	target := ""
	if len(pl) == 0 {
		target += "*"
		return &target, nil
	}
	cs := make([]interface{}, len(pl))
	for i, d := range pl {
		cs[i] = d
	}
	gen, err := expr.AddForEach(cs, b.addTableProjectionItem, ",")
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen
	return &target, nil
}

func (b *findBuilder) addTableProjectionItem(i interface{}) (*string, error) {
	p := i.(*Mysqlx_Crud.Projection)
	target := ""
	gen, err := expr.AddExpr(expr.NewConcatExpr(p.GetSource(), b.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen + *(b.addAlias(p))
	return &target, nil
}

func (b *findBuilder) addDocProjection(pl []*Mysqlx_Crud.Projection) (*string, error) {
	target := ""
	if len(pl) == 0 {
		target += "doc"
		return &target, nil
	}
	if len(pl) == 1 &&
		len(pl[0].GetAlias()) == 0 &&
		pl[0].GetSource().GetType() == Mysqlx_Expr.Expr_OBJECT {
		gen, err := expr.AddExpr(expr.NewConcatExpr(pl[0].GetSource(), b.GeneratorInfo))
		if err != nil {
			return nil, errors.Trace(err)
		}
		target += *gen + " AS doc"
		return &target, nil
	}
	gen, err := b.addDocObject(pl, b.addDocProjectionItem)
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen
	return &target, nil
}

func (b *findBuilder) addDocObject(pl []*Mysqlx_Crud.Projection, adder func(c interface{}) (*string, error)) (*string, error) {
	target := "JSON_OBJECT("
	cs := make([]interface{}, len(pl))
	for i, d := range pl {
		cs[i] = d
	}
	gen, err := expr.AddForEach(cs, adder, ",")
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen + ") AS doc"
	return &target, nil
}

func (b *findBuilder) addDocProjectionItem(i interface{}) (*string, error) {
	p := i.(*Mysqlx_Crud.Projection)
	if len(p.GetAlias()) == 0 {
		return nil, util.ErrXProjBadKeyName.Gen("Invalid projection target name")
	}
	target := util.QuoteString(p.GetAlias()) + ", "
	gen, err := expr.AddExpr(expr.NewConcatExpr(p.GetSource(), b.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen
	return &target, nil
}

func (b *findBuilder) addDocPrimaryProjectionItem(i interface{}) (*string, error) {
	p := i.(*Mysqlx_Crud.Projection)
	if len(p.GetAlias()) == 0 {
		return nil, util.ErrXProjBadKeyName.Gen("Invalid projection target name")
	}
	target := util.QuoteString(p.GetAlias()) + ", " + derivedTableName + "." + util.QuoteIdentifier(p.GetAlias())
	return &target, nil
}

func (b *findBuilder) addGrouping(gl []*Mysqlx_Expr.Expr) (*string, error) {
	target := ""
	if len(gl) > 0 {
		target += " GROUP BY "
		cs := make([]interface{}, len(gl))
		for i, d := range gl {
			cs[i] = d
		}
		gen, err := expr.AddForEach(cs, b.addExpr, ",")
		if err != nil {
			return nil, errors.Trace(err)
		}
		target += *gen
	}
	return &target, nil
}

func (b *findBuilder) addExpr(i interface{}) (*string, error) {
	e := i.(*Mysqlx_Expr.Expr)
	gen, err := expr.AddExpr(expr.NewConcatExpr(e, b.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return gen, nil
}

func (b *findBuilder) addGroupingCriteria(g *Mysqlx_Expr.Expr) (*string, error) {
	target := " HAVING "
	gen, err := b.addExpr(g)
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen
	return &target, nil
}
