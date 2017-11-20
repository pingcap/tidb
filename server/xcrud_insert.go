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
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
)

type insertBuilder struct {
	baseBuilder
}

func (b *insertBuilder) build(payload []byte) (*string, error) {
	var msg Mysqlx_Crud.Insert
	var isRelation bool

	if err := msg.Unmarshal(payload); err != nil {
		return nil, util.ErrXBadMessage
	}
	b.GeneratorInfo = expr.NewGenerator(msg.GetArgs(), msg.GetCollection().GetSchema(), msg.GetDataModel() == Mysqlx_Crud.DataModel_TABLE)

	projectionSize := 1
	if msg.GetDataModel() == Mysqlx_Crud.DataModel_TABLE {
		isRelation = true
		projectionSize = len(msg.Projection)
	}

	sqlQuery := "INSERT INTO "
	sqlQuery += *b.addCollection(msg.GetCollection())
	generatedField, err := b.addProjection(msg.GetProjection(), isRelation)
	if err != nil {
		return nil, err
	}
	sqlQuery += *generatedField

	generatedField, err = b.addValues(msg.GetRow(), projectionSize, isRelation, msg.GetArgs())
	if err != nil {
		return nil, err
	}
	sqlQuery += *generatedField

	return &sqlQuery, nil
}

func (b *insertBuilder) addProjection(p []*Mysqlx_Crud.Column, tableDataMode bool) (*string, error) {
	target := ""
	if tableDataMode {
		if len(p) != 0 {
			target += " (" + *p[0].Name
			if len(p) > 1 {
				for _, col := range p[1:] {
					target += ","
					target += *col.Name
				}
			}
			target += ")"
		}
	} else {
		if len(p) != 0 {
			return nil, util.ErrorMessage(util.CodeErrXBadProjection, "Invalid projection for document operation")
		}
		target += " (doc)"
	}
	return &target, nil
}

func (b *insertBuilder) addValues(c []*Mysqlx_Crud.Insert_TypedRow, projectionSize int, isRelation bool, msg []*Mysqlx_Datatypes.Scalar) (*string, error) {
	if len(c) == 0 {
		return nil, util.ErrorMessage(util.CodeErrXBadProjection, "Missing row data for Insert")
	}
	target := " VALUES "

	generatedField, err := b.addRow(c[0], projectionSize, isRelation, msg)
	if err != nil {
		return nil, err
	}

	target += *generatedField
	for _, row := range c[1:] {
		target += ","
		generatedField, err = b.addRow(row, projectionSize, isRelation, msg)
		if err != nil {
			return nil, err
		}
		target += *generatedField
	}

	return &target, nil
}

func (b *insertBuilder) addRow(row *Mysqlx_Crud.Insert_TypedRow, projectionSize int, isRelation bool, msg []*Mysqlx_Datatypes.Scalar) (*string, error) {
	if len(row.GetField()) == 0 || len(row.GetField()) != projectionSize {
		return nil, util.ErrorMessage(util.CodeErrXBadInsertData, "Wrong number of fields in row being inserted")
	}
	target := "("
	fields := row.GetField()
	cs := make([]interface{}, len(fields))
	for i, d := range fields {
		cs[i] = expr.NewConcatExpr(d, b.GeneratorInfo)
	}
	gen, err := expr.AddForEach(cs, expr.AddExpr, ",")
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen
	target += ")"
	return &target, nil
}
