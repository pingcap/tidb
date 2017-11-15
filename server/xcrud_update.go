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
	"github.com/pingcap/tidb/xprotocol/expr"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx/Crud"
	"github.com/pingcap/tipb/go-mysqlx/Expr"
)

type updateBuilder struct {
	baseBuilder
}

func (b *updateBuilder) build(payload []byte) (*string, error) {
	var msg Mysqlx_Crud.Update
	var isRelation bool

	if err := msg.Unmarshal(payload); err != nil {
		return nil, util.ErrXBadMessage
	}
	b.GeneratorInfo = expr.NewGenerator(msg.GetArgs(), msg.GetCollection().GetSchema(), msg.GetDataModel() == Mysqlx_Crud.DataModel_TABLE)

	if msg.GetDataModel() == Mysqlx_Crud.DataModel_TABLE {
		isRelation = true
	}

	sqlQuery := "UPDATE "
	sqlQuery += *b.addCollection(msg.GetCollection())
	generatedField, err := b.addOperation(msg.GetOperation(), isRelation)
	if err != nil {
		return nil, err
	}
	sqlQuery += *generatedField

	if c := msg.GetCriteria(); c != nil {
		generatedField, err = b.addFilter(c)
		if err != nil {
			return nil, err
		}
		sqlQuery += *generatedField
	}

	if ol := msg.GetOrder(); ol != nil {
		generatedField, err = b.addOrder(ol)
		if err != nil {
			return nil, err
		}
		sqlQuery += *generatedField
	}

	if l := msg.GetLimit(); l != nil {
		generatedField, err = b.addLimit(l, true)
		if err != nil {
			return nil, err
		}
		sqlQuery += *generatedField
	}

	return &sqlQuery, nil
}

func (b *updateBuilder) addOperation(operations []*Mysqlx_Crud.UpdateOperation,
	tableDataMode bool) (*string, error) {
	if len(operations) == 0 {
		return nil, util.ErrXBadUpdateData.GenByArgs("Invalid update expression list")
	}

	target := " SET "

	var generatedField *string
	var err error
	if tableDataMode {
		generatedField, err = b.addTableOperation(operations)
	} else {
		generatedField, err = b.addDocumentOperation(operations)
	}

	if err != nil {
		return nil, err
	}
	target += *generatedField

	return &target, nil
}

func (b *updateBuilder) addTableOperation(operations []*Mysqlx_Crud.UpdateOperation) (*string, error) {
	begin := 0
	end := findIfNotEqual(operations)
	generatedField, err := b.addTableOperationItems(operations[0 : end+1])
	if err != nil {
		return nil, err
	}
	target := *generatedField

	for {
		if end == len(operations)-1 {
			break
		}

		begin = end
		end = findIfNotEqual(operations[begin:])
		generatedField, err = b.addTableOperationItems(operations[begin : end+1])
		if err != nil {
			return nil, err
		}
		target += ","
		target += *generatedField
	}

	return &target, nil
}

func (b *updateBuilder) addTableOperationItems(operations []*Mysqlx_Crud.UpdateOperation) (*string, error) {
	begin := operations[0]
	if begin.GetSource().GetSchemaName() != "" ||
		begin.GetSource().GetTableName() != "" ||
		begin.GetSource().GetName() == "" {
		return nil, util.ErrXBadColumnToUpdate.GenByArgs("Invalid column name to update")
	}

	target := ""
	switch begin.GetOperation() {
	case Mysqlx_Crud.UpdateOperation_SET:
		if len(begin.GetSource().GetDocumentPath()) != 0 {
			return nil, util.ErrXBadColumnToUpdate.GenByArgs("Invalid column name to update")
		}

		cs := make([]interface{}, len(operations))
		for i, d := range operations {
			cs[i] = d
		}
		gen, err := expr.AddForEach(cs, b.addFieldWithValue, ",")
		if err != nil {
			return nil, err
		}
		target += *gen

		return &target, nil
	case Mysqlx_Crud.UpdateOperation_ITEM_REMOVE:
		target += util.QuoteIdentifier(begin.GetSource().GetName())
		target += "=JSON_REMOVE("
		target += util.QuoteIdentifier(begin.GetSource().GetName())

		cs := make([]interface{}, len(operations))
		for i, d := range operations {
			cs[i] = d
		}
		gen, err := expr.AddForEach(cs, b.addMember, "")
		if err != nil {
			return nil, err
		}
		target += *gen

		target += ")"
		return &target, nil
	case Mysqlx_Crud.UpdateOperation_ITEM_SET:
		target += util.QuoteIdentifier(begin.GetSource().GetName())
		target += "=JSON_SET("
		target += util.QuoteIdentifier(begin.GetSource().GetName())

		cs := make([]interface{}, len(operations))
		for i, d := range operations {
			cs[i] = d
		}
		gen, err := expr.AddForEach(cs, b.addMemberWithValue, "")
		if err != nil {
			return nil, err
		}
		target += *gen

		target += ")"
		return &target, nil
	case Mysqlx_Crud.UpdateOperation_ITEM_REPLACE:
		target += util.QuoteIdentifier(begin.GetSource().GetName())
		target += "=JSON_REPLACE("
		target += util.QuoteIdentifier(begin.GetSource().GetName())

		cs := make([]interface{}, len(operations))
		for i, d := range operations {
			cs[i] = d
		}
		gen, err := expr.AddForEach(cs, b.addMemberWithValue, "")
		if err != nil {
			return nil, err
		}
		target += *gen

		target += ")"
		return &target, nil
	case Mysqlx_Crud.UpdateOperation_ITEM_MERGE:
		target += util.QuoteIdentifier(begin.GetSource().GetName())
		target += "=JSON_MERGE("
		target += util.QuoteIdentifier(begin.GetSource().GetName())

		cs := make([]interface{}, len(operations))
		for i, d := range operations {
			cs[i] = d
		}
		gen, err := expr.AddForEach(cs, b.addValue, "")
		if err != nil {
			return nil, err
		}
		target += *gen

		target += ")"
		return &target, nil
	case Mysqlx_Crud.UpdateOperation_ARRAY_INSERT:
		target += util.QuoteIdentifier(begin.GetSource().GetName())
		target += "=JSON_ARRAY_INSERT("
		target += util.QuoteIdentifier(begin.GetSource().GetName())

		cs := make([]interface{}, len(operations))
		for i, d := range operations {
			cs[i] = d
		}
		gen, err := expr.AddForEach(cs, b.addMemberWithValue, "")
		if err != nil {
			return nil, err
		}
		target += *gen

		target += ")"
		return &target, nil
	case Mysqlx_Crud.UpdateOperation_ARRAY_APPEND:
		target += util.QuoteIdentifier(begin.GetSource().GetName())
		target += "=JSON_ARRAY_APPEND("
		target += util.QuoteIdentifier(begin.GetSource().GetName())

		cs := make([]interface{}, len(operations))
		for i, d := range operations {
			cs[i] = d
		}
		gen, err := expr.AddForEach(cs, b.addMemberWithValue, "")
		if err != nil {
			return nil, err
		}
		target += *gen

		target += ")"
		return &target, nil
	default:
		return nil, util.ErrXBadTypeOfUpdate.GenByArgs("Invalid type of update operations for table")
	}
}

func (b *updateBuilder) addMember(c interface{}) (*string, error) {
	operation, ok := c.(*Mysqlx_Crud.UpdateOperation)
	if !ok {
		return nil, util.ErrXBadColumnToUpdate.GenByArgs("Invalid column name to update")
	}

	if len(operation.GetSource().GetDocumentPath()) == 0 {
		return nil, util.ErrXBadMemberToUpdate.GenByArgs("Invalid member location")
	}

	target := ","

	gen, err := expr.AddExpr(expr.NewConcatExpr(operation.GetSource().GetDocumentPath(), b.GeneratorInfo))
	if err != nil {
		return nil, err
	}
	target += *gen

	return &target, nil
}

func (b *updateBuilder) addValue(c interface{}) (*string, error) {
	operation, ok := c.(*Mysqlx_Crud.UpdateOperation)
	if !ok {
		return nil, util.ErrXBadColumnToUpdate.GenByArgs("Invalid column name to update")
	}

	target := ","
	gen, err := expr.AddExpr(expr.NewConcatExpr(operation.GetValue(), b.GeneratorInfo))
	if err != nil {
		return nil, err
	}
	target += *gen

	return &target, nil
}

func (b *updateBuilder) addMemberWithValue(c interface{}) (*string, error) {
	gen, err := b.addMember(c)
	if err != nil {
		return nil, err
	}
	target := *gen

	gen, err = b.addValue(c)
	if err != nil {
		return nil, err
	}
	target += *gen
	return &target, nil
}

func (b *updateBuilder) addFieldWithValue(c interface{}) (*string, error) {
	operation, ok := c.(*Mysqlx_Crud.UpdateOperation)
	if !ok {
		return nil, util.ErrXBadColumnToUpdate.GenByArgs("Invalid column name to update")
	}

	target := ""
	gen, err := expr.AddExpr(expr.NewConcatExpr(operation.GetSource(), b.GeneratorInfo))
	if err != nil {
		return nil, err
	}

	target += *gen + "="

	gen, err = expr.AddExpr(expr.NewConcatExpr(operation.GetValue(), b.GeneratorInfo))
	if err != nil {
		return nil, err
	}

	target += *gen
	return &target, nil

}

func findIfNotEqual(operation []*Mysqlx_Crud.UpdateOperation) int {
	if len(operation) == 1 {
		return 0
	}
	b := operation[0]
	for i, op := range operation[1:] {
		if op.GetSource().GetName() != b.GetSource().GetName() &&
			op.GetOperation() != b.GetOperation() {
			return i + 1
		}
	}
	return len(operation) - 1
}

func (b *updateBuilder) addDocumentOperation(operations []*Mysqlx_Crud.UpdateOperation) (*string, error) {
	prev := Mysqlx_Crud.UpdateOperation_UpdateType(-1)
	target := "doc="

	for i := len(operations) - 1; i >= 0; i-- {
		op := operations[i].GetOperation()
		if prev == op {
			continue
		}

		switch op {
		case Mysqlx_Crud.UpdateOperation_ITEM_REMOVE:
			target += "JSON_REMOVE("
		case Mysqlx_Crud.UpdateOperation_ITEM_SET:
			target += "JSON_SET("
		case Mysqlx_Crud.UpdateOperation_ITEM_REPLACE:
			target += "JSON_REPLACE("
		case Mysqlx_Crud.UpdateOperation_ITEM_MERGE:
			target += "JSON_MERGE("
		case Mysqlx_Crud.UpdateOperation_ARRAY_INSERT:
			target += "JSON_ARRAY_INSERT("
		case Mysqlx_Crud.UpdateOperation_ARRAY_APPEND:
			target += "JSON_ARRAY_APPEND("
		default:
			return nil, util.ErrXBadTypeOfUpdate.GenByArgs("Invalid type of update operations for document")
		}
		prev = op
	}

	target += "doc"
	bi := 0
	prev = operations[0].GetOperation()
	for i, op := range operations {
		if prev == op.GetOperation() {
			continue
		}

		cs := make([]interface{}, len(operations[bi:i]))
		for j, d := range operations[bi:i] {
			cs[j] = d
		}
		gen, err := expr.AddForEach(cs, b.addDocumentOperationItem, "")
		if err != nil {
			return nil, err
		}
		target += *gen + ")"

		bi = i
		prev = op.GetOperation()
	}

	cs := make([]interface{}, len(operations[bi:]))
	for j, d := range operations[bi:] {
		cs[j] = d
	}
	gen, err := expr.AddForEach(cs, b.addDocumentOperationItem, "")
	if err != nil {
		return nil, err
	}
	target += *gen + ")"

	return &target, nil
}

func (b *updateBuilder) addDocumentOperationItem(c interface{}) (*string, error) {
	operation, ok := c.(*Mysqlx_Crud.UpdateOperation)
	if !ok {
		return nil, util.ErrXBadColumnToUpdate.GenByArgs("Invalid column name to update")
	}

	target := ""
	if operation.GetSource().GetSchemaName() != "" ||
		operation.GetSource().GetTableName() != "" ||
		operation.GetSource().GetName() != "" {
		return nil, util.ErrXBadColumnToUpdate.GenByArgs("Invalid column name to update")
	}

	if operation.GetOperation() != Mysqlx_Crud.UpdateOperation_ITEM_MERGE {
		if len(operation.GetSource().GetDocumentPath()) == 0 ||
			(operation.GetSource().GetDocumentPath()[0].GetType() != Mysqlx_Expr.DocumentPathItem_MEMBER &&
				operation.GetSource().GetDocumentPath()[0].GetType() != Mysqlx_Expr.DocumentPathItem_MEMBER_ASTERISK) {
			return nil, util.ErrXBadMemberToUpdate.GenByArgs("Invalid document member location")
		}

		if len(operation.GetSource().GetDocumentPath()) == 1 &&
			operation.GetSource().GetDocumentPath()[0].GetType() == Mysqlx_Expr.DocumentPathItem_MEMBER &&
			operation.GetSource().GetDocumentPath()[0].GetValue() == "_id" {
			return nil, util.ErrXBadColumnToUpdate.GenByArgs("Forbidden update operation on '$._id' member")
		}
		target += ","

		gen, err := expr.AddExpr(expr.NewConcatExpr(operation.GetSource().GetDocumentPath(), b.GeneratorInfo))
		if err != nil {
			return nil, err
		}
		target += *gen
	}

	switch operation.GetOperation() {
	case Mysqlx_Crud.UpdateOperation_ITEM_REMOVE:
		if operation.GetValue() != nil {
			return nil, util.ErrXBadUpdateData.GenByArgs("Unexpected value argument for ITEM_REMOVE operation")
		}
	case Mysqlx_Crud.UpdateOperation_ITEM_MERGE:
		gen, err := expr.AddExpr(expr.NewConcatExpr(operation.GetValue(), b.GeneratorInfo))
		if err != nil {
			return nil, err
		}
		target += ",IF(JSON_TYPE(" + *gen + ")='OBJECT',JSON_REMOVE(" + *gen + ",'$._id'),'_ERROR_')"
	default:
		target += ","

		gen, err := expr.AddExpr(expr.NewConcatExpr(operation.GetValue(), b.GeneratorInfo))
		if err != nil {
			return nil, err
		}
		target += *gen
	}

	return &target, nil
}
