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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tipb/go-mysqlx/Expr"
	log "github.com/sirupsen/logrus"
)

type generator interface {
	generate(*queryBuilder) (*queryBuilder, error)
}

// GeneratorInfo contains general members of any generator.
type GeneratorInfo struct {
	args          []*Mysqlx_Datatypes.Scalar
	defaultSchema string
	isRelation    bool
}

// NewGenerator assigns protobuf payload to get a new expression generator.
func NewGenerator(args []*Mysqlx_Datatypes.Scalar, defaultSchema string, isRelation bool) (b *GeneratorInfo) {
	return &GeneratorInfo{
		args:          args,
		defaultSchema: defaultSchema,
		isRelation:    isRelation,
	}
}

type expr struct {
	*GeneratorInfo
	expr *Mysqlx_Expr.Expr
}

func (e *expr) generate(qb *queryBuilder) (*queryBuilder, error) {
	var g generator

	expr := e.expr
	switch expr.GetType() {
	case Mysqlx_Expr.Expr_IDENT:
		g = &columnIdent{e.GeneratorInfo, expr.GetIdentifier()}
	case Mysqlx_Expr.Expr_LITERAL:
		g = &scalar{e.GeneratorInfo, expr.GetLiteral()}
	case Mysqlx_Expr.Expr_VARIABLE:
		g = &variable{e.GeneratorInfo, expr.GetVariable()}
	case Mysqlx_Expr.Expr_FUNC_CALL:
		g = &funcCall{e.GeneratorInfo, expr.GetFunctionCall()}
	case Mysqlx_Expr.Expr_OPERATOR:
		g = &operator{e.GeneratorInfo, expr.GetOperator()}
	case Mysqlx_Expr.Expr_PLACEHOLDER:
		g = &placeHolder{e.GeneratorInfo, expr.GetPosition()}
	case Mysqlx_Expr.Expr_OBJECT:
		g = &object{e.GeneratorInfo, expr.GetObject()}
	case Mysqlx_Expr.Expr_ARRAY:
		g = &array{e.GeneratorInfo, expr.GetArray()}
	default:
		log.Panicf("not supported type %s", expr.GetType().String())
	}
	return g.generate(qb)
}

type columnIdent struct {
	*GeneratorInfo
	identifier *Mysqlx_Expr.ColumnIdentifier
}

func (i *columnIdent) generate(qb *queryBuilder) (*queryBuilder, error) {
	schemaName := i.identifier.GetSchemaName()
	tableName := i.identifier.GetTableName()

	if schemaName != "" && tableName == "" {
		return nil, util.ErrorMessage(util.CodeErrXExprMissingArg,
			"Table name is required if schema name is specified in ColumnIdentifier.")
	}

	docPath := i.identifier.GetDocumentPath()
	name := i.identifier.GetName()
	if tableName == "" && name == "" && i.isRelation && (len(docPath) > 0) {
		return nil, util.ErrorMessage(util.CodeErrXExprMissingArg,
			"Column name is required if table name is specified in ColumnIdentifier.")
	}

	if len(docPath) > 0 {
		qb.put("JSON_EXTRACT(")
	}

	if schemaName != "" {
		qb.put(util.QuoteIdentifier(schemaName)).dot()
	}

	if tableName != "" {
		qb.put(util.QuoteIdentifier(tableName)).dot()
	}

	if name != "" {
		qb.put(util.QuoteIdentifier(name))
	}

	if len(docPath) > 0 {
		if name == "" {
			qb = qb.put("doc")
		}

		qb.put(",")
		generatedQuery, err := AddExpr(NewConcatExpr(docPath, i.GeneratorInfo))
		if err != nil {
			return nil, err
		}
		qb.put(*generatedQuery)
		qb.put(")")
	}
	return qb, nil
}

type scalar struct {
	*GeneratorInfo
	scalar *Mysqlx_Datatypes.Scalar
}

func (l *scalar) generate(qb *queryBuilder) (*queryBuilder, error) {
	literal := l.scalar
	switch literal.GetType() {
	case Mysqlx_Datatypes.Scalar_V_UINT:
		return qb.put(literal.GetVUnsignedInt()), nil
	case Mysqlx_Datatypes.Scalar_V_SINT:
		return qb.put(literal.GetVSignedInt()), nil
	case Mysqlx_Datatypes.Scalar_V_NULL:
		return qb.put("NULL"), nil
	case Mysqlx_Datatypes.Scalar_V_OCTETS:
		generatedQuery, err := AddExpr(NewConcatExpr(literal.GetVOctets(), l.GeneratorInfo))
		if err != nil {
			return nil, err
		}
		return qb.put(*generatedQuery), nil
	case Mysqlx_Datatypes.Scalar_V_STRING:
		if literal.GetVString().GetCollation() != 0 {
			//TODO: see line No. 231 in expr_generator.cc if the mysql's codes
		}
		return qb.QuoteString(string(literal.GetVString().GetValue())), nil
	case Mysqlx_Datatypes.Scalar_V_DOUBLE:
		return qb.put(literal.GetVDouble()), nil
	case Mysqlx_Datatypes.Scalar_V_FLOAT:
		return qb.put(literal.GetVFloat()), nil
	case Mysqlx_Datatypes.Scalar_V_BOOL:
		if literal.GetVBool() {
			return qb.put("TRUE"), nil
		}
		return qb.put("FALSE"), nil
	default:
		return nil, util.ErrorMessage(util.CodeErrXExprBadTypeValue,
			"Invalid value for Mysqlx::Datatypes::Scalar::Type "+literal.GetType().String())
	}
}

type variable struct {
	*GeneratorInfo
	variable string
}

func (v *variable) generate(qb *queryBuilder) (*queryBuilder, error) {
	return qb.put(v.variable), nil
}

type ident struct {
	*GeneratorInfo
	ident      *Mysqlx_Expr.Identifier
	isFunction bool
}

func (i *ident) generate(qb *queryBuilder) (*queryBuilder, error) {
	ident := i.ident
	if i.defaultSchema != "" && ident.GetSchemaName() == "" &&
		(!i.isFunction || expression.IsBuiltInFunc(ident.GetName())) {
		qb.put(util.QuoteIdentifierIfNeeded(i.defaultSchema)).dot()
	}

	if ident.GetSchemaName() != "" {
		qb.put(util.QuoteIdentifier(ident.GetSchemaName())).dot()
	}

	qb.put(util.QuoteIdentifierIfNeeded(ident.GetName()))

	return qb, nil
}

type funcCall struct {
	*GeneratorInfo
	functionCall *Mysqlx_Expr.FunctionCall
}

func (fc *funcCall) generate(qb *queryBuilder) (*queryBuilder, error) {
	functionCall := fc.functionCall

	generatedQuery, err := AddExpr(NewConcatExpr(functionCall.GetName(), fc.GeneratorInfo))
	if err != nil {
		return nil, err
	}
	qb.put(*generatedQuery)
	qb.put("(")

	params := functionCall.GetParam()
	cs := make([]interface{}, len(params))
	for i, d := range params {
		cs[i] = NewConcatExpr(d, fc.GeneratorInfo)
	}
	generatedQuery, err = AddForEach(cs, addUnquoteExpr, ",")
	qb.put(")")
	return qb, nil
}

type placeHolder struct {
	*GeneratorInfo
	position uint32
}

func (ph *placeHolder) generate(qb *queryBuilder) (*queryBuilder, error) {
	position := ph.position
	if position < uint32(len(ph.args)) {
		generatedQuery, err := AddExpr(NewConcatExpr(ph.args[position], ph.GeneratorInfo))
		if err != nil {
			return nil, err
		}
		return qb.put(*generatedQuery), nil
	}
	return nil, util.ErrXExprBadValue.GenByArgs("Invalid value of placeholder")
}

type objectField struct {
	*GeneratorInfo
	objectField *Mysqlx_Expr.Object_ObjectField
}

func (ob *objectField) generate(qb *queryBuilder) (*queryBuilder, error) {
	objectField := ob.objectField
	if objectField.GetKey() == "" {
		return nil, util.ErrXExprBadValue.GenByArgs("Invalid key for Mysqlx::Expr::Object")
	}

	if objectField.GetValue() == nil {
		return nil, util.ErrXExprBadValue.GenByArgs("Invalid value for Mysqlx::Expr::Object on key '" + objectField.GetKey() + "'")
	}
	qb.QuoteString(objectField.GetKey()).put(",")

	generatedQuery, err := AddExpr(NewConcatExpr(objectField.GetValue(), ob.GeneratorInfo))
	if err != nil {
		return nil, err
	}
	qb.put(*generatedQuery)

	return qb, nil
}

type object struct {
	*GeneratorInfo
	object *Mysqlx_Expr.Object
}

func (ob *object) generate(qb *queryBuilder) (*queryBuilder, error) {
	qb.put("JSON_OBJECT(")
	fields := ob.object.GetFld()
	cs := make([]interface{}, len(fields))
	for i, d := range fields {
		cs[i] = NewConcatExpr(d, ob.GeneratorInfo)
	}
	gen, err := AddForEach(cs, AddExpr, ",")
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(")")
	return qb, nil
}

type array struct {
	*GeneratorInfo
	array *Mysqlx_Expr.Array
}

func (a *array) generate(qb *queryBuilder) (*queryBuilder, error) {
	qb.put("JSON_ARRAY(")
	values := a.array.GetValue()
	cs := make([]interface{}, len(values))
	for i, d := range values {
		cs[i] = NewConcatExpr(d, a.GeneratorInfo)
	}
	gen, err := AddForEach(cs, AddExpr, ",")
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(")")
	return qb, nil
}

type docPathArray struct {
	*GeneratorInfo
	docPath []*Mysqlx_Expr.DocumentPathItem
}

func (d *docPathArray) generate(qb *queryBuilder) (*queryBuilder, error) {
	docPath := d.docPath
	if len(docPath) == 1 &&
		docPath[0].GetType() == Mysqlx_Expr.DocumentPathItem_MEMBER &&
		docPath[0].GetValue() == "" {
		qb.put(util.QuoteIdentifier("$"))
		return qb, nil
	}

	qb.Bquote().put("$")
	for _, item := range docPath {
		switch item.GetType() {
		case Mysqlx_Expr.DocumentPathItem_MEMBER:
			if item.GetValue() == "" {
				return nil, util.ErrorMessage(util.CodeErrXExprBadTypeValue,
					"Invalid empty value for Mysqlx::Expr::DocumentPathItem::MEMBER")
			}
			qb.put(".")
			qb.put(util.QuoteIdentifierIfNeeded(item.GetValue()))
		case Mysqlx_Expr.DocumentPathItem_MEMBER_ASTERISK:
			qb.put(".*")
		case Mysqlx_Expr.DocumentPathItem_ARRAY_INDEX:
			qb.put("[").put(item.GetIndex()).put("]")
		case Mysqlx_Expr.DocumentPathItem_ARRAY_INDEX_ASTERISK:
			qb.put("[*]")
		case Mysqlx_Expr.DocumentPathItem_DOUBLE_ASTERISK:
			qb.put("**")
		default:
			return nil, util.ErrorMessage(util.CodeErrXExprBadTypeValue,
				"Invalid value for Mysqlx::Expr::DocumentPathItem::Type ")
		}
	}

	qb.Equote()
	return qb, nil
}

const (
	ctPlain    = 0x0000 //   default value; general use of octets
	ctGeometry = 0x0001 //   BYTES  0x0001 GEOMETRY (WKB encoding)
	ctJSON     = 0x0002 //   BYTES  0x0002 JSON (text encoding)
	ctXML      = 0x0003 //   BYTES  0x0003 XML (text encoding)
)

type scalarOctets struct {
	*GeneratorInfo
	scalarOctets *Mysqlx_Datatypes.Scalar_Octets
}

func (so *scalarOctets) generate(qb *queryBuilder) (*queryBuilder, error) {
	scalarOctets := so.scalarOctets
	content := string(scalarOctets.GetValue())
	switch scalarOctets.GetContentType() {
	case ctPlain:
		return qb.QuoteString(content), nil
	case ctGeometry:
		return qb.put("ST_GEOMETRYFROMWKB(").QuoteString(content).put(")"), nil
	case ctJSON:
		return qb.put("CAST(").QuoteString(content).put(" AS JSON)"), nil
	case ctXML:
		return qb.QuoteString(content), nil
	default:
		return nil, util.ErrorMessage(util.CodeErrXExprBadTypeValue,
			"Invalid content type for Mysqlx::Datatypes::Scalar::Octets "+
				strconv.FormatUint(uint64(scalarOctets.GetContentType()), 10))
	}
}

type any struct {
	*GeneratorInfo
	any *Mysqlx_Datatypes.Any
}

func (a *any) generate(qb *queryBuilder) (*queryBuilder, error) {
	any := a.any
	switch any.GetType() {
	case Mysqlx_Datatypes.Any_SCALAR:
		generatedQuery, err := AddExpr(NewConcatExpr(any.GetScalar(), a.GeneratorInfo))
		if err != nil {
			return nil, err
		}
		return qb.put(*generatedQuery), nil
	default:
		return nil, util.ErrorMessage(util.CodeErrXExprBadTypeValue,
			"Invalid value for Mysqlx::Datatypes::Any::Type "+
				strconv.Itoa(int(any.GetType())))
	}
}
