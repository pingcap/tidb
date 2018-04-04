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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx/Datatypes"
	"github.com/pingcap/tipb/go-mysqlx/Expr"
)

var nameMap = map[string]string{
	"!":           "!",
	"!=":          " != ",
	"%":           " % ",
	"&":           " & ",
	"&&":          " AND ",
	"*":           "",
	"+":           " + ",
	"-":           " - ",
	"/":           " / ",
	"<":           " < ",
	"<<":          " << ",
	"<=":          " <= ",
	"==":          " = ",
	">":           " > ",
	">=":          " >= ",
	">>":          " >> ",
	"^":           " ^ ",
	"between":     " BETWEEN ",
	"cast":        "",
	"date_add":    "DATE_ADD",
	"date_sub":    "DATE_SUB",
	"default":     "DEFAULT",
	"div":         " DIV ",
	"in":          "",
	"is":          " IS ",
	"is_not":      " IS NOT ",
	"like":        " LIKE ",
	"not":         "NOT ",
	"not_between": " NOT BETWEEN ",
	"not_in":      "NOT ",
	"not_like":    " NOT LIKE ",
	"not_regexp":  " NOT REGEXP ",
	"regexp":      " REGEXP ",
	"sign_minus":  "-",
	"sign_plus":   "+",
	"xor":         " XOR ",
	"|":           " | ",
	"||":          " OR ",
	"~":           "~",
}

type operator struct {
	*GeneratorInfo
	operator *Mysqlx_Expr.Operator
}

func (op *operator) generate(qb *queryBuilder) (q *queryBuilder, err error) {
	switch name := op.operator.GetName(); name {
	case "!", "not", "sign_minus", "sign_plus", "~":
		if q, err = op.unaryOperator(qb, nameMap[name]); err != nil {
			return nil, errors.Trace(err)
		}
	case "!=", "%", "&", "&&", "+", "-", "/", "<", "<<", "<=", "==", ">", ">=", ">>", "^",
		"div", "is", "is_not", "xor", "|", "||":
		if q, err = op.binaryOperator(qb, nameMap[name]); err != nil {
			return nil, errors.Trace(err)
		}
	case "*":
		if q, err = op.asteriskOperator(qb, nameMap[name]); err != nil {
			return nil, errors.Trace(err)
		}
	case "between", "not_between":
		if q, err = op.betweenExpression(qb, nameMap[name]); err != nil {
			return nil, errors.Trace(err)
		}
	case "cast":
		if q, err = op.castExpression(qb, nameMap[name]); err != nil {
			return nil, errors.Trace(err)
		}
	case "date_add", "date_sub":
		if q, err = op.dateExpression(qb, nameMap[name]); err != nil {
			return nil, errors.Trace(err)
		}
	case "default":
		if q, err = op.nullaryExpression(qb, nameMap[name]); err != nil {
			return nil, errors.Trace(err)
		}
	case "in", "not_in":
		if q, err = op.inExpression(qb, nameMap[name]); err != nil {
			return nil, errors.Trace(err)
		}
	case "like", "not_like":
		if q, err = op.likeExpression(qb, nameMap[name]); err != nil {
			return nil, errors.Trace(err)
		}
	case "not_regexp", "regexp":
		if q, err = op.binaryExpression(qb, nameMap[name]); err != nil {
			return nil, errors.Trace(err)
		}
	default:
		return nil, util.ErrXExprBadOperator.Gen("Invalid operator %s" + name)
	}
	return
}

// All the operators or expressions below are used by "type operator struct"
func (op *operator) unaryOperator(qb *queryBuilder, str string) (*queryBuilder, error) {
	params := op.operator.GetParam()
	if len(params) != 1 {
		return nil, util.ErrXExprBadNumArgs.Gen("Unary operations require exactly one operand in expression.")
	}
	qb.put("(")
	gen, err := AddExpr(NewConcatExpr(params[0], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(")")
	return qb, nil
}

func (op *operator) binaryOperator(qb *queryBuilder, str string) (*queryBuilder, error) {
	params := op.operator.GetParam()
	if len(params) != 2 {
		return nil, util.ErrXExprBadNumArgs.Gen("Binary operations require exactly two operands in expression.")
	}
	qb.put("(")
	gen, err := AddExpr(NewConcatExpr(params[0], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(str)
	gen, err = AddExpr(NewConcatExpr(params[1], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(")")
	return qb, nil
}

func (op *operator) asteriskOperator(qb *queryBuilder, str string) (*queryBuilder, error) {
	params := op.operator.GetParam()
	switch len(params) {
	case 0:
		qb.put("*")
	case 2:
		qb.put("(")
		gen, err := addUnquoteExpr(NewConcatExpr(params[0], op.GeneratorInfo))
		if err != nil {
			return nil, errors.Trace(err)
		}
		qb.put(*gen)
		qb.put("*")
		gen, err = addUnquoteExpr(NewConcatExpr(params[0], op.GeneratorInfo))
		if err != nil {
			return nil, errors.Trace(err)
		}
		qb.put(*gen)
		qb.put(")")
	default:
		return nil, util.ErrXExprBadNumArgs.Gen("Asterisk operator require zero or two operands in expression")
	}
	return qb, nil
}

func (op *operator) betweenExpression(qb *queryBuilder, str string) (*queryBuilder, error) {
	params := op.operator.GetParam()
	if len(params) != 3 {
		return nil, util.ErrXExprBadNumArgs.Gen("BETWEEN expression requires exactly three parameters.")
	}
	qb.put("(")
	gen, err := addUnquoteExpr(NewConcatExpr(params[0], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(str)
	gen, err = addUnquoteExpr(NewConcatExpr(params[1], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(" AND ")
	gen, err = addUnquoteExpr(NewConcatExpr(params[2], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(")")
	return qb, nil
}

func (op *operator) castExpression(qb *queryBuilder, str string) (*queryBuilder, error) {
	params := op.operator.GetParam()
	if len(params) != 2 {
		return nil, util.ErrXExprBadNumArgs.Gen("CAST expression requires exactly two parameters.")
	}
	qb.put("CAST(")
	gen, err := addUnquoteExpr(NewConcatExpr(params[0], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(" AS ")
	// TODO: Need to validate the second parameter.
	gen, err = AddExpr(NewConcatExpr(params[1], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(")")
	return qb, nil
}

func (op *operator) dateExpression(qb *queryBuilder, str string) (*queryBuilder, error) {
	params := op.operator.GetParam()
	if len(params) != 3 {
		return nil, util.ErrXExprBadNumArgs.Gen("DATE expression requires exactly three parameters.")
	}
	qb.put(str)
	qb.put("(")
	gen, err := addUnquoteExpr(NewConcatExpr(params[0], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(", INTERVAL ")
	gen, err = addUnquoteExpr(NewConcatExpr(params[1], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(" ")
	// TODO: Need to validate the third parameter.
	gen, err = AddExpr(NewConcatExpr(params[2], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(")")
	return qb, nil
}

func (op *operator) nullaryExpression(qb *queryBuilder, str string) (*queryBuilder, error) {
	params := op.operator.GetParam()
	if len(params) != 0 {
		return nil, util.ErrXExprBadNumArgs.Gen("Nullary operator require no operands in expression")
	}
	qb.put(str)
	return qb, nil
}

func (op *operator) inExpression(qb *queryBuilder, str string) (*queryBuilder, error) {
	params := op.operator.GetParam()
	switch len(params) {
	case 0, 1:
		return nil, util.ErrXExprBadNumArgs.Gen("IN expression requires at least two parameters.")
	case 2:
		if isArray(params[1]) {
			qb.put(str)
			qb.put("JSON_CONTAINS(")
			gen, err := AddExpr(NewConcatExpr(params[1], op.GeneratorInfo))
			if err != nil {
				return nil, errors.Trace(err)
			}
			qb.put(*gen)
			qb.put(",")
			if isOctets(params[0]) {
				qb.put("JSON_QUOTE(")
				gen, err = AddExpr(NewConcatExpr(params[0], op.GeneratorInfo))
				if err != nil {
					return nil, errors.Trace(err)
				}
				qb.put(*gen)
				qb.put("))")
			} else {
				qb.put("CAST(")
				gen, err = AddExpr(NewConcatExpr(params[0], op.GeneratorInfo))
				if err != nil {
					return nil, errors.Trace(err)
				}
				qb.put(*gen)
				qb.put(" AS JSON))")
			}
			break
		}
		fallthrough
	default:
		qb.put("(")
		gen, err := addUnquoteExpr(NewConcatExpr(params[0], op.GeneratorInfo))
		if err != nil {
			return nil, errors.Trace(err)
		}
		qb.put(*gen)
		qb.put(" ")
		qb.put(str)
		qb.put("IN (")
		cs := make([]interface{}, len(params))
		for i, d := range params {
			cs[i] = NewConcatExpr(d, op.GeneratorInfo)
		}
		gen, err = AddForEach(cs[1:], addUnquoteExpr, ",")
		qb.put(*gen)
		qb.put("))")
	}
	return qb, nil
}

func isArray(arg *Mysqlx_Expr.Expr) bool {
	return arg.GetType() == Mysqlx_Expr.Expr_ARRAY
}

func isOctets(arg *Mysqlx_Expr.Expr) bool {
	// TODO: need to check has_octets, go version protobuf file has fewer functions than c++ version.
	return arg.GetType() == Mysqlx_Expr.Expr_LITERAL && arg.GetLiteral().GetType() == Mysqlx_Datatypes.Scalar_V_OCTETS
}

func (op *operator) likeExpression(qb *queryBuilder, str string) (*queryBuilder, error) {
	params := op.operator.GetParam()
	if len(params) != 2 && len(params) != 3 {
		return nil, util.ErrXExprBadNumArgs.Gen("LIKE expression requires exactly two or three parameters.")
	}
	qb.put("(")
	gen, err := addUnquoteExpr(NewConcatExpr(params[0], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(str)
	gen, err = addUnquoteExpr(NewConcatExpr(params[1], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	if len(params) == 3 {
		qb.put(" ESCAPE ")
		gen, err = addUnquoteExpr(NewConcatExpr(params[2], op.GeneratorInfo))
		if err != nil {
			return nil, errors.Trace(err)
		}
		qb.put(*gen)
	}
	qb.put(")")
	return qb, nil
}

func (op *operator) binaryExpression(qb *queryBuilder, str string) (*queryBuilder, error) {
	params := op.operator.GetParam()
	if len(params) != 2 {
		return nil, util.ErrXExprBadNumArgs.Gen("BETWEEN expression requires exactly three parameters.")
	}
	qb.put("(")
	gen, err := addUnquoteExpr(NewConcatExpr(params[0], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(str)
	gen, err = AddExpr(NewConcatExpr(params[1], op.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	qb.put(*gen)
	qb.put(")")
	return qb, nil
}
