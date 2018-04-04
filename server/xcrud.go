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
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/xprotocol/expr"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Crud"
	"github.com/pingcap/tipb/go-mysqlx/Expr"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
)

type builder interface {
	build([]byte) (*string, error)
}

type baseBuilder struct {
	*expr.GeneratorInfo
}

func (b *baseBuilder) addAlias(p *Mysqlx_Crud.Projection) *string {
	target := ""
	if len(p.GetAlias()) != 0 {
		target += " AS " + util.QuoteIdentifier(p.GetAlias())
	}
	return &target
}

func (b *baseBuilder) addCollection(c *Mysqlx_Crud.Collection) *string {
	target := util.QuoteIdentifier(*c.Schema)
	target += "."
	target += util.QuoteIdentifier(*c.Name)
	return &target
}

func (b *baseBuilder) addFilter(f *Mysqlx_Expr.Expr) (*string, error) {
	if f == nil {
		return nil, nil
	}
	target := " WHERE "
	gen, err := expr.AddExpr(expr.NewConcatExpr(f, b.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen
	return &target, nil
}

func (b *baseBuilder) addOrder(ol []*Mysqlx_Crud.Order) (*string, error) {
	if len(ol) == 0 {
		return nil, nil
	}
	target := " ORDER BY "
	cs := make([]interface{}, len(ol))
	for i, d := range ol {
		cs[i] = d
	}
	gen, err := expr.AddForEach(cs, b.addOrderItem, ",")
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen
	return &target, nil
}

func (b *baseBuilder) addLimit(l *Mysqlx_Crud.Limit, noOffset bool) (*string, error) {
	if l == nil {
		return nil, nil
	}
	target := " LIMIT "
	if noOffset && l.GetOffset() != 0 {
		return nil, util.ErrXInvalidCollection.Gen("Invalid parameter: non-zero offset value not allowed for this operation")
	}
	if !noOffset {
		gen, err := expr.AddExpr(expr.NewConcatExpr(l.GetOffset(), b.GeneratorInfo))
		if err != nil {
			return nil, errors.Trace(err)
		}
		target += *gen + ", "
	}
	gen, err := expr.AddExpr(expr.NewConcatExpr(l.GetRowCount(), b.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen
	return &target, nil
}

func (b *baseBuilder) addOrderItem(i interface{}) (*string, error) {
	o := i.(*Mysqlx_Crud.Order)
	target := ""
	gen, err := expr.AddExpr(expr.NewConcatExpr(o.GetExpr(), b.GeneratorInfo))
	if err != nil {
		return nil, errors.Trace(err)
	}
	target += *gen
	if o.GetDirection() == Mysqlx_Crud.Order_DESC {
		target += " DESC"
	}
	return &target, nil
}

func (crud *xCrud) createCrudBuilder(msgType Mysqlx.ClientMessages_Type) (builder, error) {
	switch msgType {
	case Mysqlx.ClientMessages_CRUD_FIND:
		return &findBuilder{}, nil
	case Mysqlx.ClientMessages_CRUD_INSERT:
		return &insertBuilder{}, nil
	case Mysqlx.ClientMessages_CRUD_UPDATE:
		return &updateBuilder{}, nil
	case Mysqlx.ClientMessages_CRUD_DELETE:
		return &deleteBuilder{}, nil
	case Mysqlx.ClientMessages_CRUD_CREATE_VIEW:
	case Mysqlx.ClientMessages_CRUD_MODIFY_VIEW:
	case Mysqlx.ClientMessages_CRUD_DROP_VIEW:
	default:
		return nil, util.ErrXBadMessage
	}
	// @TODO should be moved to default
	log.Warnf("unknown crud builder type %d", msgType.String())
	return nil, util.ErrXBadMessage
}

type xCrud struct {
	ctx   QueryCtx
	pkt   *xpacketio.XPacketIO
	alloc arena.Allocator
}

func (crud *xCrud) dealCrudStmtExecute(goCtx goctx.Context, msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	var sqlQuery *string
	builder, err := crud.createCrudBuilder(msgType)
	if err != nil {
		log.Warnf("error occurs when create builder %s", msgType.String())
		return err
	}

	sqlQuery, err = builder.build(payload)
	if err != nil {
		log.Warnf("error occurs when build msg %s", msgType.String())
		return err
	}

	log.Infof("mysqlx reported 'CRUD query: %s'", *sqlQuery)
	rs, err := crud.ctx.Execute(goCtx, *sqlQuery)
	if err != nil {
		return err
	}
	for _, r := range rs {
		if err := WriteResultSet(goCtx, r, crud.pkt, crud.alloc); err != nil {
			return err
		}
	}
	return SendExecOk(crud.pkt, crud.ctx.AffectedRows(), crud.ctx.LastInsertID())
}

func createCrud(xcc *xClientConn) *xCrud {
	return &xCrud{
		ctx:   xcc.ctx,
		pkt:   xcc.pkt,
		alloc: xcc.alloc,
	}
}
