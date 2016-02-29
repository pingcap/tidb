// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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

package rsets

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/table"
)

var (
	_ plan.Planner = (*TableRset)(nil)
)

// TableRset is record set to select table, like `select c from t as at`.
type TableRset struct {
	Schema string
	Name   string
}

// Plan gets InfoSchemaPlan/TableDefaultPlan.
func (r *TableRset) Plan(ctx context.Context) (plan.Plan, error) {
	is := sessionctx.GetDomain(ctx).InfoSchema()
	t, err := is.TableByName(model.NewCIStr(r.Schema), model.NewCIStr(r.Name))
	if err != nil {
		return nil, errors.Trace(err)
	}
	tdp := &plans.TableDefaultPlan{T: t}
	tdp.Fields = make([]*field.ResultField, 0, len(t.Cols()))
	for _, col := range t.Cols() {
		f := field.ColToResultField(col, t.Meta().Name.O)
		f.DBName = r.Schema
		tdp.Fields = append(tdp.Fields, f)
	}
	return tdp, nil
}

func newTableRset(currentSchema, name string) (*TableRset, error) {
	tr := &TableRset{}
	seps := strings.Split(name, ".")
	switch len(seps) {
	case 1:
		tr.Schema = currentSchema
		tr.Name = name
	case 2:
		tr.Schema = seps[0]
		tr.Name = seps[1]
	default:
		return nil, errors.Errorf("invalid qualified name %s", name)
	}

	return tr, nil
}

// TableSource is table source or sub select.
type TableSource struct {
	// Source is table.Ident or stmt.Statement.
	Source interface{}
	// Table source name.
	Name string
}

func (t *TableSource) String() string {
	switch x := t.Source.(type) {
	case table.Ident:
		if len(t.Name) == 0 {
			return x.String()
		}
		return fmt.Sprintf("%s AS %s", x, t.Name)
	case stmt.Statement:
		if len(t.Name) == 0 {
			return fmt.Sprintf("(%s)", x)
		}
		return fmt.Sprintf("(%s) AS %s", x, t.Name)
	}

	panic(fmt.Sprintf("invalid table source %T", t.Source))
}
