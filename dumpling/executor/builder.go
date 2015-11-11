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

package executor

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/optimizer/plan"
)

// executorBuilder builds an Executor from a Plan.
// the InfoSchema must be the same one used in InfoBinder.
type executorBuilder struct {
	ctx context.Context
	is  infoschema.InfoSchema
}

func newExecutorBuilder(ctx context.Context, is infoschema.InfoSchema) *executorBuilder {
	return &executorBuilder{
		ctx: ctx,
		is:  is,
	}
}

func (b *executorBuilder) build(p plan.Plan) Executor {
	switch v := p.(type) {
	case *plan.TableScan:
		return b.buildTableScan(v)
	case *plan.IndexScan:
		return b.buildIndexScan(v)
	}
	return nil
}

func (b *executorBuilder) buildTableScan(v *plan.TableScan) Executor {
	table, _ := b.is.TableByID(v.Table.ID)
	return &TableScanExec{
		t: table,
	}
}

func (b *executorBuilder) buildIndexScan(v *plan.IndexScan) Executor {
	table, _ := b.is.TableByID(v.Table.ID)
	e := &IndexScanExec{
		Table: table,
	}
	return e
}
