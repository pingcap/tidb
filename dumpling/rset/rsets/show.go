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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/sessionctx/db"
)

var (
	_ plan.Planner = (*ShowRset)(nil)
)

// ShowRset is record set to show.
type ShowRset struct {
	Target     int
	DBName     string
	TableName  string
	ColumnName string
	Flag       int
	Full       bool
}

// Plan gets ShowPlan.
func (r *ShowRset) Plan(ctx context.Context) (plan.Plan, error) {
	return &plans.ShowPlan{
		Target:     r.Target,
		DBName:     r.getDBName(ctx),
		TableName:  r.TableName,
		ColumnName: r.ColumnName,
		Flag:       r.Flag,
		Full:       r.Full,
	}, nil
}

func (r *ShowRset) getDBName(ctx context.Context) string {
	if len(r.DBName) > 0 {
		return r.DBName
	}

	// if r.DBName is empty, we should use current db name if possible.
	return db.GetCurrentSchema(ctx)
}
