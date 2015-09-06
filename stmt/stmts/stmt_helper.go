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

package stmts

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression/expressions"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
)

func getDefaultValue(ctx context.Context, c *column.Col) (interface{}, bool, error) {
	// Check no default value flag.
	if mysql.HasNoDefaultValueFlag(c.Flag) {
		return nil, false, errors.Errorf("Field '%s' doesn't have a default value", c.Name)
	}

	// Check and get timestamp/datetime default value.
	if c.Tp == mysql.TypeTimestamp || c.Tp == mysql.TypeDatetime {
		if c.DefaultValue == nil {
			return nil, true, nil
		}

		value, err := expressions.GetTimeValue(ctx, c.DefaultValue, c.Tp, c.Decimal)
		if err != nil {
			return nil, true, errors.Errorf("Field '%s' get default value fail - %s", c.Name, errors.Trace(err))
		}

		return value, true, nil
	}

	return c.DefaultValue, true, nil
}

func getTable(ctx context.Context, tableIdent table.Ident) (table.Table, error) {
	full := tableIdent.Full(ctx)
	return sessionctx.GetDomain(ctx).InfoSchema().TableByName(full.Schema, full.Name)
}
