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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
)

func getTable(ctx context.Context, tableIdent table.Ident) (table.Table, error) {
	full := tableIdent.Full(ctx)
	return sessionctx.GetDomain(ctx).InfoSchema().TableByName(full.Schema, full.Name)
}
