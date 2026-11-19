// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// activateSchemaReadOnlyWrite makes specialized writes visible to database
// draining before checking the latest flag. Either the transition observes the
// transaction, or this check observes its published flag.
func activateSchemaReadOnlyWrite(sctx sessionctx.Context, tbl *model.TableInfo) error {
	is := sctx.GetInfoSchema().(infoschema.InfoSchema)
	db, ok := infoschema.SchemaByTable(is, tbl)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStack("database of table %s does not exist", tbl.Name.O)
	}
	sctx.GetSessionVars().GetRelatedTableForReadOnly().Store(tbl.ID, db.Name)
	if _, err := sctx.Txn(true); err != nil {
		return err
	}
	return checkLatestSchemaReadOnly(sctx, db.Name)
}

func checkLatestSchemaReadOnly(sctx sessionctx.Context, database ast.CIStr) error {
	latestDB, ok := sctx.GetLatestISWithoutSessExt().SchemaByName(database)
	if ok && latestDB.ReadOnly && !privilege.CanWriteReadOnlySchema(sctx, database.L) {
		return infoschema.ErrSchemaInReadOnlyMode.GenWithStackByArgs(latestDB.Name.O)
	}
	return nil
}
