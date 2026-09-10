// Copyright 2025 PingCAP, Inc.
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

package utils

import (
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// DatabaseRestorePlan keeps the immutable source metadata separate from the
// metadata used to create the downstream database.
type DatabaseRestorePlan struct {
	Source *metautil.Database
	Target *model.DBInfo
}

// TableRestorePlan keeps the immutable source table and its target identity.
// Source owns backup IDs, files, checksums, and statistics. TargetDB and
// TargetInfo are clones used by the downstream DDL path.
type TableRestorePlan struct {
	Source     *metautil.Table
	TargetDB   *model.DBInfo
	TargetInfo *model.TableInfo
}

// TargetTable returns the target metadata in the shape expected by the DDL
// layer. It intentionally doesn't copy source-only restore data such as files
// and statistics.
func (p *TableRestorePlan) TargetTable() *metautil.Table {
	return &metautil.Table{DB: p.TargetDB, Info: p.TargetInfo}
}

// CreatedTable is a table created on restore process,
// but not yet filled with data.
type CreatedTable struct {
	RewriteRule *RewriteRules
	Table       *model.TableInfo
	OldTable    *metautil.Table
	TargetDB    ast.CIStr
}

// TargetDBName returns the downstream database name. The fallback keeps
// compatibility with identity-route CreatedTable values built by older tests
// and helper code.
func (t *CreatedTable) TargetDBName() ast.CIStr {
	if t.TargetDB.O != "" {
		return t.TargetDB
	}
	if t.OldTable != nil && t.OldTable.DB != nil {
		return t.OldTable.DB.Name
	}
	return ast.CIStr{}
}
