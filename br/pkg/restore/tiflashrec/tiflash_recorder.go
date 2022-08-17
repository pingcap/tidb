// Copyright 2022-present PingCAP, Inc.
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

package tiflashrec

import (
	"bytes"
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
)

type TiFlashRecorder struct {
	// Table ID -> TiFlash Count
	items map[int64]model.TiFlashReplicaInfo
}

func New() *TiFlashRecorder {
	return &TiFlashRecorder{
		items: map[int64]model.TiFlashReplicaInfo{},
	}
}

func (r *TiFlashRecorder) AddTable(tableID int64, replica model.TiFlashReplicaInfo) {
	log.Info("recording tiflash replica", zap.Int64("table", tableID), zap.Any("replica", replica))
	r.items[tableID] = replica
}

func (r *TiFlashRecorder) DelTable(tableID int64) {
	delete(r.items, tableID)
}

func (r *TiFlashRecorder) Iterate(f func(tableID int64, replica model.TiFlashReplicaInfo)) {
	for k, v := range r.items {
		f(k, v)
	}
}

func (r *TiFlashRecorder) Rewrite(oldID int64, newID int64) {
	old, ok := r.items[oldID]
	log.Info("rewriting tiflash replica", zap.Int64("old", oldID), zap.Int64("new", newID), zap.Bool("success", ok))
	if ok {
		r.items[newID] = old
		delete(r.items, oldID)
	}
}

func (r *TiFlashRecorder) GenerateAlterTableDDLs(info infoschema.InfoSchema) []string {
	items := make([]string, 0, len(r.items))
	r.Iterate(func(id int64, replica model.TiFlashReplicaInfo) {
		table, ok := info.TableByID(id)
		if !ok {
			log.Warn("Table do not exist, skipping", zap.Int64("id", id))
			return
		}
		schema, ok := info.SchemaByTable(table.Meta())
		if !ok {
			log.Warn("Schema do not exist, skipping", zap.Int64("id", id), zap.Stringer("table", table.Meta().Name))
			return
		}

		items = append(items, fmt.Sprintf(
			"ALTER TABLE %s %s",
			utils.EncloseDBAndTable(schema.Name.O, table.Meta().Name.O),
			alterTableSpecOf(replica)),
		)
	})
	return items
}

func alterTableSpecOf(replica model.TiFlashReplicaInfo) string {
	spec := &ast.AlterTableSpec{
		Tp: ast.AlterTableSetTiFlashReplica,
		TiFlashReplica: &ast.TiFlashReplicaSpec{
			Count:  replica.Count,
			Labels: replica.LocationLabels,
		},
	}

	buf := bytes.NewBuffer(make([]byte, 0, 32))
	restoreCx := format.NewRestoreCtx(
		format.RestoreKeyWordUppercase|
			format.RestoreNameBackQuotes|
			format.RestoreStringSingleQuotes|
			format.RestoreStringEscapeBackslash,
		buf)
	spec.Restore(restoreCx)
	return buf.String()
}
