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
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

// TiFlashRecorder records the information of TiFlash replicas
// during restoring.
// Because the limit of the current implementation, we add serval hooks
// to observe the information we need:
//   - Before full restore create tables:
//     We record the tiflash replica information and remove the replica info.
//     Because during PiTR restore, the transaction model would be broken, which breaks TiFlash too.
//     We must make sure they won't be replicated to TiFlash during the whole PiTR procedure.
//   - After full restore created tables, generating rewrite rules:
//     We perform the rewrite rule over our records.
//     We trace table via table ID instead of table name so we can handle `RENAME` DDLs.
//   - When doing PiTR restore, after rewriting table info in meta key:
//     We update the replica information
type TiFlashRecorder struct {
	// Table ID -> TiFlash Count
	items map[int64]model.TiFlashReplicaInfo
}

func New() *TiFlashRecorder {
	return &TiFlashRecorder{
		items: map[int64]model.TiFlashReplicaInfo{},
	}
}

func (r *TiFlashRecorder) Load(items map[int64]model.TiFlashReplicaInfo) {
	r.items = items
}

func (r *TiFlashRecorder) GetItems() map[int64]model.TiFlashReplicaInfo {
	return r.items
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
	if newID == oldID {
		return
	}
	old, ok := r.items[oldID]
	log.Info("rewriting tiflash replica", zap.Int64("old", oldID), zap.Int64("new", newID), zap.Bool("success", ok))
	if ok {
		r.items[newID] = old
		delete(r.items, oldID)
	}
}

func (r *TiFlashRecorder) GenerateResetAlterTableDDLs(info infoschema.InfoSchema) []string {
	items := make([]string, 0, len(r.items))
	r.Iterate(func(id int64, replica model.TiFlashReplicaInfo) {
		table, ok := info.TableByID(id)
		if !ok {
			log.Warn("Table do not exist, skipping", zap.Int64("id", id))
			return
		}
		schema, ok := infoschema.SchemaByTable(info, table.Meta())
		if !ok {
			log.Warn("Schema do not exist, skipping", zap.Int64("id", id), zap.Stringer("table", table.Meta().Name))
			return
		}
		// Currently, we didn't backup tiflash cluster volume during volume snapshot backup,
		// But the table has replica info after volume restoration.
		// We should reset it to 0, then set it back. otherwise, it will return error when alter tiflash replica.
		altTableSpec, err := alterTableSpecOf(replica, true)
		if err != nil {
			log.Warn("Failed to generate the alter table spec", logutil.ShortError(err), zap.Any("replica", replica))
			return
		}
		items = append(items, fmt.Sprintf(
			"ALTER TABLE %s %s",
			utils.EncloseDBAndTable(schema.Name.O, table.Meta().Name.O),
			altTableSpec),
		)
		altTableSpec, err = alterTableSpecOf(replica, false)
		if err != nil {
			log.Warn("Failed to generate the alter table spec", logutil.ShortError(err), zap.Any("replica", replica))
			return
		}
		items = append(items, fmt.Sprintf(
			"ALTER TABLE %s %s",
			utils.EncloseDBAndTable(schema.Name.O, table.Meta().Name.O),
			altTableSpec),
		)
	})
	return items
}

func (r *TiFlashRecorder) GenerateAlterTableDDLs(info infoschema.InfoSchema) []string {
	items := make([]string, 0, len(r.items))
	r.Iterate(func(id int64, replica model.TiFlashReplicaInfo) {
		table, ok := info.TableByID(id)
		if !ok {
			log.Warn("Table do not exist, skipping", zap.Int64("id", id))
			return
		}
		schema, ok := infoschema.SchemaByTable(info, table.Meta())
		if !ok {
			log.Warn("Schema do not exist, skipping", zap.Int64("id", id), zap.Stringer("table", table.Meta().Name))
			return
		}
		altTableSpec, err := alterTableSpecOf(replica, false)
		if err != nil {
			log.Warn("Failed to generate the alter table spec", logutil.ShortError(err), zap.Any("replica", replica))
			return
		}
		items = append(items, fmt.Sprintf(
			"ALTER TABLE %s %s",
			utils.EncloseDBAndTable(schema.Name.O, table.Meta().Name.O),
			altTableSpec),
		)
	})
	return items
}

func alterTableSpecOf(replica model.TiFlashReplicaInfo, reset bool) (string, error) {
	spec := &ast.AlterTableSpec{
		Tp: ast.AlterTableSetTiFlashReplica,
		TiFlashReplica: &ast.TiFlashReplicaSpec{
			Count:  replica.Count,
			Labels: replica.LocationLabels,
		},
	}
	if reset {
		spec = &ast.AlterTableSpec{
			Tp: ast.AlterTableSetTiFlashReplica,
			TiFlashReplica: &ast.TiFlashReplicaSpec{
				Count: 0,
			},
		}
	}

	buf := bytes.NewBuffer(make([]byte, 0, 32))
	restoreCx := format.NewRestoreCtx(
		format.RestoreKeyWordUppercase|
			format.RestoreNameBackQuotes|
			format.RestoreStringSingleQuotes|
			format.RestoreStringEscapeBackslash,
		buf)
	if err := spec.Restore(restoreCx); err != nil {
		return "", err
	}
	return buf.String(), nil
}
