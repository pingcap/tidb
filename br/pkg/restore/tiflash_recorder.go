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

package restore

import (
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/infoschema"
	"go.uber.org/zap"
)

type TiFlashRecorder struct {
	// Table ID -> TiFlash Count
	items map[int64]int
}

func (r *TiFlashRecorder) AddTable(tableID int64, replica int) {
	r.items[tableID] = replica
}

func (r *TiFlashRecorder) DelTable(tableID int64) {
	delete(r.items, tableID)
}

func (r *TiFlashRecorder) Iterate(f func(tableID int64, replica int)) {
	for k, v := range r.items {
		f(k, v)
	}
}

func (r *TiFlashRecorder) GenerateAlterTableDDLs(info infoschema.InfoSchema) []string {
	items := make([]string, 0, len(r.items))
	r.Iterate(func(id int64, replica int) {
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
		items = append(items, fmt.Sprintf("ALTER TABLE %s SET TIFLASH REPLICA %d", utils.EncloseDBAndTable(schema.Name.O, table.Meta().Name.O)))
	})
	return items
}
