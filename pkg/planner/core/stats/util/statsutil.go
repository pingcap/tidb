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

package util

import (
	"strconv"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// GetTblInfoForUsedStatsByPhysicalID get table name, partition name and HintedTable
// that will be used to record used stats.
func GetTblInfoForUsedStatsByPhysicalID(sctx base.PlanContext, id int64) (
	fullName string, tblInfo *model.TableInfo) {
	fullName = "tableID " + strconv.FormatInt(id, 10)

	is := sctx.GetLatestISWithoutSessExt()
	tbl, partDef := infoschema.FindTableByTblOrPartID(is.(infoschema.InfoSchema), id)
	if tbl == nil || tbl.Meta() == nil {
		return
	}
	tblInfo = tbl.Meta()
	fullName = tblInfo.Name.O
	if partDef != nil {
		fullName += " " + partDef.Name.O
	} else if pi := tblInfo.GetPartitionInfo(); pi != nil && len(pi.Definitions) > 0 {
		fullName += " global"
	}
	return
}
