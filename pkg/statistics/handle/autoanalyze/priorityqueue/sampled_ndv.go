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

package priorityqueue

import (
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
)

// sampledNDVOption chooses once per job, so partition batches share a rate.
// Queue age and rows times columns do not measure the previous scan cost.
func sampledNDVOption(sctx sessionctx.Context, handle statstypes.StatsHandle, tableID int64, version int, schema, tableName string) (string, error) {
	if version != 2 {
		return "", nil
	}
	enabled, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.TiDBEnableSampledNDV)
	if err != nil || enabled != vardef.On {
		return "", err
	}
	tbl, ok := sctx.GetLatestInfoSchema().TableInfoByID(tableID)
	if !ok {
		return "", errors.Errorf("table %d no longer exists", tableID)
	}
	count := int64(0)
	if tbl.Partition == nil {
		count = handle.GetPhysicalTableStats(tableID, tbl).RealtimeCount
	} else {
		for _, def := range tbl.Partition.Definitions {
			count += handle.GetPhysicalTableStats(def.ID, tbl).RealtimeCount
		}
	}
	thresholds := [2]int64{}
	for i, name := range []string{vardef.TiDBAnalyzeSampledNDVTableSizeThreshold, vardef.TiDBAnalyzeSampledNDVDurationThreshold} {
		value, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(name)
		if err != nil {
			return "", err
		}
		if thresholds[i], err = strconv.ParseInt(value, 10, 64); err != nil {
			return "", err
		}
	}
	const sampledOption = " WITH 0.05 NDVRATE"
	if thresholds[0] > 0 && count > thresholds[0] {
		return sampledOption, nil
	}
	if thresholds[1] > 0 {
		// A sampled run is faster. Keep sampling instead of letting that shorter
		// run switch the next one back to full input.
		rows, _, err := util.ExecRows(sctx, `SELECT TIMESTAMPDIFF(SECOND, start_time, end_time), INSTR(job_info, 'ndvrate') > 0
			FROM mysql.analyze_jobs WHERE table_schema = %? AND table_name = %?
			AND state = 'finished' AND fail_reason IS NULL
			AND (job_info LIKE 'analyze table %' OR job_info LIKE 'auto analyze table %')
			ORDER BY id DESC LIMIT 1`, schema, tableName)
		if err != nil {
			return "", err
		}
		if len(rows) > 0 && !rows[0].IsNull(0) && (rows[0].GetInt64(0) > thresholds[1] || rows[0].GetInt64(1) == 1) {
			return sampledOption, nil
		}
	}
	return "", nil
}
