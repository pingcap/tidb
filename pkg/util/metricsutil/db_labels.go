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

package metricsutil

import (
	"strings"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

// GetDBNames returns the database labels for SQL metrics, honoring RecordDBLabel.
func GetDBNames(vars *variable.SessionVars) []string {
	if vars == nil || !config.GetGlobalConfig().Status.RecordDBLabel {
		return []string{""}
	}
	dbNames := make(map[string]struct{})
	if vars.StmtCtx != nil {
		for _, table := range vars.StmtCtx.Tables {
			dbNames[table.DB] = struct{}{}
		}
	}
	if len(dbNames) == 0 {
		dbNames[strings.ToLower(vars.CurrentDB)] = struct{}{}
	}
	names := make([]string, 0, len(dbNames))
	for name := range dbNames {
		names = append(names, name)
	}
	return names
}
