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

package config

import (
	"fmt"
	"io"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var ignoredSystemVariablesForPlanReplayerLoad = map[string]struct{}{
	vardef.InnodbLockWaitTimeout: {}, // It is unnecessary to load this variable for plan replayer.
	// The following variables override the read timestamp of the loading session.
	// They do not affect the plan, but they make the load itself fail (DDL is
	// rejected under tidb_low_resolution_tso / tidb_snapshot) or make the loaded
	// schema invisible to the stale infoschema the session would read afterwards.
	vardef.TiDBLowResolutionTSO: {},
	vardef.TiDBSnapshot:         {},
	vardef.TiDBReadStaleness:    {},
}

// LoadConfigForPlanReplayerLoad loads system variables from a toml reader. it is only for plan replayer and test.
func LoadConfigForPlanReplayerLoad(ctx sessionctx.Context, v io.ReadCloser) (unLoadVars []string, err error) {
	varMap := make(map[string]string)

	_, err = toml.NewDecoder(v).Decode(&varMap)
	if err != nil {
		return nil, errors.AddStack(err)
	}
	unLoadVars = make([]string, 0)
	vars := ctx.GetSessionVars()
	for name, value := range varMap {
		if _, ok := ignoredSystemVariablesForPlanReplayerLoad[name]; ok {
			logutil.BgLogger().Warn(fmt.Sprintf("ignore set variable %s:%s", name, value), zap.Error(err))
			continue
		}
		sysVar := variable.GetSysVar(name)
		if sysVar == nil {
			unLoadVars = append(unLoadVars, name)
			logutil.BgLogger().Warn(fmt.Sprintf("skip set variable %s:%s", name, value), zap.Error(err))
			continue
		}
		sVal, err := sysVar.Validate(vars, value, vardef.ScopeSession)
		if err != nil {
			unLoadVars = append(unLoadVars, name)
			logutil.BgLogger().Warn(fmt.Sprintf("skip variable %s:%s", name, value), zap.Error(err))
			continue
		}
		err = vars.SetSystemVar(name, sVal)
		if err != nil {
			unLoadVars = append(unLoadVars, name)
			logutil.BgLogger().Warn(fmt.Sprintf("skip set variable %s:%s", name, value), zap.Error(err))
			continue
		}
	}
	return unLoadVars, nil
}
