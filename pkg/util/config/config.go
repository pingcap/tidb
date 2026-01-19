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
	"innodb_lock_wait_timeout": {}, // It is unncessary to load this variable for plan replayer.
}

// LoadConfig loads system variables from a toml reader. it is only for plan replayer and test.
func LoadConfig(ctx sessionctx.Context, v io.ReadCloser) (unLoadVars []string, err error) {
	varMap := make(map[string]string)

	_, err = toml.NewDecoder(v).Decode(&varMap)
	if err != nil {
		return nil, errors.AddStack(err)
	}
	unLoadVars = make([]string, 0)
	vars := ctx.GetSessionVars()
	for name, value := range varMap {
		if _, ok := ignoredSystemVariablesForPlanReplayerLoad[name]; ok {
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
