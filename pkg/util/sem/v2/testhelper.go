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

package sem

import (
	"strings"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

// EnableFromPathForTest enables SEM v2 in test using a configuration file.
func EnableFromPathForTest(configPath string) (func(), error) {
	semConfig, err := parseSEMConfigFromFile(configPath)
	if err != nil {
		return nil, err
	}

	variableDefValue := make(map[string]string)
	for _, v := range semConfig.RestrictedVariables {
		if v.Value != "" {
			sysVar := variable.GetSysVar(v.Name)
			if sysVar == nil {
				continue
			}

			variableDefValue[v.Name] = sysVar.Value
		}
	}

	err = Enable(configPath)
	if err != nil {
		return nil, err
	}

	return func() {
		Disable()

		for name, value := range variableDefValue {
			variable.SetSysVar(name, value)
		}
	}, nil
}

// AddRestrictedPrivilegesForTest adds restricted privileges for test.
// It's not safe to use this function while using SEM in multiple goroutines.
func AddRestrictedPrivilegesForTest(privilege string) {
	if globalSem.Load() == nil {
		return
	}

	globalSem.Load().restrictedPrivileges[strings.ToUpper(privilege)] = struct{}{}
}

// RemoveRestrictedPrivilegesForTest removes restricted privileges for test.
// It's not safe to use this function while using SEM in multiple goroutines.
func RemoveRestrictedPrivilegesForTest(privilege string) {
	if globalSem.Load() == nil {
		return
	}

	delete(globalSem.Load().restrictedPrivileges, strings.ToUpper(privilege))
}
