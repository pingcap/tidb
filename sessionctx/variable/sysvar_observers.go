// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"github.com/pingcap/tidb/types"
)

// globalSysVarObservers is a map who observes modifying of system values.
var globalSysVarObservers = map[string]func(newValue string) error{}

// RegisterGlobalSysVarObserver registers a new observer for sys var.
// All observers must be registered before any session is created.
func RegisterGlobalSysVarObserver(varName string, observer func(newValue string) error) {
	globalSysVarObservers[varName] = observer
}

// NotifyGlobalSysVarModify triggers an observer if it's registered.
func NotifyGlobalSysVarModify(varName string, newValue types.Datum) error {
	observer, ok := globalSysVarObservers[varName]
	if !ok {
		return nil
	}

	sVal := ""
	var err error
	if !newValue.IsNull() {
		sVal, err = newValue.ToString()
		if err != nil {
			return err
		}
	}

	return observer(sVal)
}
