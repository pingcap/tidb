// Copyright 2015 PingCAP, Inc.
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
	"strings"
	"sync"

	"github.com/juju/errors"
)

// statusVars is global status vars map.
var statusVars map[string]*StatusVal
var globalStatusScopes = make(map[string]ScopeFlag)
var defaultScopeFlag = ScopeGlobal | ScopeSession
var statistMu sync.RWMutex

// Statists is the set of all statists.
var Statists = make(map[string]Statist)

// StatusVal is the value of the corresponding status variable.
type StatusVal struct {
	Scope ScopeFlag
	Value interface{}
}

// Statist is the interface of statist.
type Statist interface {
	// GetDefaultStatusScopes gets default status variables scope.
	GetDefaultStatusScopes() map[string]ScopeFlag
	// Stat returns the statist statistics.
	Stat() (map[string]*StatusVal, error)
}

// RegisterStatist registers statist.
func RegisterStatist(id string, s Statist) {
	statistMu.Lock()
	Statists[id] = s
	statistMu.Unlock()

	scopes := s.GetDefaultStatusScopes()
	for status, scope := range scopes {
		globalStatusScopes[status] = scope
	}
}

// DeleteStatist deletes statist.
func DeleteStatist(id string) {
	statistMu.Lock()
	delete(Statists, id)
	statistMu.Unlock()
}

// FillStatusVal fills the status variable value.
func FillStatusVal(status string, value interface{}) *StatusVal {
	scope, ok := globalStatusScopes[status]
	if !ok {
		scope = defaultScopeFlag
	}

	return &StatusVal{Scope: scope, Value: value}
}

// GetStatusVars gets registered statists status variables.
func GetStatusVars() (map[string]*StatusVal, error) {
	statusVars = make(map[string]*StatusVal)

	statistMu.RLock()
	for _, statist := range Statists {
		stat, err := statist.Stat()
		if err != nil {
			return nil, errors.Trace(err)
		}

		for name, s := range stat {
			statusVars[name] = s
		}
	}
	statistMu.RUnlock()

	return statusVars, nil
}

// GetStatusVar returns status var infomation for name.
func GetStatusVar(name string) *StatusVal {
	name = strings.ToLower(name)
	return statusVars[name]
}
