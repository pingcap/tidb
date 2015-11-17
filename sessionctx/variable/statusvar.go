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

	"github.com/juju/errors"
)

var statusVars map[string]*StatusVal
var globalStatusScopes = make(map[string]ScopeFlag)

// DefaultScopeFlag is the status default scope.
var DefaultScopeFlag = ScopeGlobal | ScopeSession

// Statists is the set of all statists.
var Statists []Statist

// StatusVal is the value of the corresponding status variable.
type StatusVal struct {
	Scope ScopeFlag
	Value interface{}
}

// Statist is the interface of statist.
type Statist interface {
	// GetScope gets the status variables scope.
	GetScope(status string) ScopeFlag
	// Stats returns the statist statistics.
	Stats() (map[string]interface{}, error)
}

// RegisterStatist registers statist.
func RegisterStatist(s Statist) {
	Statists = append(Statists, s)
}

// GetStatusVars gets registered statists status variables.
func GetStatusVars() (map[string]*StatusVal, error) {
	statusVars = make(map[string]*StatusVal)
	ret := make(map[string]*StatusVal)

	for _, statist := range Statists {
		vals, err := statist.Stats()
		if err != nil {
			return nil, errors.Trace(err)
		}

		for name, val := range vals {
			scope := statist.GetScope(name)
			statusVars[name] = &StatusVal{Value: val, Scope: scope}
			ret[name] = &StatusVal{Value: val, Scope: scope}
		}
	}

	defaultStatusVars, err := GetDefaultStatusVars()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for status := range defaultStatusVars {
		// To get more accurate value from the global status variables table.
		ret[status] = &StatusVal{}
	}

	return ret, nil
}

// GetStatusVar returns status var infomation for name.
func GetStatusVar(name string) *StatusVal {
	name = strings.ToLower(name)
	return statusVars[name]
}

// GetDefaultStatusVars gets status variables from the global status variables table.
// TODO: Fill status variables.
func GetDefaultStatusVars() (map[string]*StatusVal, error) {
	return nil, nil
}
