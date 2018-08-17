// Copyright 2018 PingCAP, Inc.
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

package domain

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/chunk"
)

// GlobalVariableCache caches global variables.
type GlobalVariableCache struct {
	sync.RWMutex
	lastModify time.Time
	rows       []chunk.Row
	fields     []*ast.ResultField
}

const globalVariableCacheExpiry time.Duration = 2 * time.Second

// Update updates the global variable cache.
func (gvc *GlobalVariableCache) Update(rows []chunk.Row, fields []*ast.ResultField) {
	gvc.Lock()
	gvc.lastModify = time.Now()
	gvc.rows = rows
	gvc.fields = fields
	gvc.Unlock()
}

// Get gets the global variables from cache.
func (gvc *GlobalVariableCache) Get() (succ bool, rows []chunk.Row, fields []*ast.ResultField) {
	gvc.RLock()
	defer gvc.RUnlock()
	if time.Now().Sub(gvc.lastModify) < globalVariableCacheExpiry {
		succ, rows, fields = true, gvc.rows, gvc.fields
		return
	}
	succ = false
	return
}

// GetGlobalVarsCache gets the global variable cache.
func (do *Domain) GetGlobalVarsCache() *GlobalVariableCache {
	return &do.gvc
}
