// Copyright 2023 PingCAP, Inc.
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

package importer

import (
	"maps"
	"sync"
)

// Progress is the progress of the IMPORT INTO task.
type Progress struct {
	colSizeMu  sync.Mutex
	colSizeMap map[int64]int64
}

// NewProgress creates a new Progress.
func NewProgress() *Progress {
	return &Progress{
		colSizeMap: make(map[int64]int64),
	}
}

// AddColSize adds the size of the column to the progress.
func (p *Progress) AddColSize(colSizeMap map[int64]int64) {
	p.colSizeMu.Lock()
	defer p.colSizeMu.Unlock()
	for key, value := range colSizeMap {
		p.colSizeMap[key] += value
	}
}

// GetColSize returns the size of the column.
func (p *Progress) GetColSize() map[int64]int64 {
	p.colSizeMu.Lock()
	defer p.colSizeMu.Unlock()
	return maps.Clone(p.colSizeMap)
}
