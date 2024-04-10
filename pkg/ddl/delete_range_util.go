// Copyright 2022 PingCAP, Inc.
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

package ddl

type tblIdxID struct {
	physicalID int64
	indexID    int64
}

type elementIDAlloc struct {
	physicalIDs map[int64]int64
	indexIDs    map[tblIdxID]int64
}

func (e *elementIDAlloc) allocForIndexID(physicalID, indexID int64) int64 {
	if e.indexIDs == nil {
		e.indexIDs = make(map[tblIdxID]int64)
	}
	k := tblIdxID{physicalID: physicalID, indexID: indexID}
	if id, found := e.indexIDs[k]; found {
		return id
	}
	next := int64(len(e.physicalIDs) + len(e.indexIDs) + 1)
	e.indexIDs[k] = next
	return next
}

func (e *elementIDAlloc) allocForPhysicalID(tableID int64) int64 {
	if e.physicalIDs == nil {
		e.physicalIDs = make(map[int64]int64)
	}
	if id, found := e.physicalIDs[tableID]; found {
		return id
	}
	next := int64(len(e.physicalIDs) + len(e.indexIDs) + 1)
	e.physicalIDs[tableID] = next
	return next
}
