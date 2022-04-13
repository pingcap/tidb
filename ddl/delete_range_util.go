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

const (
	elemTable     byte = 't'
	elemPartition byte = 'p'
	elemIndex     byte = 'i'
)

type elementObjID struct {
	tp byte
	id int64
}

type elementIDAlloc struct {
	objIDs map[elementObjID]int64
}

func (e *elementIDAlloc) allocForIndexID(indexID int64) int64 {
	return e.alloc(elemIndex, indexID)
}

func (e *elementIDAlloc) allocForTableID(tableID int64) int64 {
	return e.alloc(elemTable, tableID)
}

func (e *elementIDAlloc) allocForPartitionID(partitionID int64) int64 {
	return e.alloc(elemPartition, partitionID)
}

func (e *elementIDAlloc) alloc(tp byte, schemaObjID int64) int64 {
	if e.objIDs == nil {
		e.objIDs = make(map[elementObjID]int64)
	}
	objID := elementObjID{tp: tp, id: schemaObjID}
	if elemID, found := e.objIDs[objID]; found {
		return elemID
	}
	newElemID := int64(len(e.objIDs) + 1)
	e.objIDs[objID] = newElemID
	return newElemID
}
