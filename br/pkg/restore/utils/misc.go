// Copyright 2024 PingCAP, Inc.
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

package utils

import (
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/codec"
)

const (
	WriteCFName   = "write"
	DefaultCFName = "default"
)

// GetPartitionIDMap creates a map maping old physical ID to new physical ID.
func GetPartitionIDMap(newTable, oldTable *model.TableInfo) map[int64]int64 {
	tableIDMap := make(map[int64]int64)

	if oldTable.Partition != nil && newTable.Partition != nil {
		nameMapID := make(map[string]int64)

		for _, old := range oldTable.Partition.Definitions {
			nameMapID[old.Name.L] = old.ID
		}
		for _, new := range newTable.Partition.Definitions {
			if oldID, exist := nameMapID[new.Name.L]; exist {
				tableIDMap[oldID] = new.ID
			}
		}
	}

	return tableIDMap
}

// GetTableIDMap creates a map maping old tableID to new tableID.
func GetTableIDMap(newTable, oldTable *model.TableInfo) map[int64]int64 {
	tableIDMap := GetPartitionIDMap(newTable, oldTable)
	tableIDMap[oldTable.ID] = newTable.ID
	return tableIDMap
}

// GetIndexIDMap creates a map maping old indexID to new indexID.
func GetIndexIDMap(newTable, oldTable *model.TableInfo) map[int64]int64 {
	indexIDMap := make(map[int64]int64)
	for _, srcIndex := range oldTable.Indices {
		for _, destIndex := range newTable.Indices {
			if srcIndex.Name == destIndex.Name {
				indexIDMap[srcIndex.ID] = destIndex.ID
			}
		}
	}

	return indexIDMap
}

func TruncateTS(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	if len(key) < 8 {
		return key
	}
	return key[:len(key)-8]
}

func EncodeKeyPrefix(key []byte) []byte {
	encodedPrefix := make([]byte, 0)
	ungroupedLen := len(key) % 8
	encodedPrefix = append(encodedPrefix, codec.EncodeBytes([]byte{}, key[:len(key)-ungroupedLen])...)
	return append(encodedPrefix[:len(encodedPrefix)-9], key[len(key)-ungroupedLen:]...)
}
