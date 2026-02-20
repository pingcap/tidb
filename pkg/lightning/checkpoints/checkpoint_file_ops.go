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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoints

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/pingcap/errors"
)

// RemoveCheckpoint implements CheckpointsDB.RemoveCheckpoint.
func (cpdb *FileCheckpointsDB) RemoveCheckpoint(_ context.Context, tableName string) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	if tableName == allTables {
		cpdb.checkpoints.Reset()
		return errors.Trace(cpdb.exStorage.DeleteFile(cpdb.ctx, cpdb.fileName))
	}

	delete(cpdb.checkpoints.Checkpoints, tableName)
	return errors.Trace(cpdb.save())
}

// MoveCheckpoints implements CheckpointsDB.MoveCheckpoints.
func (cpdb *FileCheckpointsDB) MoveCheckpoints(_ context.Context, taskID int64) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	newFileName := fmt.Sprintf("%s.%d.bak", cpdb.fileName, taskID)
	return cpdb.exStorage.Rename(cpdb.ctx, cpdb.fileName, newFileName)
}

// GetLocalStoringTables implements CheckpointsDB.GetLocalStoringTables.
func (cpdb *FileCheckpointsDB) GetLocalStoringTables(_ context.Context) (map[string][]int32, error) {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	targetTables := make(map[string][]int32)

	for tableName, tableModel := range cpdb.checkpoints.Checkpoints {
		if tableModel.Status <= uint32(CheckpointStatusMaxInvalid) ||
			tableModel.Status >= uint32(CheckpointStatusIndexImported) {
			continue
		}
		for engineID, engineModel := range tableModel.Engines {
			if engineModel.Status <= uint32(CheckpointStatusMaxInvalid) ||
				engineModel.Status >= uint32(CheckpointStatusImported) {
				continue
			}

			for _, chunkModel := range engineModel.Chunks {
				if chunkModel.Pos > chunkModel.Offset {
					targetTables[tableName] = append(targetTables[tableName], engineID)
					break
				}
			}
		}
	}

	return targetTables, nil
}

// IgnoreErrorCheckpoint implements CheckpointsDB.IgnoreErrorCheckpoint.
func (cpdb *FileCheckpointsDB) IgnoreErrorCheckpoint(_ context.Context, targetTableName string) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	for tableName, tableModel := range cpdb.checkpoints.Checkpoints {
		if !(targetTableName == allTables || targetTableName == tableName) {
			continue
		}
		if tableModel.Status <= uint32(CheckpointStatusMaxInvalid) {
			tableModel.Status = uint32(CheckpointStatusLoaded)
		}
		for _, engineModel := range tableModel.Engines {
			if engineModel.Status <= uint32(CheckpointStatusMaxInvalid) {
				engineModel.Status = uint32(CheckpointStatusLoaded)
			}
		}
	}
	return errors.Trace(cpdb.save())
}

// DestroyErrorCheckpoint implements CheckpointsDB.DestroyErrorCheckpoint.
func (cpdb *FileCheckpointsDB) DestroyErrorCheckpoint(_ context.Context, targetTableName string) ([]DestroyedTableCheckpoint, error) {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	var targetTables []DestroyedTableCheckpoint

	for tableName, tableModel := range cpdb.checkpoints.Checkpoints {
		// Obtain the list of tables
		if !(targetTableName == allTables || targetTableName == tableName) {
			continue
		}
		if tableModel.Status <= uint32(CheckpointStatusMaxInvalid) {
			var minEngineID, maxEngineID int32 = math.MaxInt32, math.MinInt32
			for engineID := range tableModel.Engines {
				if engineID < minEngineID {
					minEngineID = engineID
				}
				if engineID > maxEngineID {
					maxEngineID = engineID
				}
			}

			targetTables = append(targetTables, DestroyedTableCheckpoint{
				TableName:   tableName,
				MinEngineID: minEngineID,
				MaxEngineID: maxEngineID,
			})
		}
	}

	// Delete the checkpoints
	for _, dtcp := range targetTables {
		delete(cpdb.checkpoints.Checkpoints, dtcp.TableName)
	}
	if err := cpdb.save(); err != nil {
		return nil, errors.Trace(err)
	}

	return targetTables, nil
}

// DumpTables implements CheckpointsDB.DumpTables.
func (cpdb *FileCheckpointsDB) DumpTables(context.Context, io.Writer) error {
	return errors.Errorf("dumping file checkpoint into CSV not unsupported, you may copy %s instead", cpdb.path)
}

// DumpEngines implements CheckpointsDB.DumpEngines.
func (cpdb *FileCheckpointsDB) DumpEngines(context.Context, io.Writer) error {
	return errors.Errorf("dumping file checkpoint into CSV not unsupported, you may copy %s instead", cpdb.path)
}

// DumpChunks implements CheckpointsDB.DumpChunks.
func (cpdb *FileCheckpointsDB) DumpChunks(context.Context, io.Writer) error {
	return errors.Errorf("dumping file checkpoint into CSV not unsupported, you may copy %s instead", cpdb.path)
}

func intSlice2Int32Slice(s []int) []int32 {
	res := make([]int32, 0, len(s))
	for _, i := range s {
		res = append(res, int32(i))
	}
	return res
}
