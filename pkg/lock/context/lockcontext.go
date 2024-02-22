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

package context

import "github.com/pingcap/tidb/pkg/parser/model"

// TableLockReadContext is the interface to get table lock information.
type TableLockReadContext interface {
	// CheckTableLocked checks the table lock.
	CheckTableLocked(tblID int64) (bool, model.TableLockType)
	// GetAllTableLocks gets all table locks table id and db id hold by the session.
	GetAllTableLocks() []model.TableLockTpInfo
	// HasLockedTables uses to check whether this session locked any tables.
	HasLockedTables() bool
}

// TableLockContext is the interface to operate table lock.
type TableLockContext interface {
	TableLockReadContext
	// AddTableLock adds table lock to the session lock map.
	AddTableLock([]model.TableLockTpInfo)
	// ReleaseTableLocks releases table locks in the session lock map.
	ReleaseTableLocks(locks []model.TableLockTpInfo)
	// ReleaseTableLockByTableIDs releases table locks in the session lock map by table IDs.
	ReleaseTableLockByTableIDs(tableIDs []int64)
	// ReleaseAllTableLocks releases all table locks hold by the session.
	ReleaseAllTableLocks()
}
