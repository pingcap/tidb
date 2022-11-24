// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttl

import (
	"context"
	"time"
)

type TableStatus string

const (
	TableStatusWaiting    TableStatus = "waiting"
	TableStatusRunning                = "running"
	TableStatusCancelling             = "cancelling"
	TableStatusCancelled              = "cancelled"
	TableStatusDone                   = "done"
	TableStatusFailed                 = "failed"
)

type JobInfo struct {
	ID        string
	TableID   int64
	StartTime time.Time
}

type TableStateInfo struct {
	ID     int64
	Status string
}

type TableStatesStore interface {
	CreateNewJob(ctx context.Context, tblID int64, owner string) error
}

func NewTableStatesStore() TableStatesStore {
	// TODO:
	return nil
}

type TableStatesCache struct {
	tables map[int64]*TableStateInfo
}

func NewTableStatesCache() *TableStatesCache {
	return &TableStatesCache{
		tables: make(map[int64]*TableStateInfo),
	}
}

func (c *TableStatesCache) Update(ctx context.Context, se *Session) error {
	// TODO:
	return nil
}

func (c *TableStatesCache) UpdateTable(ctx context.Context, se *Session, tblID int) error {
	// TODO:
	return nil
}

func (c *TableStatesCache) GetTableState(id int64) (info *TableStateInfo, ok bool) {
	info, ok = c.tables[id]
	return
}
