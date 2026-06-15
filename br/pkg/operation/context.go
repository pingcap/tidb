// Copyright 2026 PingCAP, Inc.
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

package operation

import (
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/objstore"
	"go.uber.org/zap"
)

// Context carries the runtime identity of one BR operation.
//
// Initialize and update a Context at the command boundary before copying it into
// lock-capable workers. Copies carry value snapshots of operation fields. A copy
// made from an initialized Context shares restore-ID logging dedupe state with
// the original, so repeated SetRestoreID calls for the same restore ID do not
// emit duplicate operation logs.
type Context struct {
	OperationID string    `json:"operation_id"`
	StartedAt   time.Time `json:"operation_started_at"`
	RestoreID   uint64    `json:"restore_id,omitempty"`

	logState *logState
}

type logState struct {
	mu               sync.Mutex
	loggedRestoreIDs map[uint64]struct{}
}

// LockResourceType identifies the coarse resource protected by a lock file.
type LockResourceType string

const (
	LockResourceLogTruncateExclusive LockResourceType = "log-truncate-exclusive"
	LockResourceMigrationRead        LockResourceType = "migration-read"
	LockResourceMigrationWrite       LockResourceType = "migration-write"
	LockResourceMigrationAppend      LockResourceType = "migration-append"
)

// NewContext creates and logs a BR operation context for a command execution.
func NewContext(command string) (Context, error) {
	var ctx Context
	if err := ctx.Ensure(command); err != nil {
		return Context{}, err
	}
	return ctx, nil
}

// Ensure initializes and logs the operation context if it has not been created.
func (c *Context) Ensure(command string) error {
	if c.OperationID != "" {
		if c.StartedAt.IsZero() {
			return errors.New("operation started time is required")
		}
		c.ensureLogState()
		return nil
	}

	operationID, err := uuid.NewRandom()
	if err != nil {
		return errors.Annotate(err, "failed to generate operation ID")
	}

	c.OperationID = operationID.String()
	c.StartedAt = time.Now()
	c.logState = newLogState()

	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	log.Info("BR operation started",
		zap.String("operation_id", c.OperationID),
		zap.Time("operation_started_at", c.StartedAt),
		zap.String("host", host),
		zap.Int("pid", os.Getpid()),
		zap.String("command", command),
	)
	return nil
}

// SetRestoreID records the restore lineage for the operation.
func (c *Context) SetRestoreID(restoreID uint64) {
	if restoreID != 0 && !c.isInitialized() {
		return
	}
	if c.RestoreID == restoreID {
		return
	}

	c.RestoreID = restoreID
	if restoreID == 0 {
		return
	}
	if !c.markRestoreIDLogged(restoreID) {
		return
	}

	log.Info("BR operation restore ID resolved",
		zap.String("operation_id", c.OperationID),
		zap.Time("operation_started_at", c.StartedAt),
		zap.Uint64("restore_id", restoreID),
	)
}

func (c Context) isInitialized() bool {
	return c.OperationID != "" && !c.StartedAt.IsZero()
}

// LockMeta builds object-storage lock metadata from the BR operation context.
func (c Context) LockMeta(resource LockResourceType, hint string) (objstore.LockMetaInput, error) {
	if c.OperationID == "" {
		return objstore.LockMetaInput{}, errors.New("operation ID is required")
	}
	if c.StartedAt.IsZero() {
		return objstore.LockMetaInput{}, errors.New("operation started time is required")
	}
	if resource == "" {
		return objstore.LockMetaInput{}, errors.New("lock resource type is required")
	}

	startedAt := c.StartedAt
	return objstore.LockMetaInput{
		OperationID:        c.OperationID,
		OperationStartedAt: &startedAt,
		RestoreID:          c.RestoreID,
		ResourceType:       string(resource),
		Hint:               hint,
	}, nil
}

func (c *Context) ensureLogState() {
	if c.logState == nil {
		c.logState = newLogState()
	}
}

func newLogState() *logState {
	return &logState{loggedRestoreIDs: make(map[uint64]struct{})}
}

func (c *Context) markRestoreIDLogged(restoreID uint64) bool {
	c.ensureLogState()

	c.logState.mu.Lock()
	defer c.logState.mu.Unlock()

	if _, ok := c.logState.loggedRestoreIDs[restoreID]; ok {
		return false
	}
	c.logState.loggedRestoreIDs[restoreID] = struct{}{}
	return true
}
