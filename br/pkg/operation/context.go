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
	"slices"
	"strconv"
	"strings"
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
// lock-capable workers. Copies carry value snapshots of operation fields.
type Context struct {
	OperationID string    `json:"operation_id"`
	StartedAt   time.Time `json:"operation_started_at"`
	hintFields  []HintField
}

// HintField is additional caller-owned metadata rendered into lock hints.
type HintField struct {
	Key   string `json:"key"`
	Value string `json:"value"`
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
	operationID, err := uuid.NewRandom()
	if err != nil {
		return Context{}, errors.Annotate(err, "failed to generate operation ID")
	}

	ctx := Context{
		OperationID: operationID.String(),
		StartedAt:   time.Now(),
	}

	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	log.Info("BR operation started",
		zap.String("operation_id", ctx.OperationID),
		zap.Time("operation_started_at", ctx.StartedAt),
		zap.String("host", host),
		zap.Int("pid", os.Getpid()),
		zap.String("command", command),
	)
	return ctx, nil
}

// HintFields returns a copy of additional caller-owned metadata for lock hints.
func (c Context) HintFields() []HintField {
	return slices.Clone(c.hintFields)
}

// SetHintField records additional caller-owned metadata for lock hints.
func (c *Context) SetHintField(key, value string) {
	if key == "" {
		return
	}
	if value != "" && !c.isInitialized() {
		return
	}

	fieldIdx := c.hintFieldIndex(key)
	if value == "" {
		if fieldIdx >= 0 {
			c.hintFields = slices.Clone(c.hintFields)
			c.hintFields = append(c.hintFields[:fieldIdx], c.hintFields[fieldIdx+1:]...)
		}
		return
	}

	c.hintFields = slices.Clone(c.hintFields)
	if fieldIdx >= 0 {
		oldValue := c.hintFields[fieldIdx].Value
		if oldValue != value {
			log.Warn("BR operation hint field changed",
				zap.String("operation_id", c.OperationID),
				zap.Time("operation_started_at", c.StartedAt),
				zap.String("hint_key", key),
				zap.String("old_value", oldValue),
				zap.String("new_value", value),
			)
		}
		c.hintFields[fieldIdx].Value = value
	} else {
		c.hintFields = append(c.hintFields, HintField{Key: key, Value: value})
	}

	log.Info("BR operation hint field resolved",
		zap.String("operation_id", c.OperationID),
		zap.Time("operation_started_at", c.StartedAt),
		zap.String("hint_key", key),
		zap.String("hint_value", value),
	)
}

func (c Context) isInitialized() bool {
	return c.OperationID != "" && !c.StartedAt.IsZero()
}

func (c Context) hintFieldIndex(key string) int {
	for i, field := range c.hintFields {
		if field.Key == key {
			return i
		}
	}
	return -1
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

	return objstore.LockMetaInput{
		OwnerID:  c.OperationID,
		LockType: string(resource),
		Hint:     c.lockHint(hint),
	}, nil
}

func (c Context) lockHint(detail string) string {
	fields := []string{
		"operation_started_at=" + c.StartedAt.Format(time.RFC3339),
	}
	for _, field := range c.hintFields {
		if field.Key != "" && field.Value != "" {
			fields = append(fields, field.Key+"="+field.Value)
		}
	}
	if detail != "" {
		fields = append(fields, "detail="+strconv.Quote(detail))
	}
	return strings.Join(fields, " ")
}
