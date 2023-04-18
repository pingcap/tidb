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

package checkpoint

import (
	"context"
	"fmt"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type LogRestoreKeyType = string
type LogRestoreValueType struct {
	// downstream table id
	TableID int64
	// group index in the metadata
	Goff int
	// file index in the group
	Foff int
}

func (l LogRestoreValueType) IdentKey() []byte {
	return []byte(fmt.Sprint(l.Goff, '.', l.Foff, '.', l.TableID))
}

type LogRestoreRunner = CheckpointRunner[LogRestoreKeyType, LogRestoreValueType]

// only for test
func StartCheckpointLogRestoreRunnerForTest(
	ctx context.Context,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	tick time.Duration,
	taskName string,
) (*LogRestoreRunner, error) {
	runner := newCheckpointRunner[LogRestoreKeyType, LogRestoreValueType](
		ctx, storage, cipher, nil, flushPositionForRestore(taskName))

	runner.startCheckpointMainLoop(ctx, tick, 0)
	return runner, nil
}

func StartCheckpointRunnerForLogRestore(ctx context.Context,
	storage storage.ExternalStorage,
	cipher *backuppb.CipherInfo,
	taskName string,
) (*LogRestoreRunner, error) {
	runner := newCheckpointRunner[LogRestoreKeyType, LogRestoreValueType](
		ctx, storage, cipher, nil, flushPositionForRestore(taskName))

	// for restore, no need to set lock
	runner.startCheckpointMainLoop(ctx, tickDurationForFlush, 0)
	return runner, nil
}

func AppendRangeForLogRestore(
	ctx context.Context,
	r *LogRestoreRunner,
	groupKey LogRestoreKeyType,
	tableID int64,
	goff int,
	foff int,
) error {
	return r.Append(ctx, &CheckpointMessage[LogRestoreKeyType, LogRestoreValueType]{
		GroupKey: groupKey,
		Group: []LogRestoreValueType{
			{
				TableID: tableID,
				Goff:    goff,
				Foff:    foff,
			},
		},
	})
}
