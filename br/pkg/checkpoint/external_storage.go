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

package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type externalCheckpointStorage struct {
	flushPosition
	storage storage.ExternalStorage

	lockId uint64
	timer  GlobalTimer
}

func newExternalCheckpointStorage(
	ctx context.Context,
	s storage.ExternalStorage,
	timer GlobalTimer,
) (*externalCheckpointStorage, error) {
	checkpointStorage := &externalCheckpointStorage{
		flushPosition: flushPositionForBackup(),
		storage:       s,
		timer:         timer,
	}
	if timer != nil {
		if err := checkpointStorage.initialLock(ctx); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return checkpointStorage, nil
}

func (s *externalCheckpointStorage) close() {}

func (s *externalCheckpointStorage) flushCheckpointData(ctx context.Context, data []byte) error {
	fname := fmt.Sprintf("%s/%x.cpt", s.CheckpointDataDir, uuid.New())
	return s.storage.WriteFile(ctx, fname, data)
}

func (s *externalCheckpointStorage) flushCheckpointChecksum(ctx context.Context, data []byte) error {
	fname := fmt.Sprintf("%s/%x.cpt", s.CheckpointChecksumDir, uuid.New())
	return s.storage.WriteFile(ctx, fname, data)
}

func (s *externalCheckpointStorage) getTS(ctx context.Context) (int64, int64, error) {
	var (
		p     int64 = 0
		l     int64 = 0
		retry int   = 0
	)
	errRetry := utils.WithRetry(ctx, func() error {
		var err error
		p, l, err = s.timer.GetTS(ctx)
		if err != nil {
			retry++
			log.Info("failed to get ts", zap.Int("retry", retry), zap.Error(err))
			return err
		}

		return nil
	}, utils.NewAggressivePDBackoffStrategy())

	return p, l, errors.Trace(errRetry)
}

type CheckpointLock struct {
	LockId   uint64 `json:"lock-id"`
	ExpireAt int64  `json:"expire-at"`
}

// flush the lock to the external storage
func (s *externalCheckpointStorage) flushLock(ctx context.Context, p int64) error {
	lock := &CheckpointLock{
		LockId:   s.lockId,
		ExpireAt: p + lockTimeToLive.Milliseconds(),
	}
	log.Info("start to flush the checkpoint lock", zap.Int64("lock-at", p),
		zap.Int64("expire-at", lock.ExpireAt))
	data, err := json.Marshal(lock)
	if err != nil {
		return errors.Trace(err)
	}

	err = s.storage.WriteFile(ctx, s.CheckpointLockPath, data)
	return errors.Trace(err)
}

// check whether this lock belongs to this BR
func (s *externalCheckpointStorage) checkLockFile(ctx context.Context, now int64) error {
	data, err := s.storage.ReadFile(ctx, s.CheckpointLockPath)
	if err != nil {
		return errors.Trace(err)
	}
	lock := &CheckpointLock{}
	err = json.Unmarshal(data, lock)
	if err != nil {
		return errors.Trace(err)
	}
	if lock.ExpireAt <= now {
		if lock.LockId > s.lockId {
			return errors.Errorf("There are another BR(%d) running after but setting lock before this one(%d). "+
				"Please check whether the BR is running. If not, you can retry.", lock.LockId, s.lockId)
		}
		if lock.LockId == s.lockId {
			log.Warn("The lock has expired.",
				zap.Int64("expire-at(ms)", lock.ExpireAt), zap.Int64("now(ms)", now))
		}
	} else if lock.LockId != s.lockId {
		return errors.Errorf("The existing lock will expire in %d seconds. "+
			"There may be another BR(%d) running. If not, you can wait for the lock to expire, "+
			"or delete the file `%s%s` manually.",
			(lock.ExpireAt-now)/1000, lock.LockId, strings.TrimRight(s.storage.URI(), "/"), s.CheckpointLockPath)
	}

	return nil
}

// Attempt to initialize the lock. Need to stop the backup when there is an unexpired locks.
func (s *externalCheckpointStorage) initialLock(ctx context.Context) error {
	p, l, err := s.getTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	s.lockId = oracle.ComposeTS(p, l)
	exist, err := s.storage.FileExists(ctx, s.CheckpointLockPath)
	if err != nil {
		return errors.Trace(err)
	}
	if exist {
		if err := s.checkLockFile(ctx, p); err != nil {
			return errors.Trace(err)
		}
	}
	if err = s.flushLock(ctx, p); err != nil {
		return errors.Trace(err)
	}

	// wait for 3 seconds to check whether the lock file is overwritten by another BR
	time.Sleep(3 * time.Second)
	err = s.checkLockFile(ctx, p)
	return errors.Trace(err)
}

// generate a new lock and flush the lock to the external storage
func (s *externalCheckpointStorage) updateLock(ctx context.Context) error {
	p, _, err := s.getTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if err = s.checkLockFile(ctx, p); err != nil {
		return errors.Trace(err)
	}
	if err = s.flushLock(ctx, p); err != nil {
		return errors.Trace(err)
	}

	failpoint.Inject("failed-after-checkpoint-updates-lock", func(_ failpoint.Value) {
		failpoint.Return(errors.Errorf("failpoint: failed after checkpoint updates lock"))
	})

	return nil
}

func (s *externalCheckpointStorage) deleteLock(ctx context.Context) {
	if s.lockId > 0 {
		err := s.storage.DeleteFile(ctx, s.CheckpointLockPath)
		if err != nil {
			log.Warn("failed to remove the checkpoint lock", zap.Error(err))
		}
	}
}
