// Copyright 2026 PingCAP, Inc.
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

package importinto

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/lightning/pkg/precheck"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
)

// PrecheckRunner runs prechecks.
type PrecheckRunner struct {
	checkers []precheck.Checker
}

// NewPrecheckRunner creates a new PrecheckRunner.
func NewPrecheckRunner() *PrecheckRunner {
	return &PrecheckRunner{
		checkers: make([]precheck.Checker, 0),
	}
}

// Register registers a checker.
func (r *PrecheckRunner) Register(checker precheck.Checker) {
	r.checkers = append(r.checkers, checker)
}

// Run runs all registered checkers.
func (r *PrecheckRunner) Run(ctx context.Context) error {
	for _, checker := range r.checkers {
		itemID := checker.GetCheckItemID()
		log.L().Debug("running precheck", zap.String("item", string(itemID)))

		res, err := checker.Check(ctx)
		if err != nil {
			log.L().Error("precheck error", zap.String("item", string(itemID)), zap.Error(err))
			return errors.Annotatef(err, "precheck %s failed", itemID)
		}

		if !res.Passed {
			log.L().Error("precheck failed", zap.String("item", string(itemID)), zap.String("message", res.Message))
			return errors.Errorf("precheck %s failed: %s", itemID, res.Message)
		}

		if res.Message != "" {
			log.L().Info("precheck passed", zap.String("item", string(itemID)), zap.String("message", res.Message))
		} else {
			log.L().Info("precheck passed", zap.String("item", string(itemID)))
		}
	}
	return nil
}

// CheckpointCheckItem validates the existing checkpoint state before import starts.
type CheckpointCheckItem struct {
	cfg   *config.Config
	cpMgr CheckpointManager
}

// NewCheckpointCheckItem returns a checkpoint precheck implementation.
func NewCheckpointCheckItem(cfg *config.Config, cpMgr CheckpointManager) *CheckpointCheckItem {
	return &CheckpointCheckItem{cfg: cfg, cpMgr: cpMgr}
}

// GetCheckItemID implements precheck.Checker.
func (*CheckpointCheckItem) GetCheckItemID() precheck.CheckItemID {
	return precheck.CheckCheckpoints
}

// Check validates that checkpoints are in a resumable state.
func (c *CheckpointCheckItem) Check(ctx context.Context) (*precheck.CheckResult, error) {
	if !c.cfg.Checkpoint.Enable {
		return &precheck.CheckResult{
			Passed: true,
		}, nil
	}

	cps, err := c.cpMgr.GetCheckpoints(ctx)
	if err != nil {
		return nil, err
	}

	if len(cps) == 0 {
		return &precheck.CheckResult{
			Passed: true,
		}, nil
	}

	for _, cp := range cps {
		if cp.Status == CheckpointStatusFailed {
			return &precheck.CheckResult{
				Passed:  false,
				Message: fmt.Sprintf("The checkpoint table contains failed tasks (e.g. table %s). Please use `tidb-lightning-ctl --checkpoint-error-destroy=all` to clean up the failed checkpoints, or `tidb-lightning-ctl --checkpoint-remove=all` to remove all checkpoints.", cp.TableName),
			}, nil
		}
	}

	return &precheck.CheckResult{
		Passed:   true,
		Severity: precheck.Warn,
		Message:  "The checkpoint table is not empty. If you want to resume the import, please use the same configuration. If you want to start a new import, please use `tidb-lightning-ctl --checkpoint-remove=all` to remove the checkpoints.",
	}, nil
}
