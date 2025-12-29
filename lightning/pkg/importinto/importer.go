// Copyright 2025 PingCAP, Inc.
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
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/importsdk"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
)

const (
	cancelTimeout = time.Minute
)

// ImporterOption is a function that configures the Importer.
type ImporterOption func(*Importer)

// WithCheckpointManager sets the CheckpointManager for the Importer.
func WithCheckpointManager(cpMgr CheckpointManager) ImporterOption {
	return func(i *Importer) {
		i.cpMgr = cpMgr
	}
}

// WithBackendSDK sets the BackendSDK for the Importer.
func WithBackendSDK(sdk importsdk.SDK) ImporterOption {
	return func(i *Importer) {
		i.sdk = sdk
	}
}

// WithOrchestrator sets the JobOrchestrator for the Importer.
func WithOrchestrator(orchestrator JobOrchestrator) ImporterOption {
	return func(i *Importer) {
		i.orchestrator = orchestrator
	}
}

// Importer is the implementation of LightningImporter for the 'import into' backend.
type Importer struct {
	cfg          *config.Config
	db           *sql.DB
	sdk          importsdk.SDK
	logger       log.Logger
	cpMgr        CheckpointManager
	orchestrator JobOrchestrator
	groupKey     string

	isPaused atomic.Bool
	resumeCh chan struct{}
}

// NewImporter creates a new Importer.
func NewImporter(
	ctx context.Context,
	cfg *config.Config,
	db *sql.DB,
	opts ...ImporterOption,
) (*Importer, error) {
	imp := &Importer{
		cfg:      cfg,
		db:       db,
		resumeCh: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(imp)
	}

	if imp.logger.Logger == nil {
		imp.logger = log.L().With(zap.String("backend", "import-into"))
	}

	if imp.sdk == nil {
		sdkOpts := []importsdk.SDKOption{
			importsdk.WithSQLMode(cfg.TiDB.SQLMode),
			importsdk.WithFilter(cfg.Mydumper.Filter),
			importsdk.WithFileRouters(cfg.Mydumper.FileRouters),
			importsdk.WithRoutes(cfg.Routes),
			importsdk.WithCharset(cfg.Mydumper.CharacterSet),
			importsdk.WithLogger(imp.logger),
		}
		sdk, err := importsdk.NewImportSDK(ctx, cfg.Mydumper.SourceDir, db, sdkOpts...)
		if err != nil {
			return nil, err
		}
		imp.sdk = sdk
	}

	if imp.cpMgr == nil {
		cpMgr, err := NewCheckpointManager(cfg)
		if err != nil {
			return nil, err
		}
		imp.cpMgr = cpMgr
	}

	if err := imp.cpMgr.Initialize(ctx); err != nil {
		return nil, err
	}

	if err := imp.initGroupKey(ctx); err != nil {
		return nil, err
	}

	if imp.orchestrator == nil {
		imp.orchestrator = imp.buildOrchestrator()
	}

	return imp, nil
}

func (i *Importer) buildOrchestrator() JobOrchestrator {
	submitter := NewJobSubmitter(i.sdk, i.cfg, i.groupKey, i.logger.With(zap.String("component", "submitter")))

	return NewJobOrchestrator(OrchestratorConfig{
		Submitter:         submitter,
		CheckpointMgr:     i.cpMgr,
		SDK:               i.sdk,
		SubmitConcurrency: i.cfg.App.TableConcurrency,
		PollInterval:      DefaultPollInterval,
		LogInterval:       i.cfg.Cron.LogProgress.Duration,
		Logger:            i.logger.With(zap.String("component", "orchestrator")),
	})
}

// Run starts the import process.
func (i *Importer) Run(ctx context.Context) error {
	err := i.runWithPauser(ctx, i.runOnce)
	if common.IsContextCanceledError(err) {
		i.logger.Info("context canceled, cancelling import jobs...")
		cancelCtx, cancel := context.WithTimeout(context.Background(), cancelTimeout)
		defer cancel()

		if cancelErr := i.orchestrator.Cancel(cancelCtx); cancelErr != nil {
			i.logger.Warn("failed to cancel import jobs", zap.Error(cancelErr))
		} else {
			i.logger.Info("import jobs cancelled successfully")
		}
	}
	return err
}

// runWithPauser executes the given function, and retries it after resume if paused.
func (i *Importer) runWithPauser(ctx context.Context, fn func(context.Context) error) error {
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}

		// If not paused, return the error directly
		if !i.isPaused.Load() {
			return err
		}

		// Wait for resume
		if !i.waitForResume(ctx) {
			return ctx.Err()
		}
	}
}

// waitForResume waits for the resume signal or context cancellation.
// Returns true if resumed, false if context is done.
func (i *Importer) waitForResume(ctx context.Context) bool {
	i.logger.Info("import paused, waiting for resume")
	select {
	case <-i.resumeCh:
		i.logger.Info("resuming import...")
		i.isPaused.Store(false)
		return true
	case <-ctx.Done():
		return false
	}
}

func (i *Importer) runOnce(ctx context.Context) error {
	if err := i.sdk.CreateSchemasAndTables(ctx); err != nil {
		return err
	}

	tables, err := i.sdk.GetTableMetas(ctx)
	if err != nil {
		return err
	}

	// Run prechecks if enabled
	if i.cfg.App.CheckRequirements {
		if err := i.runPrechecks(ctx); err != nil {
			return err
		}
	} else {
		i.logger.Info("skipping prechecks as CheckRequirements is disabled")
	}

	// Use orchestrator to handle submission and monitoring
	if err := i.orchestrator.SubmitAndWait(ctx, tables); err != nil {
		return err
	}

	if i.cfg.Checkpoint.Enable && i.cfg.Checkpoint.KeepAfterSuccess == config.CheckpointRemove {
		i.logger.Info("removing all checkpoints")
		if err := i.cpMgr.Remove(ctx, "all"); err != nil {
			i.logger.Warn("failed to remove checkpoints", zap.Error(err))
		}
	}

	return nil
}

// Pause cancels the current import process.
// Since TiDB does not support PAUSE IMPORT JOB, we implement Pause by cancelling the jobs.
// The Resume operation will restart the jobs from checkpoints.
func (i *Importer) Pause(ctx context.Context) error {
	i.logger.Info("pausing import by cancelling jobs")
	i.isPaused.Store(true)
	return i.orchestrator.Cancel(ctx)
}

// Resume resumes the import process.
// It calls Run internally to restart the process from checkpoints.
func (i *Importer) Resume(ctx context.Context) error {
	if !i.isPaused.Load() {
		return nil
	}
	i.logger.Info("resuming import")
	select {
	case i.resumeCh <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (i *Importer) runPrechecks(ctx context.Context) error {
	i.logger.Info("running prechecks")

	precheckRunner := NewPrecheckRunner()

	// Register all precheck items
	precheckRunner.Register(NewCheckpointCheckItem(i.cfg, i.cpMgr))

	if err := precheckRunner.Run(ctx); err != nil {
		i.logger.Error("precheck failed", zap.Error(err))
		return errors.Annotate(err, "precheck failed")
	}

	i.logger.Info("all prechecks passed")
	return nil
}

func (i *Importer) initGroupKey(ctx context.Context) error {
	// 1. Try to load from existing checkpoints
	cps, err := i.cpMgr.GetCheckpoints(ctx)
	if err != nil {
		return err
	}

	for _, cp := range cps {
		if cp.GroupKey != "" {
			i.groupKey = cp.GroupKey
			i.logger.Info("restored group key from checkpoint", zap.String("groupKey", i.groupKey))
			return nil
		}
	}

	// 2. Generate new group key
	i.groupKey = fmt.Sprintf("lightning-%s", uuid.New().String())
	i.logger.Info("generated new group key", zap.String("groupKey", i.groupKey))

	return nil
}

// Close closes the importer and releases resources.
func (i *Importer) Close() {
	logger := i.logger
	if logger.Logger == nil {
		logger = log.L()
	}

	if i.cpMgr != nil {
		if err := i.cpMgr.Close(); err != nil {
			logger.Warn("failed to close checkpoint manager", zap.Error(err))
		}
	}
	if i.sdk != nil {
		if err := i.sdk.Close(); err != nil {
			logger.Warn("failed to close sdk", zap.Error(err))
		}
	}
	if i.db != nil {
		if err := i.db.Close(); err != nil {
			logger.Warn("failed to close database connection", zap.Error(err))
		}
	}
}
