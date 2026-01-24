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
	"database/sql"
	"fmt"
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

// ErrFailoverCancel is the cancellation cause used by DM worker failover.
// When the import context is canceled with this cause, import-into backend should
// keep IMPORT INTO jobs running and let the next DM worker instance take over.
var ErrFailoverCancel = errors.New("lightning: failover cancel")

// ProgressUpdater is an interface for updating the progress of the import process.
type ProgressUpdater interface {
	UpdateTotalSize(size int64)
	UpdateFinishedSize(size int64)
}

// ImporterOption is a function that configures the Importer.
type ImporterOption func(*Importer)

// WithProgressUpdater sets the ProgressUpdater for the Importer.
func WithProgressUpdater(pu ProgressUpdater) ImporterOption {
	return func(i *Importer) {
		i.progressUpdater = pu
	}
}

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
	cfg             *config.Config
	db              *sql.DB
	sdk             importsdk.SDK
	logger          log.Logger
	cpMgr           CheckpointManager
	orchestrator    JobOrchestrator
	groupKey        string
	progressUpdater ProgressUpdater
}

// NewImporter creates a new Importer.
func NewImporter(
	ctx context.Context,
	cfg *config.Config,
	db *sql.DB,
	opts ...ImporterOption,
) (*Importer, error) {
	imp := &Importer{
		cfg: cfg,
		db:  db,
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
		ProgressUpdater:   i.progressUpdater,
	})
}

// Run starts the import process.
func (i *Importer) Run(ctx context.Context) error {
	err := i.runOnce(ctx)
	if common.IsContextCanceledError(err) {
		if errors.Cause(context.Cause(ctx)) == ErrFailoverCancel {
			i.logger.Info("context canceled by failover, skipping job cancellation")
			return err
		}

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
		if err := i.cpMgr.Remove(ctx, common.AllTables); err != nil {
			i.logger.Warn("failed to remove checkpoints", zap.Error(err))
		}
	}

	return nil
}

// Pause cancels the current import process.
// Since TiDB does not support PAUSE IMPORT JOB, we implement Pause by cancelling the jobs.
// The Resume operation will restart the jobs from checkpoints.
func (i *Importer) Pause(_ context.Context) error {
	i.logger.Info("pause is not supported for 'import into' backend")
	return nil
}

// Resume resumes the import process.
// It calls Run internally to restart the process from checkpoints.
func (i *Importer) Resume(_ context.Context) error {
	i.logger.Info("resume is not supported for 'import into' backend")
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
	if i.cpMgr != nil {
		if err := i.cpMgr.Close(); err != nil {
			i.logger.Warn("failed to close checkpoint manager", zap.Error(err))
		}
	}
	if i.sdk != nil {
		if err := i.sdk.Close(); err != nil {
			i.logger.Warn("failed to close sdk", zap.Error(err))
		}
	}
	if i.db != nil {
		if err := i.db.Close(); err != nil {
			i.logger.Warn("failed to close database connection", zap.Error(err))
		}
	}
}
