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
	"net/url"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/importsdk"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
)

// ImportJob represents a submitted import job with its metadata.
type ImportJob struct {
	JobID     int64
	TableMeta *importsdk.TableMeta
	GroupKey  string
}

// JobSubmitter is responsible for submitting import jobs to TiDB.
type JobSubmitter interface {
	SubmitTable(ctx context.Context, tableMeta *importsdk.TableMeta) (*ImportJob, error)
	GetGroupKey() string
}

// DefaultJobSubmitter is the default implementation of JobSubmitter.
type DefaultJobSubmitter struct {
	sdk      importsdk.SDK
	config   *config.Config
	groupKey string
	logger   log.Logger
}

// NewJobSubmitter creates a new job submitter.
func NewJobSubmitter(sdk importsdk.SDK, cfg *config.Config, groupKey string, logger log.Logger) JobSubmitter {
	return &DefaultJobSubmitter{
		sdk:      sdk,
		config:   cfg,
		groupKey: groupKey,
		logger:   logger,
	}
}

// SubmitTable submits an import job for a single table.
// It returns the job ID if successful, or an error if submission fails.
func (s *DefaultJobSubmitter) SubmitTable(ctx context.Context, tableMeta *importsdk.TableMeta) (*ImportJob, error) {
	logger := s.logger.With(
		zap.String("database", tableMeta.Database),
		zap.String("table", tableMeta.Table),
	)

	options := s.buildImportOptions(tableMeta)
	sql, err := s.sdk.GenerateImportSQL(tableMeta, options)
	if err != nil {
		return nil, errors.Annotate(err, "generate import SQL")
	}

	logger.Info("submitting import job", zap.String("sql", sql))
	jobID, err := s.sdk.SubmitJob(ctx, sql)
	if err != nil {
		return nil, errors.Annotate(err, "submit job")
	}

	logger.Info("import job submitted", zap.Int64("jobID", jobID))
	return &ImportJob{
		JobID:     jobID,
		TableMeta: tableMeta,
		GroupKey:  s.groupKey,
	}, nil
}

// GetGroupKey returns the group key.
func (s *DefaultJobSubmitter) GetGroupKey() string {
	return s.groupKey
}

func (s *DefaultJobSubmitter) buildImportOptions(tableMeta *importsdk.TableMeta) *importsdk.ImportOptions {
	cfg := s.config
	opts := &importsdk.ImportOptions{
		Detached:        true,
		GroupKey:        s.groupKey,
		DisablePrecheck: !cfg.App.CheckRequirements,
	}

	if len(tableMeta.DataFiles) > 0 {
		opts.Format = tableMeta.DataFiles[0].Format.String()
	}
	if opts.Format == "csv" {
		opts.CSVConfig = &cfg.Mydumper.CSV
		if cfg.Mydumper.CSV.Header {
			opts.SkipRows = 1
		}
	}

	if cfg.TikvImporter.DiskQuota > 0 && cfg.TikvImporter.DiskQuota != config.UnlimitedQuota {
		opts.DiskQuota = units.BytesSize(float64(cfg.TikvImporter.DiskQuota))
	}

	if cfg.TikvImporter.StoreWriteBWLimit > 0 {
		opts.MaxWriteSpeed = units.BytesSize(float64(cfg.TikvImporter.StoreWriteBWLimit))
	}

	opts.SplitFile = cfg.Mydumper.StrictFormat

	maxTypeError := cfg.App.MaxError.Type.Load()
	if maxTypeError > 0 {
		opts.RecordErrors = maxTypeError
	}

	// Set character set
	if cfg.Mydumper.DataCharacterSet != "" && cfg.Mydumper.DataCharacterSet != "binary" {
		opts.CharacterSet = cfg.Mydumper.DataCharacterSet
	}

	// Set checksum option based on post-restore config
	switch cfg.PostRestore.Checksum {
	case config.OpLevelRequired:
		opts.ChecksumTable = "required"
	case config.OpLevelOptional:
		opts.ChecksumTable = "optional"
	case config.OpLevelOff:
		opts.ChecksumTable = "off"
	}

	if cfg.Mydumper.SourceDir != "" {
		u, err := url.Parse(cfg.Mydumper.SourceDir)
		if err == nil {
			opts.ResourceParameters = u.RawQuery
		}
	}

	return opts
}
