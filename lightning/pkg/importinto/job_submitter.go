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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/importsdk"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/s3like"
	"github.com/pingcap/tidb/pkg/parser/ast"
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
	sdk                           importsdk.SDK
	config                        *config.Config
	groupKey                      string
	logger                        log.Logger
	stripS3ExternalIDForImportSQL bool
}

// JobSubmitterOption is a function that configures the JobSubmitter.
type JobSubmitterOption func(*DefaultJobSubmitter)

// WithJobSubmitterStripS3ExternalIDForImportSQL strips explicit S3 external ID
// from IMPORT INTO SQL resource parameters. The original source directory is
// kept unchanged for Lightning's own storage access. Enable it only for callers
// that need to keep compatibility with older IMPORT INTO planners that reject
// explicit S3 external ID; newer planners can accept an explicit external ID
// matching the target value.
func WithJobSubmitterStripS3ExternalIDForImportSQL(strip bool) JobSubmitterOption {
	return func(s *DefaultJobSubmitter) {
		s.stripS3ExternalIDForImportSQL = strip
	}
}

// NewJobSubmitter creates a new job submitter.
func NewJobSubmitter(sdk importsdk.SDK, cfg *config.Config, groupKey string, logger log.Logger, opts ...JobSubmitterOption) JobSubmitter {
	submitter := &DefaultJobSubmitter{
		sdk:      sdk,
		config:   cfg,
		groupKey: groupKey,
		logger:   logger,
	}
	WithJobSubmitterStripS3ExternalIDForImportSQL(cfg.TikvImporter.StripS3ExternalIDForImportSQL)(submitter)
	for _, opt := range opts {
		opt(submitter)
	}
	return submitter
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

	sqlForLog, logErr := generateImportSQLForLog(tableMeta, options)
	if logErr != nil {
		logger.Info("submitting import job")
	} else {
		logger.Info("submitting import job", zap.String("sql", sqlForLog))
	}
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

	opts.SplitFile = cfg.Mydumper.StrictFormat

	maxTypeError := cfg.App.MaxError.Type.Load()
	if maxTypeError > 0 {
		opts.RecordErrors = maxTypeError
	}

	// Set character set
	if cfg.Mydumper.DataCharacterSet != "" && cfg.Mydumper.DataCharacterSet != "binary" {
		opts.CharacterSet = cfg.Mydumper.DataCharacterSet
	}

	if cfg.Mydumper.SourceDir != "" {
		u, err := url.Parse(cfg.Mydumper.SourceDir)
		if err == nil {
			opts.ResourceParameters = buildResourceParametersForImportSQL(u, s.stripS3ExternalIDForImportSQL)
		}
	}

	return opts
}

func buildResourceParametersForImportSQL(u *url.URL, stripS3ExternalID bool) string {
	if !(stripS3ExternalID && objstore.IsS3Like(u)) {
		return u.RawQuery
	}

	values := u.Query()
	if !stripS3ExternalIDResourceParameters(values) {
		return u.RawQuery
	}
	return values.Encode()
}

func stripS3ExternalIDResourceParameters(values url.Values) bool {
	stripped := false
	for key := range values {
		if objstore.NormalizeQueryParameterKey(key) == s3like.S3ExternalID {
			delete(values, key)
			stripped = true
		}
	}
	return stripped
}

func generateImportSQLForLog(tableMeta *importsdk.TableMeta, options *importsdk.ImportOptions) (string, error) {
	// Best-effort redaction: build a second SQL statement for logging purposes.
	redactedMeta := *tableMeta
	redactedOpts := *options

	path := redactedMeta.WildcardPath
	if redactedOpts.ResourceParameters != "" {
		// Same logic as importsdk.sqlGenerator.GenerateImportSQL: append resource
		// parameters to the wildcard path, then redact secrets from the full URL.
		u, err := url.Parse(path)
		if err == nil {
			if u.RawQuery != "" {
				u.RawQuery += "&" + redactedOpts.ResourceParameters
			} else {
				u.RawQuery = redactedOpts.ResourceParameters
			}
			path = u.String()
		}
		// Avoid adding ResourceParameters twice when re-generating SQL.
		redactedOpts.ResourceParameters = ""
	}
	redactedMeta.WildcardPath = ast.RedactURL(path)
	if redactedOpts.CloudStorageURI != "" {
		redactedOpts.CloudStorageURI = ast.RedactURL(redactedOpts.CloudStorageURI)
	}

	return importsdk.NewSQLGenerator().GenerateImportSQL(&redactedMeta, &redactedOpts)
}
