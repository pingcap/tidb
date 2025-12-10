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

package importsdk

import (
	"context"
	"database/sql"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
)

// SDK defines the interface for cloud import services
type SDK interface {
	// CreateSchemasAndTables creates all database schemas and tables from source path
	CreateSchemasAndTables(ctx context.Context) error

	// GetTableMetas returns metadata for all tables in the source path
	GetTableMetas(ctx context.Context) ([]*TableMeta, error)

	// GetTableMetaByName returns metadata for a specific table
	GetTableMetaByName(ctx context.Context, schema, table string) (*TableMeta, error)

	// GetTotalSize returns the cumulative size (in bytes) of all data files under the source path
	GetTotalSize(ctx context.Context) int64

	// SubmitImportJob runs an IMPORT INTO statement and returns the resulting job metadata.
	SubmitImportJob(ctx context.Context, importSQL string) (*JobDetail, error)

	// FetchImportJob retrieves a single IMPORT INTO job via SHOW IMPORT JOB <id>.
	FetchImportJob(ctx context.Context, jobID int64) (*JobDetail, error)

	// FetchImportJobs lists all visible IMPORT INTO jobs via SHOW IMPORT JOBS.
	FetchImportJobs(ctx context.Context) ([]*JobDetail, error)

	// CancelImportJob cancels an IMPORT INTO job via CANCEL IMPORT JOB <id>.
	CancelImportJob(ctx context.Context, jobID int64) error

	// PauseImportJob pauses a running IMPORT INTO job via PAUSE IMPORT JOB <id>.
	PauseImportJob(ctx context.Context, jobID int64) error

	// ResumeImportJob resumes a paused IMPORT INTO job via RESUME IMPORT JOB <id>.
	ResumeImportJob(ctx context.Context, jobID int64) error

	// Close releases resources used by the SDK
	Close() error
}

// ImportSDK implements SDK interface
type ImportSDK struct {
	sourcePath string
	db         *sql.DB
	store      storage.ExternalStorage
	loader     *mydump.MDLoader
	logger     log.Logger
	config     *sdkConfig
}

// NewImportSDK creates a new CloudImportSDK instance
func NewImportSDK(ctx context.Context, sourcePath string, db *sql.DB, options ...SDKOption) (SDK, error) {
	cfg := defaultSDKConfig()
	for _, opt := range options {
		opt(cfg)
	}

	u, err := storage.ParseBackend(sourcePath, nil)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to parse storage backend URL (source=%s). Please verify the URL format and credentials", sourcePath)
	}
	store, err := storage.New(ctx, u, &storage.ExternalStorageOptions{})
	if err != nil {
		return nil, errors.Annotatef(err, "failed to create external storage (source=%s). Check network/connectivity and permissions", sourcePath)
	}

	ldrCfg := mydump.LoaderConfig{
		SourceURL:        sourcePath,
		Filter:           cfg.filter,
		FileRouters:      cfg.fileRouteRules,
		DefaultFileRules: len(cfg.fileRouteRules) == 0,
		CharacterSet:     cfg.charset,
	}

	loader, err := mydump.NewLoaderWithStore(ctx, ldrCfg, store)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to create MyDump loader (source=%s, charset=%s, filter=%v). Please check dump layout and router rules", sourcePath, cfg.charset, cfg.filter)
	}

	return &ImportSDK{
		sourcePath: sourcePath,
		db:         db,
		store:      store,
		loader:     loader,
		logger:     cfg.logger,
		config:     cfg,
	}, nil
}

// Close implements CloudImportSDK interface
func (sdk *ImportSDK) Close() error {
	// close external storage
	if sdk.store != nil {
		sdk.store.Close()
	}
	return nil
}
