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
)

// SDK defines the interface for cloud import services
type SDK interface {
	FileScanner
	JobManager
	SQLGenerator
	Close() error
}

// importSDK implements SDK interface
type importSDK struct {
	FileScanner
	JobManager
	SQLGenerator
}

// NewImportSDK creates a new CloudImportSDK instance
func NewImportSDK(ctx context.Context, sourcePath string, db *sql.DB, options ...SDKOption) (SDK, error) {
	cfg := defaultSDKConfig()
	for _, opt := range options {
		opt(cfg)
	}

	scanner, err := NewFileScanner(ctx, sourcePath, db, cfg)
	if err != nil {
		return nil, err
	}

	jobManager := NewJobManager(db)
	sqlGenerator := NewSQLGenerator()

	return &importSDK{
		FileScanner:  scanner,
		JobManager:   jobManager,
		SQLGenerator: sqlGenerator,
	}, nil
}

// Close releases resources used by the SDK
func (sdk *importSDK) Close() error {
	return sdk.FileScanner.Close()
}
