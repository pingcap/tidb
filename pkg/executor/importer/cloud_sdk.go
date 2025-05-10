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

package importer


import (
	"context"
	"database/sql"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"golang.org/x/sync/errgroup"
)

type CreateSchemasOption func(cfg *createSchemasConfig)

type createSchemasConfig struct {
	concurrency int
}

func defaultCreateSchemasConfig() *createSchemasConfig {
	return &createSchemasConfig{
		concurrency: 4,
	}
}

// WithConcurrency sets the number of concurrent DB/schema workers.
func WithConcurrency(n int) CreateSchemasOption {
	return func(cfg *createSchemasConfig) {
		if n > 0 {
			cfg.concurrency = n
		}
	}
}

// CreateSchemas scans the given sourcePath, discovers schemas, and applies them.
// Options can adjust parallelism and other behaviors.
func CreateSchemas(
	ctx context.Context,
	sourcePath string,
	db *sql.DB,
	opts ...CreateSchemasOption,
) error {
	// apply options
	cfg := defaultCreateSchemasConfig()
	for _, o := range opts {
		o(cfg)
	}

	u, err := storage.ParseBackend(sourcePath, nil)
	if err != nil {
		return errors.Trace(err)
	}
	store, err := storage.New(ctx, u, &storage.ExternalStorageOptions{})
	if err != nil {
		return errors.Trace(err)
	}

	ldrCfg := mydump.LoaderConfig{
		SourceURL:        sourcePath,
		CharacterSet:     "",
		Filter:           []string{"*.*"}, // match all schemas and tables
		FileRouters:      nil,
		CaseSensitive:    false,
		DefaultFileRules: true,
	}
	loader, err := mydump.NewLoaderWithStore(ctx, ldrCfg, store)
	if err != nil {
		return errors.Trace(err)
	}
	dbMetas := loader.GetDatabases()

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(cfg.concurrency)
	retryer := common.SQLWithRetry{DB: db, Logger: log.FromContext(ctx)}

	for _, dbMeta := range dbMetas {
		dm := dbMeta
		eg.Go(func() error {
			// create database
			createDBSQL := dm.GetSchema(egCtx, store)
			if err := retryer.Exec(egCtx, "create database", createDBSQL); err != nil {
				return errors.Wrapf(err, "create database %s failed", dm.Name)
			}
			// create tables
			for _, tbl := range dm.Tables {
				sqlTbl, err := tbl.GetSchema(egCtx, store)
				if err != nil {
					return errors.Wrapf(err, "read schema for table %s.%s failed", dm.Name, tbl.Name)
				}
				if err := retryer.Exec(egCtx, "create table", sqlTbl); err != nil {
					return errors.Wrapf(err, "create table %s.%s failed", dm.Name, tbl.Name)
				}
			}
			// create views
			for _, vw := range dm.Views {
				sqlVW, err := vw.GetSchema(egCtx, store)
				if err != nil {
					return errors.Wrapf(err, "read schema for view %s.%s failed", dm.Name, vw.Name)
				}
				if err := retryer.Exec(egCtx, "create view", sqlVW); err != nil {
					return errors.Wrapf(err, "create view %s.%s failed", dm.Name, vw.Name)
				}
			}
			return nil
		})
	}

	return eg.Wait()
}
