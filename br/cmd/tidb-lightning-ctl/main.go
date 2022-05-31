// Copyright 2019 PingCAP, Inc.
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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/restore"
	"github.com/pingcap/tidb/br/pkg/lightning/tikv"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, errors.ErrorStack(err))
		exit(1)
	}
}

// main_test.go override exit to pass unit test.
var exit = os.Exit

func run() error {
	var (
		compact, flagFetchMode                      *bool
		mode                                        *string
		cpRemove, cpErrIgnore, cpErrDestroy, cpDump *string
		localStoringTables                          *bool

		fsUsage func()
	)

	globalCfg := config.Must(config.LoadGlobalConfig(os.Args[1:], func(fs *flag.FlagSet) {
		// change the default of `-d` from empty to 'noop://'.
		// there is a check if `-d` points to a valid storage, and '' is not.
		// since tidb-lightning-ctl does not need `-d` we change the default to a valid but harmless value.
		dFlag := fs.Lookup("d")
		_ = dFlag.Value.Set("noop://")
		dFlag.DefValue = "noop://"

		compact = fs.Bool("compact", false, "do manual compaction on the target cluster")
		mode = fs.String("switch-mode", "", "switch tikv into import mode or normal mode, values can be ['import', 'normal']")
		flagFetchMode = fs.Bool("fetch-mode", false, "obtain the current mode of every tikv in the cluster")

		cpRemove = fs.String("checkpoint-remove", "", "remove the checkpoint associated with the given table (value can be 'all' or '`db`.`table`')")
		cpErrIgnore = fs.String("checkpoint-error-ignore", "", "ignore errors encoutered previously on the given table (value can be 'all' or '`db`.`table`'); may corrupt this table if used incorrectly")
		cpErrDestroy = fs.String("checkpoint-error-destroy", "", "deletes imported data with table which has an error before (value can be 'all' or '`db`.`table`')")
		cpDump = fs.String("checkpoint-dump", "", "dump the checkpoint information as two CSV files in the given folder")

		localStoringTables = fs.Bool("check-local-storage", false, "show tables that are missing local intermediate files (value can be 'all' or '`db`.`table`')")

		fsUsage = fs.Usage
	}))

	ctx := context.Background()

	cfg := config.NewConfig()
	if err := cfg.LoadFromGlobal(globalCfg); err != nil {
		return err
	}
	if err := cfg.Adjust(ctx); err != nil {
		return err
	}

	tls, err := cfg.ToTLS()
	if err != nil {
		return err
	}
	if err = cfg.TiDB.Security.RegisterMySQL(); err != nil {
		return err
	}

	if *compact {
		return errors.Trace(compactCluster(ctx, cfg, tls))
	}
	if *flagFetchMode {
		return errors.Trace(fetchMode(ctx, cfg, tls))
	}
	if len(*mode) != 0 {
		return errors.Trace(lightning.SwitchMode(ctx, cfg, tls, *mode))
	}

	if len(*cpRemove) != 0 {
		return errors.Trace(lightning.CheckpointRemove(ctx, cfg, *cpRemove))
	}
	if len(*cpErrIgnore) != 0 {
		return errors.Trace(checkpointErrorIgnore(ctx, cfg, *cpErrIgnore))
	}
	if len(*cpErrDestroy) != 0 {
		return errors.Trace(checkpointErrorDestroy(ctx, cfg, tls, *cpErrDestroy))
	}
	if len(*cpDump) != 0 {
		return errors.Trace(checkpointDump(ctx, cfg, *cpDump))
	}
	if *localStoringTables {
		return errors.Trace(getLocalStoringTables(ctx, cfg))
	}

	fsUsage()
	return nil
}

func compactCluster(ctx context.Context, cfg *config.Config, tls *common.TLS) error {
	return tikv.ForAllStores(
		ctx,
		tls.WithHost(cfg.TiDB.PdAddr),
		tikv.StoreStateDisconnected,
		func(c context.Context, store *tikv.Store) error {
			return tikv.Compact(c, tls, store.Address, restore.FullLevelCompact)
		},
	)
}

func fetchMode(ctx context.Context, cfg *config.Config, tls *common.TLS) error {
	return tikv.ForAllStores(
		ctx,
		tls.WithHost(cfg.TiDB.PdAddr),
		tikv.StoreStateDisconnected,
		func(c context.Context, store *tikv.Store) error {
			mode, err := tikv.FetchMode(c, tls, store.Address)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%-30s | Error: %v\n", store.Address, err)
			} else {
				fmt.Fprintf(os.Stderr, "%-30s | %s mode\n", store.Address, mode)
			}
			return nil
		},
	)
}

func checkpointErrorIgnore(ctx context.Context, cfg *config.Config, tableName string) error {
	cpdb, err := checkpoints.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer cpdb.Close()

	return errors.Trace(cpdb.IgnoreErrorCheckpoint(ctx, tableName))
}

func checkpointErrorDestroy(ctx context.Context, cfg *config.Config, tls *common.TLS, tableName string) error {
	cpdb, err := checkpoints.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer cpdb.Close()

	target, err := restore.NewTiDBManager(ctx, cfg.TiDB, tls)
	if err != nil {
		return errors.Trace(err)
	}
	defer target.Close()

	targetTables, err := cpdb.DestroyErrorCheckpoint(ctx, tableName)
	if err != nil {
		return errors.Trace(err)
	}

	var lastErr error

	for _, table := range targetTables {
		fmt.Fprintln(os.Stderr, "Dropping table:", table.TableName)
		err := target.DropTable(ctx, table.TableName)
		if err != nil {
			fmt.Fprintln(os.Stderr, "* Encountered error while dropping table:", err)
			lastErr = err
		}
	}

	// For importer backend, engine was stored in importer's memory, we can retrieve it from alive importer process.
	// But in local backend, if we want to use common API `UnsafeCloseEngine` and `Cleanup`,
	// we need either lightning process alive or engine map persistent.
	// both of them seems unnecessary if we only need to do is cleanup specify engine directory.
	// so we didn't choose to use common API.
	if cfg.TikvImporter.Backend == "local" {
		for _, table := range targetTables {
			for engineID := table.MinEngineID; engineID <= table.MaxEngineID; engineID++ {
				fmt.Fprintln(os.Stderr, "Closing and cleaning up engine:", table.TableName, engineID)
				_, eID := backend.MakeUUID(table.TableName, engineID)
				engine := local.Engine{UUID: eID}
				err := engine.Cleanup(cfg.TikvImporter.SortedKVDir)
				if err != nil {
					fmt.Fprintln(os.Stderr, "* Encountered error while cleanup engine:", err)
					lastErr = err
				}
			}
		}
	}

	// try clean up metas
	if lastErr == nil {
		lastErr = lightning.CleanupMetas(ctx, cfg, tableName)
	}

	return errors.Trace(lastErr)
}

func checkpointDump(ctx context.Context, cfg *config.Config, dumpFolder string) error {
	cpdb, err := checkpoints.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer cpdb.Close()

	if err := os.MkdirAll(dumpFolder, 0o750); err != nil {
		return errors.Trace(err)
	}

	tablesFileName := filepath.Join(dumpFolder, "tables.csv")
	tablesFile, err := os.Create(tablesFileName)
	if err != nil {
		return errors.Annotatef(err, "failed to create %s", tablesFileName)
	}
	defer tablesFile.Close()

	enginesFileName := filepath.Join(dumpFolder, "engines.csv")
	enginesFile, err := os.Create(tablesFileName)
	if err != nil {
		return errors.Annotatef(err, "failed to create %s", enginesFileName)
	}
	defer enginesFile.Close()

	chunksFileName := filepath.Join(dumpFolder, "chunks.csv")
	chunksFile, err := os.Create(chunksFileName)
	if err != nil {
		return errors.Annotatef(err, "failed to create %s", chunksFileName)
	}
	defer chunksFile.Close()

	if err := cpdb.DumpTables(ctx, tablesFile); err != nil {
		return errors.Trace(err)
	}
	if err := cpdb.DumpEngines(ctx, enginesFile); err != nil {
		return errors.Trace(err)
	}
	if err := cpdb.DumpChunks(ctx, chunksFile); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func getLocalStoringTables(ctx context.Context, cfg *config.Config) (err2 error) {
	//nolint:prealloc // This is a placeholder.
	var tables []string
	defer func() {
		if err2 == nil {
			if len(tables) == 0 {
				fmt.Fprintln(os.Stderr, "No table has lost intermediate files according to given config")
			} else {
				fmt.Fprintln(os.Stderr, "These tables are missing intermediate files:", tables)
			}
		}
	}()

	if cfg.TikvImporter.Backend != config.BackendLocal {
		return nil
	}
	exist, err := checkpoints.IsCheckpointsDBExists(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return nil
	}
	cpdb, err := checkpoints.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer cpdb.Close()

	tableWithEngine, err := cpdb.GetLocalStoringTables(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	tables = make([]string, 0, len(tableWithEngine))
	for tableName := range tableWithEngine {
		tables = append(tables, tableName)
	}

	return nil
}
