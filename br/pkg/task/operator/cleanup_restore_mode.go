// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package operator

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"google.golang.org/grpc/keepalive"
)

// CleanupRestoreModeConfig is the configuration for cleanup restore mode.
type CleanupRestoreModeConfig struct {
	task.Config
}

// ParseFromFlags parses the config from command line flags.
func (cfg *CleanupRestoreModeConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	return cfg.Config.ParseFromFlags(flags)
}

// RunCleanupRestoreMode runs the cleanup restore mode command.
func RunCleanupRestoreMode(ctx context.Context, cfg *CleanupRestoreModeConfig) error {
	if !cfg.ExplicitFilter {
		return errors.New("must specify at least one filter rule to cleanup restore mode, " +
			"full cluster cleanup is not allowed")
	}

	g := gluetidb.New()
	mgr, err := task.NewMgr(ctx, g, cfg.PD, cfg.TLS, keepalive.ClientParameters{
		Time:                cfg.GRPCKeepaliveTime,
		Timeout:             cfg.GRPCKeepaliveTimeout,
		PermitWithoutStream: false,
	}, cfg.CheckRequirements, true, conn.NormalVersionChecker)
	if err != nil {
		return errors.Trace(err)
	}
	defer mgr.Close()

	store := mgr.GetStorage()
	dom, err := g.GetDomain(store)
	if err != nil {
		return errors.Trace(err)
	}

	se, err := g.CreateSession(store)
	if err != nil {
		return errors.Trace(err)
	}
	defer se.Close()

	info := dom.InfoSchema()
	schemas := info.AllSchemas()
	for _, schema := range schemas {
		if !cfg.TableFilter.MatchSchema(schema.Name.L) {
			continue
		}

		tables, err := info.SchemaTableInfos(ctx, schema.Name)
		if err != nil {
			return errors.Trace(err)
		}
		for _, table := range tables {
			if !cfg.TableFilter.MatchTable(schema.Name.L, table.Name.L) {
				continue
			}

			if table.Mode == model.TableModeRestore {
				log.Info("found table in restore mode",
					zap.String("schema", schema.Name.O),
					zap.String("table", table.Name.O))
				err = se.AlterTableMode(ctx, schema.ID, table.ID, model.TableModeNormal)
				if err != nil {
					return errors.Trace(err)
				}
				log.Info("set table mode to normal",
					zap.String("schema", schema.Name.O),
					zap.String("table", table.Name.O))
			}
		}
	}

	return nil
}
