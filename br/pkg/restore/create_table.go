package restore

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/pipeline"
	"github.com/pingcap/tidb/domain"
	"go.uber.org/zap"
)

func NewCreateTablesPipe(rc *Client, dom *domain.Domain, tables []*metautil.Table, newTS uint64) pipeline.Traceable[struct{}, CreatedTable] {
	return createTablesPipe{
		rc:     rc,
		dom:    rc.GetDomain(),
		tables: tables,
		newTS:  newTS,
	}
}

type createTablesPipe struct {
	rc     *Client
	dom    *domain.Domain
	tables []*metautil.Table
	newTS  uint64
}

func (c createTablesPipe) Name() string {
	return "create table"
}

func (c createTablesPipe) Size() int {
	return len(c.tables)
}

func (c createTablesPipe) MainLoop(ctx pipeline.Context[CreatedTable], input <-chan struct{}) {
	defer ctx.Finish()
	// Could we have a smaller size of tables?
	log.Info("start create tables")

	rc := c.rc
	rc.GenerateRebasedTables(c.tables)
	rater := logutil.TraceRateOver(logutil.MetricTableCreatedCounter)
	if err := rc.allocTableIDs(ctx, c.tables); err != nil {
		ctx.EmitErr(err)
		return
	}

	var err error

	if rc.batchDdlSize > minBatchDdlSize && len(rc.dbPool) > 0 {
		err = rc.createTablesInWorkerPool(ctx, c.dom, c.tables, c.newTS, ctx)

		if err == nil {
			defer log.Debug("all tables are created")
			return
		} else if utils.FallBack2CreateTable(err) {
			// fall back to old create table (sequential create table)
			log.Info("fall back to the sequential create table")
		} else {
			ctx.EmitErr(err)
			return
		}
	}

	createOneTable := func(cx context.Context, db *DB, t *metautil.Table) error {
		select {
		case <-cx.Done():
			return cx.Err()
		default:
		}
		rt, err := rc.createTable(cx, db, c.dom, t, c.newTS)
		if err != nil {
			log.Error("create table failed",
				zap.Error(err),
				zap.Stringer("db", t.DB.Name),
				zap.Stringer("table", t.Info.Name))
			return errors.Trace(err)
		}
		log.Debug("table created and send to next",
			zap.Stringer("table", t.Info.Name),
			zap.Stringer("database", t.DB.Name))
		ctx.Emit(rt)
		rater.Inc()
		rater.L().Info("table created",
			zap.Stringer("table", t.Info.Name),
			zap.Stringer("database", t.DB.Name))
		return nil
	}
	if len(rc.dbPool) > 0 {
		err = rc.createTablesWithDBPool(ctx, createOneTable, c.tables)
	} else {
		err = rc.createTablesWithSoleDB(ctx, createOneTable, c.tables)
	}
	if err != nil {
		ctx.EmitErr(err)
	}
}
