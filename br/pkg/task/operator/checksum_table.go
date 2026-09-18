package operator

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checksum"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type checksumTableCtx struct {
	cfg ChecksumWithRewriteRulesConfig

	mgr *conn.Mgr
	dom *domain.Domain
}

type tableInDB struct {
	info   *model.TableInfo
	dbName string
}

func RunChecksumTable(ctx context.Context, g glue.Glue, cfg ChecksumWithRewriteRulesConfig) error {
	c := &checksumTableCtx{cfg: cfg}

	if err := c.init(ctx, g); err != nil {
		return errors.Trace(err)
	}

	curr, err := c.getTables(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	old, err := c.loadOldTableIDs(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	reqs, err := c.genRequests(ctx, old, curr)
	if err != nil {
		return errors.Trace(err)
	}

	results, err := c.runChecksum(ctx, reqs)
	if err != nil {
		return errors.Trace(err)
	}

	for _, result := range results {
		log.Info("Checksum result", zap.String("db", result.DBName), zap.String("table", result.TableName), zap.Uint64("checksum", result.Checksum),
			zap.Uint64("total_bytes", result.TotalBytes), zap.Uint64("total_kvs", result.TotalKVs))
	}

	return json.NewEncoder(os.Stdout).Encode(results)
}

func (c *checksumTableCtx) init(ctx context.Context, g glue.Glue) error {
	cfg := c.cfg
	var err error
	c.mgr, err = task.NewMgr(ctx, g, cfg.PD, cfg.TLS, task.GetKeepalive(&cfg.Config), cfg.CheckRequirements, true, conn.NormalVersionChecker)
	if err != nil {
		return err
	}

	c.dom, err = g.GetDomain(c.mgr.GetStorage())
	if err != nil {
		return err
	}
	return nil
}

func (c *checksumTableCtx) getTables(ctx context.Context) (res []tableInDB, err error) {
	sch := c.dom.InfoSchema()
	dbs := sch.AllSchemas()
	for _, db := range dbs {
		if !c.cfg.TableFilter.MatchSchema(db.Name.L) {
			continue
		}

		tbls, err := sch.SchemaTableInfos(ctx, db.Name)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to load data for db %s", db.Name)
		}
		for _, tbl := range tbls {
			if !c.cfg.TableFilter.MatchTable(db.Name.L, tbl.Name.L) {
				continue
			}
			log.Info("Added table from cluster.", zap.String("db", db.Name.L), zap.String("table", tbl.Name.L))
			res = append(res, tableInDB{
				info:   tbl,
				dbName: db.Name.L,
			})
		}
	}

	return
}

func (c *checksumTableCtx) loadOldTableIDs(ctx context.Context) (res []*metautil.Table, err error) {
	_, strg, err := task.GetStorage(ctx, c.cfg.Storage, &c.cfg.Config)
	if err != nil {
		return nil, errors.Annotate(err, "failed to create storage")
	}

	mPath := metautil.MetaFile
	metaContent, err := strg.ReadFile(ctx, mPath)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to open metafile %s", mPath)
	}

	var backupMeta backup.BackupMeta
	if err := backupMeta.Unmarshal(metaContent); err != nil {
		return nil, errors.Annotate(err, "failed to parse backupmeta")
	}

	metaReader := metautil.NewMetaReader(&backupMeta, strg, &c.cfg.CipherInfo)

	tblCh := make(chan *metautil.Table, 1024)
	errCh := make(chan error, 1)
	go func() {
		if err := metaReader.ReadSchemasFiles(ctx, tblCh, metautil.SkipFiles, metautil.SkipStats); err != nil {
			errCh <- errors.Annotate(err, "failed to read schema files")
		}
		close(tblCh)
	}()

	for {
		select {
		case err := <-errCh:
			return nil, err
		case tbl, ok := <-tblCh:
			if !ok {
				return
			}
			if !c.cfg.TableFilter.MatchTable(tbl.DB.Name.L, tbl.Info.Name.L) {
				continue
			}
			log.Info("Added table from backup data.", zap.String("db", tbl.DB.Name.L), zap.String("table", tbl.Info.Name.L))
			res = append(res, tbl)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

type request struct {
	copReq    *checksum.Executor
	tableName string
	dbName    string
}

func (c *checksumTableCtx) genRequests(ctx context.Context, bkup []*metautil.Table, curr []tableInDB) (reqs []request, err error) {
	phy, logi, err := c.mgr.GetPDClient().GetTS(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "failed to get TSO for checksumming")
	}
	tso := oracle.ComposeTS(phy, logi)

	bkupTbls := map[string]map[string]*metautil.Table{}
	for _, t := range bkup {
		m, ok := bkupTbls[t.DB.Name.L]
		if !ok {
			m = make(map[string]*metautil.Table)
			bkupTbls[t.DB.Name.L] = m
		}

		m[t.Info.Name.L] = t
	}

	for _, t := range curr {
		rb := checksum.NewExecutorBuilder(t.info, tso)
		rb.SetConcurrency(c.cfg.ChecksumConcurrency)
		oldDB, ok := bkupTbls[t.dbName]
		if !ok {
			log.Warn("db not found, will skip", zap.String("db", t.dbName))
			continue
		}
		oldTable, ok := oldDB[t.info.Name.L]
		if !ok {
			log.Warn("table not found, will skip", zap.String("db", t.dbName), zap.String("table", t.info.Name.L))
			continue
		}

		rb.SetOldTable(oldTable)
		rb.SetExplicitRequestSourceType(kvutil.ExplicitTypeBR)
		req, err := rb.Build()
		if err != nil {
			return nil, errors.Annotatef(err, "failed to build checksum builder for table %s.%s", t.dbName, t.info.Name.L)
		}
		reqs = append(reqs, request{
			copReq:    req,
			dbName:    t.dbName,
			tableName: t.info.Name.L,
		})
	}

	return
}

type ChecksumResult struct {
	DBName    string `json:"db_name"`
	TableName string `json:"table_name"`

	Checksum   uint64 `json:"checksum"`
	TotalBytes uint64 `json:"total_bytes"`
	TotalKVs   uint64 `json:"total_kvs"`
}

func (c *checksumTableCtx) runChecksum(ctx context.Context, reqs []request) ([]ChecksumResult, error) {
	wkPool := util.NewWorkerPool(c.cfg.TableConcurrency, "checksum")
	eg, ectx := errgroup.WithContext(ctx)
	results := make([]ChecksumResult, 0, len(reqs))
	resultsMu := new(sync.Mutex)

	for _, req := range reqs {
		wkPool.ApplyOnErrorGroup(eg, func() error {
			total := req.copReq.Len()
			finished := new(atomic.Int64)
			resp, err := req.copReq.Execute(ectx, c.mgr.GetStorage().GetClient(), func() {
				finished.Add(1)
				log.Info(
					"Finish one request of a table.",
					zap.String("db", req.dbName),
					zap.String("table", req.tableName),
					zap.Int64("finished", finished.Load()),
					zap.Int64("total", int64(total)),
				)
			})
			if err != nil {
				return err
			}
			res := ChecksumResult{
				DBName:    req.dbName,
				TableName: req.tableName,

				Checksum:   resp.Checksum,
				TotalBytes: resp.TotalBytes,
				TotalKVs:   resp.TotalKvs,
			}
			resultsMu.Lock()
			results = append(results, res)
			resultsMu.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return results, nil
}
