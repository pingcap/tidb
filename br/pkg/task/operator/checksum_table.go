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
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
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

func RunUpstreamChecksumTable(ctx context.Context, g glue.Glue, cfg ChecksumUpstreamConfig) error {
	c := &checksumTableCtx{cfg: ChecksumWithRewriteRulesConfig{Config: cfg.Config}}
	if err := c.init(ctx, g); err != nil {
		return errors.Trace(err)
	}
	curr, err := c.getTables(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	reqs, err := c.genUpstreamRequests(curr, cfg.RestoreTS)
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

func (c *checksumTableCtx) loadPitrIdMap(ctx context.Context, g glue.Glue, restoredTS uint64, clusterID uint64) ([]*backup.PitrDBMap, error) {
	if len(c.cfg.Storage) > 0 {
		_, stg, err := task.GetStorage(ctx, c.cfg.Storage, &c.cfg.Config)
		if err != nil {
			return nil, errors.Annotate(err, "failed to create storage")
		}
		metaFileName := logclient.PitrIDMapsFilename(clusterID, restoredTS)
		log.Info("get pitr id map from the external storage", zap.String("file name", metaFileName))
		metaData, err := stg.ReadFile(ctx, metaFileName)
		if err != nil {
			return nil, errors.Annotate(err, "failed to load pitr id map file")
		}
		backupMeta := &backup.BackupMeta{}
		if err := backupMeta.Unmarshal(metaData); err != nil {
			return nil, errors.Annotate(err, "failed to unmarshal pitr id map file")
		}
		return backupMeta.GetDbMaps(), nil
	}
	log.Info("get pitr id map from table", zap.Uint64("restored-ts", restoredTS))
	table, err := c.dom.InfoSchema().TableByName(ctx, ast.NewCIStr("mysql"), ast.NewCIStr("tidb_pitr_id_map"))
	if err != nil {
		return nil, errors.Annotate(err, "failed to get the table")
	}
	hasRestoreIDColumn := false
	for _, col := range table.Meta().Columns {
		if col.Name.L == "restore_id" {
			hasRestoreIDColumn = true
			break
		}
	}
	var getPitrIDMapSQL string
	var getRowColumns func(row chunk.Row) (uint64, uint64, []byte)
	if hasRestoreIDColumn {
		// new version with restore_id column
		getPitrIDMapSQL = "SELECT restore_id, segment_id, id_map FROM mysql.tidb_pitr_id_map WHERE restored_ts = %? and upstream_cluster_id = %? ORDER BY restore_id, segment_id;"
		getRowColumns = func(row chunk.Row) (uint64, uint64, []byte) {
			return row.GetUint64(0), row.GetUint64(1), row.GetBytes(2)
		}
	} else {
		// old version without restore_id column
		log.Info("mysql.tidb_pitr_id_map table does not have restore_id column, using backward compatible mode")
		getPitrIDMapSQL = "SELECT segment_id, id_map FROM mysql.tidb_pitr_id_map WHERE restored_ts = %? and upstream_cluster_id = %? ORDER BY segment_id;"
		getRowColumns = func(row chunk.Row) (uint64, uint64, []byte) {
			return 0, row.GetUint64(0), row.GetBytes(1)
		}
	}
	se, err := g.CreateSession(c.mgr.GetStorage())
	if err != nil {
		return nil, errors.Annotate(err, "failed to create session")
	}
	defer se.Close()
	execCtx := se.GetSessionCtx().GetRestrictedSQLExecutor()
	rows, _, errSQL := execCtx.ExecRestrictedSQL(
		kv.WithInternalSourceType(ctx, kv.InternalTxnBR),
		nil,
		getPitrIDMapSQL,
		restoredTS, clusterID,
	)
	if errSQL != nil {
		return nil, errors.Annotate(err, "failed to get pitr id map from mysql.tidb_pitr_id_map")
	}

	pitrDBMap := make([]*backup.PitrDBMap, 0)
	metaData := make([]byte, 0)
	lastRestoreID := uint64(0)
	nextSegmentID := uint64(0)
	for _, row := range rows {
		restoreID, elementID, data := getRowColumns(row)
		if lastRestoreID != restoreID {
			backupMeta := &backup.BackupMeta{}
			if err := backupMeta.Unmarshal(metaData); err != nil {
				return nil, errors.Trace(err)
			}
			pitrDBMap = append(pitrDBMap, backupMeta.DbMaps...)
			metaData = make([]byte, 0)
			lastRestoreID = restoreID
			nextSegmentID = uint64(0)
		}
		if nextSegmentID != elementID {
			return nil, errors.Errorf("the part(segment_id = %d) of pitr id map is lost", nextSegmentID)
		}
		if len(data) == 0 {
			return nil, errors.Errorf("get the empty part(segment_id = %d) of pitr id map", nextSegmentID)
		}
		metaData = append(metaData, data...)
		nextSegmentID += 1
	}
	if len(metaData) > 0 {
		backupMeta := &backup.BackupMeta{}
		if err := backupMeta.Unmarshal(metaData); err != nil {
			return nil, errors.Trace(err)
		}
		pitrDBMap = append(pitrDBMap, backupMeta.DbMaps...)
	}
	return pitrDBMap, nil
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

func (c *checksumTableCtx) genRequestsWithIDMap(curr []tableInDB, idmaps []*backup.PitrDBMap, restoreTS uint64) (reqs []request, err error) {
	router := make(map[string]map[string]map[int64]int64)
	for _, dbidmap := range idmaps {
		tableRouter, exists := router[dbidmap.Name]
		if !exists {
			tableRouter = make(map[string]map[int64]int64)
			router[dbidmap.Name] = tableRouter
		}
		for _, tableidmap := range dbidmap.Tables {
			down2upmap, exists := tableRouter[tableidmap.Name]
			if !exists {
				down2upmap = make(map[int64]int64)
				tableRouter[tableidmap.Name] = down2upmap
			}
			down2upmap[tableidmap.IdMap.DownstreamId] = tableidmap.IdMap.UpstreamId
			for _, phyidmap := range tableidmap.Partitions {
				down2upmap[phyidmap.DownstreamId] = phyidmap.UpstreamId
			}
		}
	}

	for _, t := range curr {
		rb := checksum.NewExecutorBuilder(t.info, restoreTS)
		rb.SetConcurrency(c.cfg.ChecksumConcurrency)
		fakeOldTable := t.info.Clone()
		tableRouter, exists := router[t.dbName]
		if !exists {
			return nil, errors.Errorf("no db map found by db name: %s", t.dbName)
		}
		idRouter, exists := tableRouter[t.info.Name.O]
		if !exists {
			return nil, errors.Errorf("no table map found by table name: %s", t.info.Name.O)
		}
		upstreamID, exists := idRouter[t.info.ID]
		if !exists {
			return nil, errors.Errorf("no id map found by id: %d", t.info.ID)
		}
		if t.info.Partition != nil {
			for i, part := range t.info.Partition.Definitions {
				upstreamPartID, exists := idRouter[part.ID]
				if !exists {
					return nil, errors.Errorf("no part id map found by id: %d", part.ID)
				}
				fakeOldTable.Partition.Definitions[i].ID = upstreamPartID
			}
		}
		fakeOldTable.ID = upstreamID
		rb.SetOldTable(&metautil.Table{Info: fakeOldTable})
		rb.SetExplicitRequestSourceType(kvutil.ExplicitTypeBR)
		req, err := rb.Build()
		if err != nil {
			return nil, errors.Annotatef(err, "failed to build checksum executor for table %s.%s", t.dbName, t.info.Name.O)
		}
		reqs = append(reqs, request{copReq: req, dbName: t.dbName, tableName: t.info.Name.L})
	}
	return
}

func (c *checksumTableCtx) genUpstreamRequests(curr []tableInDB, checksumTS uint64) (reqs []request, err error) {
	for _, t := range curr {
		rb := checksum.NewExecutorBuilder(t.info, checksumTS)
		rb.SetConcurrency(c.cfg.ChecksumConcurrency)
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

func RunPitrChecksumTable(ctx context.Context, g glue.Glue, cfg ChecksumWithPitrIdMapConfig) error {
	c := &checksumTableCtx{cfg: ChecksumWithRewriteRulesConfig{Config: cfg.Config}}
	if err := c.init(ctx, g); err != nil {
		return errors.Trace(err)
	}
	curr, err := c.getTables(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	pitrIdMap, err := c.loadPitrIdMap(ctx, g, cfg.RestoreTS, cfg.UpstreamClusterID)
	if err != nil {
		return errors.Trace(err)
	}
	reqs, err := c.genRequestsWithIDMap(curr, pitrIdMap, cfg.RestoreTS)
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
