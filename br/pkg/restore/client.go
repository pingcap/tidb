// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checksum"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/pdtypes"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// defaultChecksumConcurrency is the default number of the concurrent
// checksum tasks.
const defaultChecksumConcurrency = 64
const defaultDDLConcurrency = 16
const minBatchDdlSize = 1

const (
	strictPlacementPolicyMode = "STRICT"
	ignorePlacementPolicyMode = "IGNORE"
)

// Client sends requests to restore files.
type Client struct {
	pdClient      pd.Client
	toolClient    SplitClient
	fileImporter  FileImporter
	workerPool    *utils.WorkerPool
	tlsConf       *tls.Config
	keepaliveConf keepalive.ClientParameters

	databases map[string]*utils.Database
	ddlJobs   []*model.Job
	// ddlJobsMap record the tables' auto id need to restore after create table
	ddlJobsMap map[UniqueTableName]bool
	backupMeta *backuppb.BackupMeta
	// TODO Remove this field or replace it with a []*DB,
	// since https://github.com/pingcap/br/pull/377 needs more DBs to speed up DDL execution.
	// And for now, we must inject a pool of DBs to `Client.GoCreateTables`, otherwise there would be a race condition.
	// This is dirty: why we need DBs from different sources?
	// By replace it with a []*DB, we can remove the dirty parameter of `Client.GoCreateTable`,
	// along with them in some private functions.
	// Before you do it, you can firstly read discussions at
	// https://github.com/pingcap/br/pull/377#discussion_r446594501,
	// this probably isn't as easy as it seems like (however, not hard, too :D)
	db *DB

	// use db pool to speed up restoration in BR binary mode.
	dbPool          []*DB
	rateLimit       uint64
	isOnline        bool
	noSchema        bool
	hasSpeedLimited bool

	restoreStores []uint64

	cipher             *backuppb.CipherInfo
	switchModeInterval time.Duration
	switchCh           chan struct{}

	// statHandler and dom are used for analyze table after restore.
	// it will backup stats with #dump.DumpStatsToJSON
	// and restore stats with #dump.LoadStatsFromJSON
	statsHandler *handle.Handle
	dom          *domain.Domain

	batchDdlSize uint

	// correspond to --tidb-placement-mode config.
	// STRICT(default) means policy related SQL can be executed in tidb.
	// IGNORE means policy related SQL will be ignored.
	policyMode string

	// policy name -> policy info
	policyMap *sync.Map

	supportPolicy bool
}

// NewRestoreClient returns a new RestoreClient.
func NewRestoreClient(
	pdClient pd.Client,
	tlsConf *tls.Config,
	keepaliveConf keepalive.ClientParameters,
	isRawKv bool,
) *Client {
	return &Client{
		pdClient:      pdClient,
		toolClient:    NewSplitClient(pdClient, tlsConf, isRawKv),
		tlsConf:       tlsConf,
		keepaliveConf: keepaliveConf,
		switchCh:      make(chan struct{}),
	}
}

// Init create db connection and domain for storage.
func (rc *Client) Init(g glue.Glue, store kv.Storage) error {
	// setDB must happen after set PolicyMode.
	// we will use policyMode to set session variables.
	var err error
	rc.db, rc.supportPolicy, err = NewDB(g, store, rc.policyMode)
	if err != nil {
		return errors.Trace(err)
	}
	rc.dom, err = g.GetDomain(store)
	if err != nil {
		return errors.Trace(err)
	}
	// tikv.Glue will return nil, tidb.Glue will return available domain
	if rc.dom != nil {
		rc.statsHandler = rc.dom.StatsHandle()
	}

	// Only in binary we can use multi-thread sessions to create tables.
	// so use OwnStorage() to tell whether we are use binary or SQL.
	if g.OwnsStorage() {
		// Maybe allow user modify the DDL concurrency isn't necessary,
		// because executing DDL is really I/O bound (or, algorithm bound?),
		// and we cost most of time at waiting DDL jobs be enqueued.
		// So these jobs won't be faster or slower when machine become faster or slower,
		// hence make it a fixed value would be fine.
		rc.dbPool, err = makeDBPool(defaultDDLConcurrency, func() (*DB, error) {
			db, _, err := NewDB(g, store, rc.policyMode)
			return db, err
		})
		if err != nil {
			log.Warn("create session pool failed, we will send DDLs only by created sessions",
				zap.Error(err),
				zap.Int("sessionCount", len(rc.dbPool)),
			)
		}
	}
	return errors.Trace(err)
}

// SetPlacementPolicyMode to policy mode.
func (rc *Client) SetPlacementPolicyMode(withPlacementPolicy string) {
	switch strings.ToUpper(withPlacementPolicy) {
	case strictPlacementPolicyMode:
		rc.policyMode = strictPlacementPolicyMode
	case ignorePlacementPolicyMode:
		rc.policyMode = ignorePlacementPolicyMode
	default:
		rc.policyMode = strictPlacementPolicyMode
	}
	log.Info("set placement policy mode", zap.String("mode", rc.policyMode))
}

// SetRateLimit to set rateLimit.
func (rc *Client) SetRateLimit(rateLimit uint64) {
	rc.rateLimit = rateLimit
}

func (rc *Client) SetCrypter(crypter *backuppb.CipherInfo) {
	rc.cipher = crypter
}

// SetPolicyMap set policyMap.
func (rc *Client) SetPolicyMap(p *sync.Map) {
	rc.policyMap = p
}

// GetPolicyMap set policyMap.
func (rc *Client) GetPolicyMap() *sync.Map {
	return rc.policyMap
}

// GetSupportPolicy tells whether target tidb support placement policy.
func (rc *Client) GetSupportPolicy() bool {
	return rc.supportPolicy
}

// GetPDClient returns a pd client.
func (rc *Client) GetPDClient() pd.Client {
	return rc.pdClient
}

// IsOnline tells if it's a online restore.
func (rc *Client) IsOnline() bool {
	return rc.isOnline
}

// SetSwitchModeInterval set switch mode interval for client.
func (rc *Client) SetSwitchModeInterval(interval time.Duration) {
	rc.switchModeInterval = interval
}

func (rc *Client) SetBatchDdlSize(batchDdlsize uint) {
	rc.batchDdlSize = batchDdlsize
}

func (rc *Client) GetBatchDdlSize() uint {
	return rc.batchDdlSize
}

// Close a client.
func (rc *Client) Close() {
	// rc.db can be nil in raw kv mode.
	if rc.db != nil {
		rc.db.Close()
	}
	log.Info("Restore client closed")
}

// InitBackupMeta loads schemas from BackupMeta to initialize RestoreClient.
func (rc *Client) InitBackupMeta(
	c context.Context,
	backupMeta *backuppb.BackupMeta,
	backend *backuppb.StorageBackend,
	externalStorage storage.ExternalStorage,
	reader *metautil.MetaReader) error {
	if !backupMeta.IsRawKv {
		databases, err := utils.LoadBackupTables(c, reader)
		if err != nil {
			return errors.Trace(err)
		}
		rc.databases = databases

		var ddlJobs []*model.Job
		// ddls is the bytes of json.Marshal
		ddls, err := reader.ReadDDLs(c)
		if err != nil {
			return errors.Trace(err)
		}
		if len(ddls) != 0 {
			err = json.Unmarshal(ddls, &ddlJobs)
			if err != nil {
				return errors.Trace(err)
			}
		}
		rc.ddlJobs = ddlJobs
	}
	rc.backupMeta = backupMeta
	log.Info("load backupmeta", zap.Int("databases", len(rc.databases)), zap.Int("jobs", len(rc.ddlJobs)))

	metaClient := NewSplitClient(rc.pdClient, rc.tlsConf, rc.backupMeta.IsRawKv)
	importCli := NewImportClient(metaClient, rc.tlsConf, rc.keepaliveConf)
	rc.fileImporter = NewFileImporter(metaClient, importCli, backend, rc.backupMeta.IsRawKv, rc.rateLimit)
	return rc.fileImporter.CheckMultiIngestSupport(c, rc.pdClient)
}

// IsRawKvMode checks whether the backup data is in raw kv format, in which case transactional recover is forbidden.
func (rc *Client) IsRawKvMode() bool {
	return rc.backupMeta.IsRawKv
}

// GetFilesInRawRange gets all files that are in the given range or intersects with the given range.
func (rc *Client) GetFilesInRawRange(startKey []byte, endKey []byte, cf string) ([]*backuppb.File, error) {
	if !rc.IsRawKvMode() {
		return nil, errors.Annotate(berrors.ErrRestoreModeMismatch, "the backup data is not in raw kv mode")
	}

	for _, rawRange := range rc.backupMeta.RawRanges {
		// First check whether the given range is backup-ed. If not, we cannot perform the restore.
		if rawRange.Cf != cf {
			continue
		}

		if (len(rawRange.EndKey) > 0 && bytes.Compare(startKey, rawRange.EndKey) >= 0) ||
			(len(endKey) > 0 && bytes.Compare(rawRange.StartKey, endKey) >= 0) {
			// The restoring range is totally out of the current range. Skip it.
			continue
		}

		if bytes.Compare(startKey, rawRange.StartKey) < 0 ||
			utils.CompareEndKey(endKey, rawRange.EndKey) > 0 {
			// Only partial of the restoring range is in the current backup-ed range. So the given range can't be fully
			// restored.
			return nil, errors.Annotatef(berrors.ErrRestoreRangeMismatch,
				"the given range to restore [%s, %s) is not fully covered by the range that was backed up [%s, %s)",
				redact.Key(startKey), redact.Key(endKey), redact.Key(rawRange.StartKey), redact.Key(rawRange.EndKey),
			)
		}

		// We have found the range that contains the given range. Find all necessary files.
		files := make([]*backuppb.File, 0)

		for _, file := range rc.backupMeta.Files {
			if file.Cf != cf {
				continue
			}

			if len(file.EndKey) > 0 && bytes.Compare(file.EndKey, startKey) < 0 {
				// The file is before the range to be restored.
				continue
			}
			if len(endKey) > 0 && bytes.Compare(endKey, file.StartKey) <= 0 {
				// The file is after the range to be restored.
				// The specified endKey is exclusive, so when it equals to a file's startKey, the file is still skipped.
				continue
			}

			files = append(files, file)
		}

		// There should be at most one backed up range that covers the restoring range.
		return files, nil
	}

	return nil, errors.Annotate(berrors.ErrRestoreRangeMismatch, "no backup data in the range")
}

// SetConcurrency sets the concurrency of dbs tables files.
func (rc *Client) SetConcurrency(c uint) {
	rc.workerPool = utils.NewWorkerPool(c, "file")
}

// EnableOnline sets the mode of restore to online.
func (rc *Client) EnableOnline() {
	rc.isOnline = true
}

// GetTLSConfig returns the tls config.
func (rc *Client) GetTLSConfig() *tls.Config {
	return rc.tlsConf
}

// GetTS gets a new timestamp from PD.
func (rc *Client) GetTS(ctx context.Context) (uint64, error) {
	p, l, err := rc.pdClient.GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	restoreTS := oracle.ComposeTS(p, l)
	return restoreTS, nil
}

// ResetTS resets the timestamp of PD to a bigger value.
func (rc *Client) ResetTS(ctx context.Context, pdAddrs []string) error {
	restoreTS := rc.backupMeta.GetEndVersion()
	log.Info("reset pd timestamp", zap.Uint64("ts", restoreTS))
	i := 0
	return utils.WithRetry(ctx, func() error {
		idx := i % len(pdAddrs)
		i++
		return pdutil.ResetTS(ctx, pdAddrs[idx], restoreTS, rc.tlsConf)
	}, utils.NewPDReqBackoffer())
}

// GetPlacementRules return the current placement rules.
func (rc *Client) GetPlacementRules(ctx context.Context, pdAddrs []string) ([]pdtypes.Rule, error) {
	var placementRules []pdtypes.Rule
	i := 0
	errRetry := utils.WithRetry(ctx, func() error {
		var err error
		idx := i % len(pdAddrs)
		i++
		placementRules, err = pdutil.GetPlacementRules(ctx, pdAddrs[idx], rc.tlsConf)
		return errors.Trace(err)
	}, utils.NewPDReqBackoffer())
	return placementRules, errors.Trace(errRetry)
}

// GetDatabases returns all databases.
func (rc *Client) GetDatabases() []*utils.Database {
	dbs := make([]*utils.Database, 0, len(rc.databases))
	for _, db := range rc.databases {
		dbs = append(dbs, db)
	}
	return dbs
}

// GetDatabase returns a database by name.
func (rc *Client) GetDatabase(name string) *utils.Database {
	return rc.databases[name]
}

// GetPlacementPolicies returns policies.
func (rc *Client) GetPlacementPolicies() (*sync.Map, error) {
	policies := &sync.Map{}
	for _, p := range rc.backupMeta.Policies {
		policyInfo := &model.PolicyInfo{}
		err := json.Unmarshal(p.Info, policyInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		policies.Store(policyInfo.Name.L, policyInfo)
	}
	return policies, nil
}

// GetDDLJobs returns ddl jobs.
func (rc *Client) GetDDLJobs() []*model.Job {
	return rc.ddlJobs
}

// GetTableSchema returns the schema of a table from TiDB.
func (rc *Client) GetTableSchema(
	dom *domain.Domain,
	dbName model.CIStr,
	tableName model.CIStr,
) (*model.TableInfo, error) {
	info := dom.InfoSchema()
	table, err := info.TableByName(dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return table.Meta(), nil
}

// CreatePolicies creates all policies in full restore.
func (rc *Client) CreatePolicies(ctx context.Context, policyMap *sync.Map) error {
	var err error
	policyMap.Range(func(key, value interface{}) bool {
		e := rc.db.CreatePlacementPolicy(ctx, value.(*model.PolicyInfo))
		if e != nil {
			err = e
			return false
		}
		return true
	})
	return err
}

// CreateDatabase creates a database.
func (rc *Client) CreateDatabase(ctx context.Context, db *model.DBInfo) error {
	if rc.IsSkipCreateSQL() {
		log.Info("skip create database", zap.Stringer("database", db.Name))
		return nil
	}

	if !rc.supportPolicy {
		log.Info("set placementPolicyRef to nil when target tidb not support policy",
			zap.Stringer("database", db.Name))
		db.PlacementPolicyRef = nil
	}

	if db.PlacementPolicyRef != nil {
		if err := rc.db.ensurePlacementPolicy(ctx, db.PlacementPolicyRef.Name, rc.policyMap); err != nil {
			return errors.Trace(err)
		}
	}

	return rc.db.CreateDatabase(ctx, db)
}

// CreateTables creates multiple tables, and returns their rewrite rules.
func (rc *Client) CreateTables(
	dom *domain.Domain,
	tables []*metautil.Table,
	newTS uint64,
) (*RewriteRules, []*model.TableInfo, error) {
	rewriteRules := &RewriteRules{
		Data: make([]*import_sstpb.RewriteRule, 0),
	}
	newTables := make([]*model.TableInfo, 0, len(tables))
	errCh := make(chan error, 1)
	tbMapping := map[string]int{}
	for i, t := range tables {
		tbMapping[t.Info.Name.String()] = i
	}
	dataCh := rc.GoCreateTables(context.TODO(), dom, tables, newTS, errCh)
	for et := range dataCh {
		rules := et.RewriteRule
		rewriteRules.Data = append(rewriteRules.Data, rules.Data...)
		newTables = append(newTables, et.Table)
	}
	// Let's ensure that it won't break the original order.
	sort.Slice(newTables, func(i, j int) bool {
		return tbMapping[newTables[i].Name.String()] < tbMapping[newTables[j].Name.String()]
	})

	select {
	case err, ok := <-errCh:
		if ok {
			return nil, nil, errors.Trace(err)
		}
	default:
	}
	return rewriteRules, newTables, nil
}
func (rc *Client) createTables(
	ctx context.Context,
	db *DB,
	dom *domain.Domain,
	tables []*metautil.Table,
	newTS uint64,
) ([]CreatedTable, error) {
	log.Info("client to create tables")
	if rc.IsSkipCreateSQL() {
		log.Info("skip create table and alter autoIncID")
	} else {
		err := db.CreateTables(ctx, tables, rc.GetDDLJobsMap(), rc.GetSupportPolicy(), rc.GetPolicyMap())
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	cts := make([]CreatedTable, 0, len(tables))
	for _, table := range tables {
		newTableInfo, err := rc.GetTableSchema(dom, table.DB.Name, table.Info.Name)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if newTableInfo.IsCommonHandle != table.Info.IsCommonHandle {
			return nil, errors.Annotatef(berrors.ErrRestoreModeMismatch,
				"Clustered index option mismatch. Restored cluster's @@tidb_enable_clustered_index should be %v (backup table = %v, created table = %v).",
				transferBoolToValue(table.Info.IsCommonHandle),
				table.Info.IsCommonHandle,
				newTableInfo.IsCommonHandle)
		}
		rules := GetRewriteRules(newTableInfo, table.Info, newTS)
		ct := CreatedTable{
			RewriteRule: rules,
			Table:       newTableInfo,
			OldTable:    table,
		}
		log.Debug("new created tables", zap.Any("table", ct))
		cts = append(cts, ct)
	}
	return cts, nil
}

func (rc *Client) createTable(
	ctx context.Context,
	db *DB,
	dom *domain.Domain,
	table *metautil.Table,
	newTS uint64,
) (CreatedTable, error) {
	if rc.IsSkipCreateSQL() {
		log.Info("skip create table and alter autoIncID", zap.Stringer("table", table.Info.Name))
	} else {
		err := db.CreateTable(ctx, table, rc.GetDDLJobsMap(), rc.GetSupportPolicy(), rc.GetPolicyMap())
		if err != nil {
			return CreatedTable{}, errors.Trace(err)
		}
	}
	newTableInfo, err := rc.GetTableSchema(dom, table.DB.Name, table.Info.Name)
	if err != nil {
		return CreatedTable{}, errors.Trace(err)
	}
	if newTableInfo.IsCommonHandle != table.Info.IsCommonHandle {
		return CreatedTable{}, errors.Annotatef(berrors.ErrRestoreModeMismatch,
			"Clustered index option mismatch. Restored cluster's @@tidb_enable_clustered_index should be %v (backup table = %v, created table = %v).",
			transferBoolToValue(table.Info.IsCommonHandle),
			table.Info.IsCommonHandle,
			newTableInfo.IsCommonHandle)
	}
	rules := GetRewriteRules(newTableInfo, table.Info, newTS)
	et := CreatedTable{
		RewriteRule: rules,
		Table:       newTableInfo,
		OldTable:    table,
	}
	return et, nil
}

// GoCreateTables create tables, and generate their information.
// this function will use workers as the same number of sessionPool,
// leave sessionPool nil to send DDLs sequential.
func (rc *Client) GoCreateTables(
	ctx context.Context,
	dom *domain.Domain,
	tables []*metautil.Table,
	newTS uint64,
	errCh chan<- error,
) <-chan CreatedTable {
	// Could we have a smaller size of tables?
	log.Info("start create tables")

	rc.GenerateDDLJobsMap()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.GoCreateTables", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	outCh := make(chan CreatedTable, len(tables))
	rater := logutil.TraceRateOver(logutil.MetricTableCreatedCounter)

	var err error

	if rc.batchDdlSize > minBatchDdlSize && len(rc.dbPool) > 0 {

		err = rc.createTablesInWorkerPool(ctx, dom, tables, newTS, outCh)

		if err == nil {
			defer log.Debug("all tables are created")
			close(outCh)
			return outCh
			// fall back to old create table (sequential create table)
		} else if utils.FallBack2CreateTable(err) {
			log.Info("fall back to the sequential create table")
		} else {
			errCh <- err
			close(outCh)
			return outCh
		}

	}

	createOneTable := func(c context.Context, db *DB, t *metautil.Table) error {
		select {
		case <-c.Done():
			return c.Err()
		default:

		}
		rt, err := rc.createTable(c, db, dom, t, newTS)
		if err != nil {
			log.Error("create table failed",
				zap.Error(err),
				zap.Stringer("db", t.DB.Name),
				zap.Stringer("table", t.Info.Name))
			return errors.Trace(err)
		}
		log.Debug("table created and send to next",
			zap.Int("output chan size", len(outCh)),
			zap.Stringer("table", t.Info.Name),
			zap.Stringer("database", t.DB.Name))
		outCh <- rt
		rater.Inc()
		rater.L().Info("table created",
			zap.Stringer("table", t.Info.Name),
			zap.Stringer("database", t.DB.Name))
		return nil
	}
	go func() {
		defer close(outCh)
		defer log.Debug("all tables are created")
		var err error
		if len(rc.dbPool) > 0 {
			err = rc.createTablesWithDBPool(ctx, createOneTable, tables)
		} else {
			err = rc.createTablesWithSoleDB(ctx, createOneTable, tables)
		}
		if err != nil {
			errCh <- err
		}
	}()

	return outCh
}

func (rc *Client) createTablesWithSoleDB(ctx context.Context,
	createOneTable func(ctx context.Context, db *DB, t *metautil.Table) error,
	tables []*metautil.Table) error {
	for _, t := range tables {
		if err := createOneTable(ctx, rc.db, t); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (rc *Client) createTablesWithDBPool(ctx context.Context,
	createOneTable func(ctx context.Context, db *DB, t *metautil.Table) error,
	tables []*metautil.Table) error {
	eg, ectx := errgroup.WithContext(ctx)
	workers := utils.NewWorkerPool(uint(len(rc.dbPool)), "DDL workers")
	for _, t := range tables {
		table := t
		workers.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			db := rc.dbPool[id%uint64(len(rc.dbPool))]
			return createOneTable(ectx, db, table)
		})
	}
	return eg.Wait()
}

func (rc *Client) createTablesInWorkerPool(ctx context.Context, dom *domain.Domain, tables []*metautil.Table, newTS uint64, outCh chan<- CreatedTable) error {
	eg, ectx := errgroup.WithContext(ctx)
	rater := logutil.TraceRateOver(logutil.MetricTableCreatedCounter)
	workers := utils.NewWorkerPool(uint(len(rc.dbPool)), "Create Tables Worker")
	numOfTables := len(tables)

	for lastSent := 0; lastSent < numOfTables; lastSent += int(rc.batchDdlSize) {
		end := utils.MinInt(lastSent+int(rc.batchDdlSize), len(tables))
		log.Info("create tables", zap.Int("table start", lastSent), zap.Int("table end", end))

		tableSlice := tables[lastSent:end]
		workers.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			db := rc.dbPool[id%uint64(len(rc.dbPool))]
			cts, err := rc.createTables(ectx, db, dom, tableSlice, newTS) // ddl job for [lastSent:i)
			failpoint.Inject("restore-createtables-error", func(val failpoint.Value) {
				if val.(bool) {
					err = errors.New("sample error without extra message")
				}
			})
			if err != nil {
				log.Error("create tables fail")
				return err
			}
			for _, ct := range cts {
				log.Debug("table created and send to next",
					zap.Int("output chan size", len(outCh)),
					zap.Stringer("table", ct.OldTable.Info.Name),
					zap.Stringer("database", ct.OldTable.DB.Name))
				outCh <- ct
				rater.Inc()
				rater.L().Info("table created",
					zap.Stringer("table", ct.OldTable.Info.Name),
					zap.Stringer("database", ct.OldTable.DB.Name))
			}
			return err
		})
	}
	return eg.Wait()
}

// ExecDDLs executes the queries of the ddl jobs.
func (rc *Client) ExecDDLs(ctx context.Context, ddlJobs []*model.Job) error {
	// Sort the ddl jobs by schema version in ascending order.
	sort.Slice(ddlJobs, func(i, j int) bool {
		return ddlJobs[i].BinlogInfo.SchemaVersion < ddlJobs[j].BinlogInfo.SchemaVersion
	})

	for _, job := range ddlJobs {
		err := rc.db.ExecDDL(ctx, job)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("execute ddl query",
			zap.String("db", job.SchemaName),
			zap.String("query", job.Query),
			zap.Int64("historySchemaVersion", job.BinlogInfo.SchemaVersion))
	}
	return nil
}

func (rc *Client) setSpeedLimit(ctx context.Context) error {
	if !rc.hasSpeedLimited && rc.rateLimit != 0 {
		stores, err := conn.GetAllTiKVStores(ctx, rc.pdClient, conn.SkipTiFlash)
		if err != nil {
			return errors.Trace(err)
		}
		for _, store := range stores {
			err = rc.fileImporter.setDownloadSpeedLimit(ctx, store.GetId())
			if err != nil {
				return errors.Trace(err)
			}
		}
		rc.hasSpeedLimited = true
	}
	return nil
}

// isFilesBelongToSameRange check whether two files are belong to the same range with different cf.
func isFilesBelongToSameRange(f1, f2 string) bool {
	// the backup date file pattern is `{store_id}_{region_id}_{epoch_version}_{key}_{ts}_{cf}.sst`
	// so we need to compare with out the `_{cf}.sst` suffix
	idx1 := strings.LastIndex(f1, "_")
	idx2 := strings.LastIndex(f2, "_")

	if idx1 < 0 || idx2 < 0 {
		panic(fmt.Sprintf("invalid backup data file name: '%s', '%s'", f1, f2))
	}

	return f1[:idx1] == f2[:idx2]
}

func drainFilesByRange(files []*backuppb.File, supportMulti bool) ([]*backuppb.File, []*backuppb.File) {
	if len(files) == 0 {
		return nil, nil
	}
	if !supportMulti {
		return files[:1], files[1:]
	}
	idx := 1
	for idx < len(files) {
		if !isFilesBelongToSameRange(files[idx-1].Name, files[idx].Name) {
			break
		}
		idx++
	}

	return files[:idx], files[idx:]
}

// RestoreFiles tries to restore the files.
func (rc *Client) RestoreFiles(
	ctx context.Context,
	files []*backuppb.File,
	rewriteRules *RewriteRules,
	updateCh glue.Progress,
) (err error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if err == nil {
			log.Info("Restore files", zap.Duration("take", elapsed), logutil.Files(files))
			summary.CollectSuccessUnit("files", len(files), elapsed)
		}
	}()

	log.Debug("start to restore files", zap.Int("files", len(files)))

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.RestoreFiles", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	eg, ectx := errgroup.WithContext(ctx)
	err = rc.setSpeedLimit(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	var rangeFiles []*backuppb.File
	var leftFiles []*backuppb.File
	for rangeFiles, leftFiles = drainFilesByRange(files, rc.fileImporter.supportMultiIngest); len(rangeFiles) != 0; rangeFiles, leftFiles = drainFilesByRange(leftFiles, rc.fileImporter.supportMultiIngest) {
		filesReplica := rangeFiles
		rc.workerPool.ApplyOnErrorGroup(eg,
			func() error {
				fileStart := time.Now()
				defer func() {
					log.Info("import files done", logutil.Files(filesReplica),
						zap.Duration("take", time.Since(fileStart)))
					updateCh.Inc()
				}()
				return rc.fileImporter.Import(ectx, filesReplica, rewriteRules, rc.cipher, rc.backupMeta.ApiVersion)
			})
	}

	if err := eg.Wait(); err != nil {
		summary.CollectFailureUnit("file", err)
		log.Error(
			"restore files failed",
			zap.Error(err),
		)
		return errors.Trace(err)
	}
	return nil
}

// RestoreRaw tries to restore raw keys in the specified range.
func (rc *Client) RestoreRaw(
	ctx context.Context, startKey []byte, endKey []byte, files []*backuppb.File, updateCh glue.Progress,
) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Info("Restore Raw",
			logutil.Key("startKey", startKey),
			logutil.Key("endKey", endKey),
			zap.Duration("take", elapsed))
	}()
	errCh := make(chan error, len(files))
	eg, ectx := errgroup.WithContext(ctx)
	defer close(errCh)

	err := rc.fileImporter.SetRawRange(startKey, endKey)
	if err != nil {
		return errors.Trace(err)
	}

	for _, file := range files {
		fileReplica := file
		rc.workerPool.ApplyOnErrorGroup(eg,
			func() error {
				defer updateCh.Inc()
				return rc.fileImporter.Import(ectx, []*backuppb.File{fileReplica}, EmptyRewriteRule(), rc.cipher, rc.backupMeta.ApiVersion)
			})
	}
	if err := eg.Wait(); err != nil {
		log.Error(
			"restore raw range failed",
			logutil.Key("startKey", startKey),
			logutil.Key("endKey", endKey),
			zap.Error(err),
		)
		return errors.Trace(err)
	}
	log.Info(
		"finish to restore raw range",
		logutil.Key("startKey", startKey),
		logutil.Key("endKey", endKey),
	)
	return nil
}

// SwitchToImportMode switch tikv cluster to import mode.
func (rc *Client) SwitchToImportMode(ctx context.Context) {
	// tikv automatically switch to normal mode in every 10 minutes
	// so we need ping tikv in less than 10 minute
	go func() {
		tick := time.NewTicker(rc.switchModeInterval)
		defer tick.Stop()

		// [important!] switch tikv mode into import at the beginning
		log.Info("switch to import mode at beginning")
		err := rc.switchTiKVMode(ctx, import_sstpb.SwitchMode_Import)
		if err != nil {
			log.Warn("switch to import mode failed", zap.Error(err))
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				log.Info("switch to import mode")
				err := rc.switchTiKVMode(ctx, import_sstpb.SwitchMode_Import)
				if err != nil {
					log.Warn("switch to import mode failed", zap.Error(err))
				}
			case <-rc.switchCh:
				log.Info("stop automatic switch to import mode")
				return
			}
		}
	}()
}

// SwitchToNormalMode switch tikv cluster to normal mode.
func (rc *Client) SwitchToNormalMode(ctx context.Context) error {
	close(rc.switchCh)
	return rc.switchTiKVMode(ctx, import_sstpb.SwitchMode_Normal)
}

func (rc *Client) switchTiKVMode(ctx context.Context, mode import_sstpb.SwitchMode) error {
	stores, err := conn.GetAllTiKVStores(ctx, rc.pdClient, conn.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = time.Second * 3
	for _, store := range stores {
		opt := grpc.WithInsecure()
		if rc.tlsConf != nil {
			opt = grpc.WithTransportCredentials(credentials.NewTLS(rc.tlsConf))
		}
		gctx, cancel := context.WithTimeout(ctx, time.Second*5)
		connection, err := grpc.DialContext(
			gctx,
			store.GetAddress(),
			opt,
			grpc.WithBlock(),
			grpc.FailOnNonTempDialError(true),
			grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
			// we don't need to set keepalive timeout here, because the connection lives
			// at most 5s. (shorter than minimal value for keepalive time!)
		)
		cancel()
		if err != nil {
			return errors.Trace(err)
		}
		client := import_sstpb.NewImportSSTClient(connection)
		_, err = client.SwitchMode(ctx, &import_sstpb.SwitchModeRequest{
			Mode: mode,
		})
		if err != nil {
			return errors.Trace(err)
		}
		err = connection.Close()
		if err != nil {
			log.Error("close grpc connection failed in switch mode", zap.Error(err))
			continue
		}
	}
	return nil
}

// GoValidateChecksum forks a goroutine to validate checksum after restore.
// it returns a channel fires a struct{} when all things get done.
func (rc *Client) GoValidateChecksum(
	ctx context.Context,
	tableStream <-chan CreatedTable,
	kvClient kv.Client,
	errCh chan<- error,
	updateCh glue.Progress,
	concurrency uint,
) <-chan struct{} {
	log.Info("Start to validate checksum")
	outCh := make(chan struct{}, 1)
	wg := new(sync.WaitGroup)
	wg.Add(2)
	loadStatCh := make(chan *CreatedTable, 1024)
	// run the stat loader
	go func() {
		defer wg.Done()
		rc.updateMetaAndLoadStats(ctx, loadStatCh)
	}()
	workers := utils.NewWorkerPool(defaultChecksumConcurrency, "RestoreChecksum")
	go func() {
		eg, ectx := errgroup.WithContext(ctx)
		defer func() {
			if err := eg.Wait(); err != nil {
				errCh <- err
			}
			close(loadStatCh)
			wg.Done()
		}()

		for {
			select {
			// if we use ectx here, maybe canceled will mask real error.
			case <-ctx.Done():
				errCh <- ctx.Err()
			case tbl, ok := <-tableStream:
				if !ok {
					return
				}

				workers.ApplyOnErrorGroup(eg, func() error {
					start := time.Now()
					defer func() {
						elapsed := time.Since(start)
						summary.CollectSuccessUnit("table checksum", 1, elapsed)
					}()
					err := rc.execChecksum(ectx, tbl, kvClient, concurrency, loadStatCh)
					if err != nil {
						return errors.Trace(err)
					}
					updateCh.Inc()
					return nil
				})
			}
		}
	}()
	go func() {
		wg.Wait()
		log.Info("all checksum ended")
		close(outCh)
	}()
	return outCh
}

func (rc *Client) execChecksum(
	ctx context.Context,
	tbl CreatedTable,
	kvClient kv.Client,
	concurrency uint,
	loadStatCh chan<- *CreatedTable,
) error {
	logger := log.With(
		zap.String("db", tbl.OldTable.DB.Name.O),
		zap.String("table", tbl.OldTable.Info.Name.O),
	)

	if tbl.OldTable.NoChecksum() {
		logger.Warn("table has no checksum, skipping checksum")
		return nil
	}

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.execChecksum", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	startTS, err := rc.GetTS(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	exe, err := checksum.NewExecutorBuilder(tbl.Table, startTS).
		SetOldTable(tbl.OldTable).
		SetConcurrency(concurrency).
		Build()
	if err != nil {
		return errors.Trace(err)
	}
	checksumResp, err := exe.Execute(ctx, kvClient, func() {
		// TODO: update progress here.
	})
	if err != nil {
		return errors.Trace(err)
	}

	table := tbl.OldTable
	if checksumResp.Checksum != table.Crc64Xor ||
		checksumResp.TotalKvs != table.TotalKvs ||
		checksumResp.TotalBytes != table.TotalBytes {
		logger.Error("failed in validate checksum",
			zap.Uint64("origin tidb crc64", table.Crc64Xor),
			zap.Uint64("calculated crc64", checksumResp.Checksum),
			zap.Uint64("origin tidb total kvs", table.TotalKvs),
			zap.Uint64("calculated total kvs", checksumResp.TotalKvs),
			zap.Uint64("origin tidb total bytes", table.TotalBytes),
			zap.Uint64("calculated total bytes", checksumResp.TotalBytes),
		)
		return errors.Annotate(berrors.ErrRestoreChecksumMismatch, "failed to validate checksum")
	}

	loadStatCh <- &tbl
	return nil
}

func (rc *Client) updateMetaAndLoadStats(ctx context.Context, input <-chan *CreatedTable) {
	for {
		select {
		case <-ctx.Done():
			return
		case tbl, ok := <-input:
			if !ok {
				return
			}

			// Not need to return err when failed because of update analysis-meta
			restoreTS, err := rc.GetTS(ctx)
			if err != nil {
				log.Error("getTS failed", zap.Error(err))
			} else {
				err = rc.db.UpdateStatsMeta(ctx, tbl.Table.ID, restoreTS, tbl.OldTable.TotalKvs)
				if err != nil {
					log.Error("update stats meta failed", zap.Any("table", tbl.Table), zap.Error(err))
				}
			}

			table := tbl.OldTable
			if table.Stats != nil {
				log.Info("start loads analyze after validate checksum",
					zap.Int64("old id", tbl.OldTable.Info.ID),
					zap.Int64("new id", tbl.Table.ID),
				)
				start := time.Now()
				if err := rc.statsHandler.LoadStatsFromJSON(rc.dom.InfoSchema(), table.Stats); err != nil {
					log.Error("analyze table failed", zap.Any("table", table.Stats), zap.Error(err))
				}
				log.Info("restore stat done",
					zap.String("table", table.Info.Name.L),
					zap.String("db", table.DB.Name.L),
					zap.Duration("cost", time.Since(start)))
			}
		}
	}
}

const (
	restoreLabelKey   = "exclusive"
	restoreLabelValue = "restore"
)

// LoadRestoreStores loads the stores used to restore data.
func (rc *Client) LoadRestoreStores(ctx context.Context) error {
	if !rc.isOnline {
		return nil
	}
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Client.LoadRestoreStores", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	stores, err := rc.pdClient.GetAllStores(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	for _, s := range stores {
		if s.GetState() != metapb.StoreState_Up {
			continue
		}
		for _, l := range s.GetLabels() {
			if l.GetKey() == restoreLabelKey && l.GetValue() == restoreLabelValue {
				rc.restoreStores = append(rc.restoreStores, s.GetId())
				break
			}
		}
	}
	log.Info("load restore stores", zap.Uint64s("store-ids", rc.restoreStores))
	return nil
}

// ResetRestoreLabels removes the exclusive labels of the restore stores.
func (rc *Client) ResetRestoreLabels(ctx context.Context) error {
	if !rc.isOnline {
		return nil
	}
	log.Info("start reseting store labels")
	return rc.toolClient.SetStoresLabel(ctx, rc.restoreStores, restoreLabelKey, "")
}

// SetupPlacementRules sets rules for the tables' regions.
func (rc *Client) SetupPlacementRules(ctx context.Context, tables []*model.TableInfo) error {
	if !rc.isOnline || len(rc.restoreStores) == 0 {
		return nil
	}
	log.Info("start setting placement rules")
	rule, err := rc.toolClient.GetPlacementRule(ctx, "pd", "default")
	if err != nil {
		return errors.Trace(err)
	}
	rule.Index = 100
	rule.Override = true
	rule.LabelConstraints = append(rule.LabelConstraints, pdtypes.LabelConstraint{
		Key:    restoreLabelKey,
		Op:     "in",
		Values: []string{restoreLabelValue},
	})
	for _, t := range tables {
		rule.ID = rc.getRuleID(t.ID)
		rule.StartKeyHex = hex.EncodeToString(codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID)))
		rule.EndKeyHex = hex.EncodeToString(codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID+1)))
		err = rc.toolClient.SetPlacementRule(ctx, rule)
		if err != nil {
			return errors.Trace(err)
		}
	}
	log.Info("finish setting placement rules")
	return nil
}

// WaitPlacementSchedule waits PD to move tables to restore stores.
func (rc *Client) WaitPlacementSchedule(ctx context.Context, tables []*model.TableInfo) error {
	if !rc.isOnline || len(rc.restoreStores) == 0 {
		return nil
	}
	log.Info("start waiting placement schedule")
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ok, progress, err := rc.checkRegions(ctx, tables)
			if err != nil {
				return errors.Trace(err)
			}
			if ok {
				log.Info("finish waiting placement schedule")
				return nil
			}
			log.Info("placement schedule progress: " + progress)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (rc *Client) checkRegions(ctx context.Context, tables []*model.TableInfo) (bool, string, error) {
	for i, t := range tables {
		start := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID))
		end := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(t.ID+1))
		ok, regionProgress, err := rc.checkRange(ctx, start, end)
		if err != nil {
			return false, "", errors.Trace(err)
		}
		if !ok {
			return false, fmt.Sprintf("table %v/%v, %s", i, len(tables), regionProgress), nil
		}
	}
	return true, "", nil
}

func (rc *Client) checkRange(ctx context.Context, start, end []byte) (bool, string, error) {
	regions, err := rc.toolClient.ScanRegions(ctx, start, end, -1)
	if err != nil {
		return false, "", errors.Trace(err)
	}
	for i, r := range regions {
	NEXT_PEER:
		for _, p := range r.Region.GetPeers() {
			for _, storeID := range rc.restoreStores {
				if p.GetStoreId() == storeID {
					continue NEXT_PEER
				}
			}
			return false, fmt.Sprintf("region %v/%v", i, len(regions)), nil
		}
	}
	return true, "", nil
}

// ResetPlacementRules removes placement rules for tables.
func (rc *Client) ResetPlacementRules(ctx context.Context, tables []*model.TableInfo) error {
	if !rc.isOnline || len(rc.restoreStores) == 0 {
		return nil
	}
	log.Info("start reseting placement rules")
	var failedTables []int64
	for _, t := range tables {
		err := rc.toolClient.DeletePlacementRule(ctx, "pd", rc.getRuleID(t.ID))
		if err != nil {
			log.Info("failed to delete placement rule for table", zap.Int64("table-id", t.ID))
			failedTables = append(failedTables, t.ID)
		}
	}
	if len(failedTables) > 0 {
		return errors.Annotatef(berrors.ErrPDInvalidResponse, "failed to delete placement rules for tables %v", failedTables)
	}
	return nil
}

func (rc *Client) getRuleID(tableID int64) string {
	return "restore-t" + strconv.FormatInt(tableID, 10)
}

// IsIncremental returns whether this backup is incremental.
func (rc *Client) IsIncremental() bool {
	return !(rc.backupMeta.StartVersion == rc.backupMeta.EndVersion ||
		rc.backupMeta.StartVersion == 0)
}

// EnableSkipCreateSQL sets switch of skip create schema and tables.
func (rc *Client) EnableSkipCreateSQL() {
	rc.noSchema = true
}

// IsSkipCreateSQL returns whether we need skip create schema and tables in restore.
func (rc *Client) IsSkipCreateSQL() bool {
	return rc.noSchema
}

// GenerateDDLJobsMap returns a map[UniqueTableName]bool about < db table, hasCreate/hasTruncate DDL >.
// if we execute some DDLs before create table.
// we may get two situation that need to rebase auto increment/random id.
// 1. truncate table: truncate will generate new id cache.
// 2. create table/create and rename table: the first create table will lock down the id cache.
// because we cannot create onExistReplace table.
// so the final create DDL with the correct auto increment/random id won't be executed.
func (rc *Client) GenerateDDLJobsMap() {
	rc.ddlJobsMap = make(map[UniqueTableName]bool)
	for _, job := range rc.ddlJobs {
		switch job.Type {
		case model.ActionTruncateTable, model.ActionCreateTable, model.ActionRenameTable:
			rc.ddlJobsMap[UniqueTableName{job.SchemaName, job.BinlogInfo.TableInfo.Name.String()}] = true
		}
	}
}

func (rc *Client) GetDDLJobsMap() map[UniqueTableName]bool {
	return rc.ddlJobsMap
}

// PreCheckTableTiFlashReplica checks whether TiFlash replica is less than TiFlash node.
func (rc *Client) PreCheckTableTiFlashReplica(
	ctx context.Context,
	tables []*metautil.Table,
) error {
	tiFlashStores, err := conn.GetAllTiKVStores(ctx, rc.pdClient, conn.TiFlashOnly)
	if err != nil {
		return errors.Trace(err)
	}
	tiFlashStoreCount := len(tiFlashStores)
	for _, table := range tables {
		if table.Info.TiFlashReplica != nil && table.Info.TiFlashReplica.Count > uint64(tiFlashStoreCount) {
			// we cannot satisfy TiFlash replica in restore cluster. so we should
			// set TiFlashReplica to unavailable in tableInfo, to avoid TiDB cannot sense TiFlash and make plan to TiFlash
			// see details at https://github.com/pingcap/br/issues/931
			table.Info.TiFlashReplica = nil
		}
	}
	return nil
}

// PreCheckTableClusterIndex checks whether backup tables and existed tables have different cluster index options。
func (rc *Client) PreCheckTableClusterIndex(
	tables []*metautil.Table,
	ddlJobs []*model.Job,
	dom *domain.Domain,
) error {
	for _, table := range tables {
		oldTableInfo, err := rc.GetTableSchema(dom, table.DB.Name, table.Info.Name)
		// table exists in database
		if err == nil {
			if table.Info.IsCommonHandle != oldTableInfo.IsCommonHandle {
				return errors.Annotatef(berrors.ErrRestoreModeMismatch,
					"Clustered index option mismatch. Restored cluster's @@tidb_enable_clustered_index should be %v (backup table = %v, created table = %v).",
					transferBoolToValue(table.Info.IsCommonHandle),
					table.Info.IsCommonHandle,
					oldTableInfo.IsCommonHandle)
			}
		}
	}
	for _, job := range ddlJobs {
		if job.Type == model.ActionCreateTable {
			tableInfo := job.BinlogInfo.TableInfo
			if tableInfo != nil {
				oldTableInfo, err := rc.GetTableSchema(dom, model.NewCIStr(job.SchemaName), tableInfo.Name)
				// table exists in database
				if err == nil {
					if tableInfo.IsCommonHandle != oldTableInfo.IsCommonHandle {
						return errors.Annotatef(berrors.ErrRestoreModeMismatch,
							"Clustered index option mismatch. Restored cluster's @@tidb_enable_clustered_index should be %v (backup table = %v, created table = %v).",
							transferBoolToValue(tableInfo.IsCommonHandle),
							tableInfo.IsCommonHandle,
							oldTableInfo.IsCommonHandle)
					}
				}
			}
		}
	}
	return nil
}

func transferBoolToValue(enable bool) string {
	if enable {
		return "ON"
	}
	return "OFF"
}
