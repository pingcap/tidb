// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package restore

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/tidb"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	_ "github.com/pingcap/tidb/planner/core" // to setup expression.EvalAstExpr. Otherwise we cannot parse the default value
	"github.com/pingcap/tidb/store/pdtypes"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

// PreRestoreInfoGetter defines the operations to get information from sources and target.
// These information are used in the preparation of the import ( like precheck ).
type PreRestoreInfoGetter interface {
	TargetInfoGetter
	// GetAllTableStructures gets all the table structures with the information from both the source and the target.
	GetAllTableStructures(ctx context.Context) (map[string]*checkpoints.TidbDBInfo, error)
	// ReadFirstNRowsByTableName reads the first N rows of data of an importing source table.
	ReadFirstNRowsByTableName(ctx context.Context, schemaName string, tableName string, n int) (cols []string, rows [][]types.Datum, err error)
	// ReadFirstNRowsByFileMeta reads the first N rows of an data file.
	ReadFirstNRowsByFileMeta(ctx context.Context, dataFileMeta mydump.SourceFileMeta, n int) (cols []string, rows [][]types.Datum, err error)
	// EstimateSourceDataSize estimates the datasize to generate during the import as well as some other sub-informaiton.
	// It will return:
	// * the estimated data size to generate during the import,
	//   which might include some extra index data to generate besides the source file data
	// * the total data size of all the source files,
	// * whether there are some unsorted big tables
	EstimateSourceDataSize(ctx context.Context) (int64, int64, bool, error)
}

// TargetInfoGetter defines the operations to get information from target.
type TargetInfoGetter interface {
	// FetchRemoteTableModels fetches the table structures from the remote target.
	FetchRemoteTableModels(ctx context.Context, schemaName string) ([]*model.TableInfo, error)
	// DoesTableContainData checks whether the specified table on the target DB contains data or not.
	DoesTableContainData(ctx context.Context, schemaName string, tableName string) (bool, error)
	// GetTargetSysVariablesForImport gets some important systam variables for importing on the target.
	GetTargetSysVariablesForImport(ctx context.Context) map[string]string
	// GetReplicationConfig gets the replication config on the target.
	GetReplicationConfig(ctx context.Context) (*pdtypes.ReplicationConfig, error)
	// GetStorageInfo gets the storage information on the target.
	GetStorageInfo(ctx context.Context) (*pdtypes.StoresInfo, error)
	// GetEmptyRegionsInfo gets the region information of all the empty regions on the target.
	GetEmptyRegionsInfo(ctx context.Context) (*pdtypes.RegionsInfo, error)
}

// TargetInfoGetterImpl implements the operations to get information from the target.
type TargetInfoGetterImpl struct {
	cfg          *config.Config
	targetDBGlue glue.Glue
	tls          *common.TLS
	backend      backend.TargetInfoGetter
}

// NewTargetInfoGetterImpl creates a TargetInfoGetterImpl object.
func NewTargetInfoGetterImpl(
	cfg *config.Config,
	targetDB *sql.DB,
) (*TargetInfoGetterImpl, error) {
	targetDBGlue := glue.NewExternalTiDBGlue(targetDB, cfg.TiDB.SQLMode)
	tls, err := cfg.ToTLS()
	if err != nil {
		return nil, errors.Trace(err)
	}
	var backendTargetInfoGetter backend.TargetInfoGetter
	switch cfg.TikvImporter.Backend {
	case config.BackendTiDB:
		backendTargetInfoGetter = tidb.NewTargetInfoGetter(targetDB)
	case config.BackendLocal:
		backendTargetInfoGetter = local.NewTargetInfoGetter(tls)
	default:
		return nil, common.ErrUnknownBackend.GenWithStackByArgs(cfg.TikvImporter.Backend)
	}
	return &TargetInfoGetterImpl{
		targetDBGlue: targetDBGlue,
		tls:          tls,
		backend:      backendTargetInfoGetter,
	}, nil
}

// FetchRemoteTableModels fetches the table structures from the remote target.
// It implements the TargetInfoGetter interface.
func (g *TargetInfoGetterImpl) FetchRemoteTableModels(ctx context.Context, schemaName string) ([]*model.TableInfo, error) {
	if !g.targetDBGlue.OwnsSQLExecutor() {
		return g.targetDBGlue.GetTables(ctx, schemaName)
	}
	return g.backend.FetchRemoteTableModels(ctx, schemaName)
}

// DoesTableContainData checks whether the specified table on the target DB contains data or not.
// It implements the TargetInfoGetter interface.
// It tries to select the row count from the target DB.
func (g *TargetInfoGetterImpl) DoesTableContainData(ctx context.Context, schemaName string, tableName string) (bool, error) {
	failpoint.Inject("CheckTableEmptyFailed", func() {
		failpoint.Return(false, errors.New("mock error"))
	})
	db, err := g.targetDBGlue.GetDB()
	if err != nil {
		return false, errors.Trace(err)
	}
	exec := common.SQLWithRetry{
		DB:     db,
		Logger: log.L(),
	}
	var dump int
	err = exec.QueryRow(ctx, "check table empty",
		fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", common.UniqueTable(schemaName, tableName)),
		&dump,
	)

	switch {
	case errors.ErrorEqual(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, errors.Trace(err)
	default:
		return true, nil
	}
}

// GetTargetSysVariablesForImport gets some important systam variables for importing on the target.
// It implements the TargetInfoGetter interface.
// It uses the SQL to fetch sys variables from the target.
func (g *TargetInfoGetterImpl) GetTargetSysVariablesForImport(ctx context.Context) map[string]string {
	sysVars := ObtainImportantVariables(ctx, g.targetDBGlue.GetSQLExecutor(), !isTiDBBackend(g.cfg))
	// override by manually set vars
	maps.Copy(sysVars, g.cfg.TiDB.Vars)
	return sysVars
}

// GetReplicationConfig gets the replication config on the target.
// It implements the TargetInfoGetter interface.
// It uses the PD interface through TLS to get the information.
func (g *TargetInfoGetterImpl) GetReplicationConfig(ctx context.Context) (*pdtypes.ReplicationConfig, error) {
	result := new(pdtypes.ReplicationConfig)
	if err := g.tls.WithHost(g.cfg.TiDB.PdAddr).GetJSON(ctx, pdReplicate, &result); err != nil {
		return nil, errors.Trace(err)
	}
	return result, nil
}

// GetStorageInfo gets the storage information on the target.
// It implements the TargetInfoGetter interface.
// It uses the PD interface through TLS to get the information.
func (g *TargetInfoGetterImpl) GetStorageInfo(ctx context.Context) (*pdtypes.StoresInfo, error) {
	result := new(pdtypes.StoresInfo)
	if err := g.tls.WithHost(g.cfg.TiDB.PdAddr).GetJSON(ctx, pdStores, result); err != nil {
		return nil, errors.Trace(err)
	}
	return result, nil
}

// GetEmptyRegionsInfo gets the region information of all the empty regions on the target.
// It implements the TargetInfoGetter interface.
// It uses the PD interface through TLS to get the information.
func (g *TargetInfoGetterImpl) GetEmptyRegionsInfo(ctx context.Context) (*pdtypes.RegionsInfo, error) {
	result := new(pdtypes.RegionsInfo)
	if err := g.tls.WithHost(g.cfg.TiDB.PdAddr).GetJSON(ctx, pdEmptyRegions, &result); err != nil {
		return nil, errors.Trace(err)
	}
	return result, nil
}

// PreRestoreInfoGetterImpl implements the operations to get information used in importing preparation.
type PreRestoreInfoGetterImpl struct {
	cfg              *config.Config
	srcStorage       storage.ExternalStorage
	ioWorkers        *worker.Pool
	kvEncBuilder     backend.KVEncodingBuilder
	targetInfoGetter TargetInfoGetter

	dbMetas          []*mydump.MDDatabaseMeta
	mdDBMetaMap      map[string]*mydump.MDDatabaseMeta
	mdDBTableMetaMap map[string]map[string]*mydump.MDTableMeta

	// cached data
	dbInfos map[string]*checkpoints.TidbDBInfo
	sysVars map[string]string
}

// NewPreRestoreInfoGetter creates a PreRestoreInfoGetterImpl object.
func NewPreRestoreInfoGetter(
	cfg *config.Config,
	dbMetas []*mydump.MDDatabaseMeta,
	srcStorage storage.ExternalStorage,
	targetInfoGetter TargetInfoGetter,
	ioWorkers *worker.Pool,
) (*PreRestoreInfoGetterImpl, error) {
	if ioWorkers == nil {
		ioWorkers = worker.NewPool(context.Background(), cfg.App.IOConcurrency, "pre_info_getter_io")
	}
	var kvEncBuilder backend.KVEncodingBuilder
	switch cfg.TikvImporter.Backend {
	case config.BackendTiDB:
		kvEncBuilder = tidb.NewKVEncodingBuilder()
	case config.BackendLocal:
		kvEncBuilder = local.NewKVEncodingBuilder(context.Background())
	default:
		return nil, common.ErrUnknownBackend.GenWithStackByArgs(cfg.TikvImporter.Backend)
	}

	result := &PreRestoreInfoGetterImpl{
		cfg:              cfg,
		dbMetas:          dbMetas,
		srcStorage:       srcStorage,
		ioWorkers:        ioWorkers,
		kvEncBuilder:     kvEncBuilder,
		targetInfoGetter: targetInfoGetter,
	}
	result.Init()
	return result, nil
}

// Init initializes some internal data and states for PreRestoreInfoGetterImpl.
func (p *PreRestoreInfoGetterImpl) Init() {
	mdDBMetaMap := make(map[string]*mydump.MDDatabaseMeta)
	mdDBTableMetaMap := make(map[string]map[string]*mydump.MDTableMeta)
	for _, dbMeta := range p.dbMetas {
		dbName := dbMeta.Name
		mdDBMetaMap[dbName] = dbMeta
		mdTableMetaMap, ok := mdDBTableMetaMap[dbName]
		if !ok {
			mdTableMetaMap = make(map[string]*mydump.MDTableMeta)
			mdDBTableMetaMap[dbName] = mdTableMetaMap
		}
		for _, tblMeta := range dbMeta.Tables {
			tblName := tblMeta.Name
			mdTableMetaMap[tblName] = tblMeta
		}
	}
	p.mdDBMetaMap = mdDBMetaMap
	p.mdDBTableMetaMap = mdDBTableMetaMap

	// clear the cache
	p.dbInfos = nil
	p.sysVars = nil
}

// GetAllTableStructures gets all the table structures with the information from both the source and the target.
// It implements the PreRestoreInfoGetter interface.
// It has a caching mechanism: the table structures will be obtained from the source only once.
func (p *PreRestoreInfoGetterImpl) GetAllTableStructures(ctx context.Context) (map[string]*checkpoints.TidbDBInfo, error) {
	if p.dbInfos == nil {
		dbInfos, err := LoadSchemaInfo(ctx, p.dbMetas, func(ctx context.Context, dbName string) ([]*model.TableInfo, error) {
			return p.getTableStructuresByFileMeta(ctx, p.mdDBMetaMap[dbName])
		})
		if err != nil {
			return nil, errors.Trace(err)
		}
		p.dbInfos = dbInfos
	}
	return p.dbInfos, nil
}

func (p *PreRestoreInfoGetterImpl) getTableStructuresByFileMeta(ctx context.Context, dbSrcFileMeta *mydump.MDDatabaseMeta) ([]*model.TableInfo, error) {
	dbName := dbSrcFileMeta.Name
	currentTableInfosFromDB, err := p.targetInfoGetter.FetchRemoteTableModels(ctx, dbName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	currentTableInfosMap := make(map[string]*model.TableInfo)
	for _, tblInfo := range currentTableInfosFromDB {
		currentTableInfosMap[tblInfo.Name.L] = tblInfo
	}
	resultInfos := make([]*model.TableInfo, len(dbSrcFileMeta.Tables))
	for i, tableFileMeta := range dbSrcFileMeta.Tables {
		if curTblInfo, ok := currentTableInfosMap[strings.ToLower(tableFileMeta.Name)]; ok {
			resultInfos[i] = curTblInfo
			continue
		}
		createTblSQL, err := tableFileMeta.GetSchema(ctx, p.srcStorage)
		if err != nil {
			return nil, errors.Annotatef(err, "get create table statement from schema file error: %s", tableFileMeta.Name)
		}
		theTableInfo, err := newTableInfo(createTblSQL, 0)
		if err != nil {
			errMsg := "generate table info from SQL error"
			log.L().Error(errMsg, zap.Error(err), zap.String("sql", createTblSQL), zap.String("table_name", tableFileMeta.Name))
			return nil, errors.Annotatef(err, "%s: %s", errMsg, tableFileMeta.Name)
		}
		resultInfos[i] = theTableInfo
	}
	return resultInfos, nil
}

func newTableInfo(createTblSQL string, tableID int64) (*model.TableInfo, error) {
	parser := parser.New()
	astNode, err := parser.ParseOneStmt(createTblSQL, "", "")
	if err != nil {
		errMsg := "parse sql statement error"
		log.L().Error(errMsg, zap.Error(err), zap.String("sql", createTblSQL))
		return nil, errors.Trace(err)
	}
	sctx := mock.NewContext()
	createTableStmt, ok := astNode.(*ast.CreateTableStmt)
	if !ok {
		return nil, errors.New("cannot transfer the parsed SQL as an CREATE TABLE statement")
	}
	info, err := ddl.MockTableInfo(sctx, createTableStmt, tableID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// set a auto_random bit if AUTO_RANDOM is set
	setAutoRandomBits(info, createTableStmt.Cols)
	info.State = model.StatePublic
	return info, nil
}

func setAutoRandomBits(tblInfo *model.TableInfo, colDefs []*ast.ColumnDef) {
	if tblInfo.PKIsHandle {
		pkColName := tblInfo.GetPkName()
		for _, colDef := range colDefs {
			if colDef.Name.Name.L == pkColName.L && colDef.Tp.GetType() == mysql.TypeLonglong {
				// potential AUTO_RANDOM candidate column, examine the options
				hasAutoRandom := false
				canSetAutoRandom := true
				var autoRandomBits int
				for _, option := range colDef.Options {
					if option.Tp == ast.ColumnOptionAutoRandom {
						hasAutoRandom = true
						autoRandomBits = option.AutoRandomBitLength
						switch {
						case autoRandomBits == types.UnspecifiedLength:
							autoRandomBits = autoid.DefaultAutoRandomBits
						case autoRandomBits <= 0 || autoRandomBits > autoid.MaxAutoRandomBits:
							canSetAutoRandom = false
						}
					}
					if option.Tp == ast.ColumnOptionAutoIncrement {
						canSetAutoRandom = false
					}
					if option.Tp == ast.ColumnOptionDefaultValue {
						canSetAutoRandom = false
					}
				}
				if hasAutoRandom && canSetAutoRandom {
					tblInfo.AutoRandomBits = uint64(autoRandomBits)
				}
			}
		}
	}
}

// ReadFirstNRowsByTableName reads the first N rows of data of an importing source table.
// It implements the PreRestoreInfoGetter interface.
func (p *PreRestoreInfoGetterImpl) ReadFirstNRowsByTableName(ctx context.Context, schemaName string, tableName string, n int) ([]string, [][]types.Datum, error) {
	mdTableMetaMap, ok := p.mdDBTableMetaMap[schemaName]
	if !ok {
		return nil, nil, errors.Errorf("cannot find the schema: %s", schemaName)
	}
	mdTableMeta, ok := mdTableMetaMap[tableName]
	if !ok {
		return nil, nil, errors.Errorf("cannot find the table: %s.%s", schemaName, tableName)
	}
	if len(mdTableMeta.DataFiles) <= 0 {
		return nil, [][]types.Datum{}, nil
	}
	return p.ReadFirstNRowsByFileMeta(ctx, mdTableMeta.DataFiles[0].FileMeta, n)
}

// ReadFirstNRowsByFileMeta reads the first N rows of an data file.
// It implements the PreRestoreInfoGetter interface.
func (p *PreRestoreInfoGetterImpl) ReadFirstNRowsByFileMeta(ctx context.Context, dataFileMeta mydump.SourceFileMeta, n int) ([]string, [][]types.Datum, error) {
	var (
		reader storage.ReadSeekCloser
		err    error
	)
	if dataFileMeta.Type == mydump.SourceTypeParquet {
		reader, err = mydump.OpenParquetReader(ctx, p.srcStorage, dataFileMeta.Path, dataFileMeta.FileSize)
	} else {
		reader, err = p.srcStorage.Open(ctx, dataFileMeta.Path)
	}
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var parser mydump.Parser
	blockBufSize := int64(p.cfg.Mydumper.ReadBlockSize)
	switch dataFileMeta.Type {
	case mydump.SourceTypeCSV:
		hasHeader := p.cfg.Mydumper.CSV.Header
		// Create a utf8mb4 convertor to encode and decode data with the charset of CSV files.
		charsetConvertor, err := mydump.NewCharsetConvertor(p.cfg.Mydumper.DataCharacterSet, p.cfg.Mydumper.DataInvalidCharReplace)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		parser, err = mydump.NewCSVParser(ctx, &p.cfg.Mydumper.CSV, reader, blockBufSize, p.ioWorkers, hasHeader, charsetConvertor)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	case mydump.SourceTypeSQL:
		parser = mydump.NewChunkParser(ctx, p.cfg.TiDB.SQLMode, reader, blockBufSize, p.ioWorkers)
	case mydump.SourceTypeParquet:
		parser, err = mydump.NewParquetParser(ctx, p.srcStorage, reader, dataFileMeta.Path)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	default:
		panic(fmt.Sprintf("unknown file type '%s'", dataFileMeta.Type))
	}
	defer parser.Close()

	rows := [][]types.Datum{}
	for i := 0; i < n; i++ {
		err := parser.ReadRow()
		if err != nil {
			if errors.Cause(err) != io.EOF {
				return nil, nil, errors.Trace(err)
			} else {
				break
			}
		}
		rows = append(rows, parser.LastRow().Row)
	}
	return parser.Columns(), rows, nil

}

// EstimateSourceDataSize estimates the datasize to generate during the import as well as some other sub-informaiton.
// It implements the PreRestoreInfoGetter interface.
func (p *PreRestoreInfoGetterImpl) EstimateSourceDataSize(ctx context.Context) (int64, int64, bool, error) {
	estimatedDataSizeToGenerate := int64(0)
	sourceTotalSize := int64(0)
	tableCount := 0
	unSortedBigTableCount := 0
	errMgr := errormanager.New(nil, p.cfg)
	dbInfos, err := p.GetAllTableStructures(ctx)
	if err != nil {
		return 0.0, 0.0, false, errors.Trace(err)
	}
	sysVars := p.GetTargetSysVariablesForImport(ctx)
	for _, db := range p.dbMetas {
		info, ok := dbInfos[db.Name]
		if !ok {
			continue
		}
		for _, tbl := range db.Tables {
			sourceTotalSize += tbl.TotalSize
			tableInfo, ok := info.Tables[tbl.Name]
			if ok {
				// Do not sample small table because there may a large number of small table and it will take a long
				// time to sample data for all of them.
				if isTiDBBackend(p.cfg) || tbl.TotalSize < int64(config.SplitRegionSize) {
					estimatedDataSizeToGenerate += tbl.TotalSize
					tbl.IndexRatio = 1.0
					tbl.IsRowOrdered = false
				} else {
					sampledIndexRatio, isRowOrderedFromSample, err := p.sampleDataFromTable(ctx, db.Name, tbl, tableInfo.Core, errMgr, sysVars)
					if err != nil {
						return 0.0, 0.0, false, errors.Trace(err)
					}
					tbl.IndexRatio = sampledIndexRatio
					tbl.IsRowOrdered = isRowOrderedFromSample

					if tbl.IndexRatio > 0 {
						estimatedDataSizeToGenerate += int64(float64(tbl.TotalSize) * tbl.IndexRatio)
					} else {
						// if sample data failed due to max-error, fallback to use source size
						estimatedDataSizeToGenerate += tbl.TotalSize
					}

					if tbl.TotalSize > int64(config.DefaultBatchSize)*2 && !tbl.IsRowOrdered {
						unSortedBigTableCount++
					}
				}
				tableCount += 1
			}
		}
	}

	return estimatedDataSizeToGenerate, sourceTotalSize, (unSortedBigTableCount > 0), nil

}

// sampleDataFromTable samples the source data file to get the extra data ratio for the index
// It returns:
// * the extra data ratio with index size accounted
// * is the sample data ordered by row
func (p *PreRestoreInfoGetterImpl) sampleDataFromTable(
	ctx context.Context,
	dbName string,
	tableMeta *mydump.MDTableMeta,
	tableInfo *model.TableInfo,
	errMgr *errormanager.ErrorManager,
	sysVars map[string]string,
) (float64, bool, error) {
	resultIndexRatio := 1.0
	isRowOrdered := false
	if len(tableMeta.DataFiles) == 0 {
		return resultIndexRatio, isRowOrdered, nil
	}
	sampleFile := tableMeta.DataFiles[0].FileMeta
	var reader storage.ReadSeekCloser
	var err error
	if sampleFile.Type == mydump.SourceTypeParquet {
		reader, err = mydump.OpenParquetReader(ctx, p.srcStorage, sampleFile.Path, sampleFile.FileSize)
	} else {
		reader, err = p.srcStorage.Open(ctx, sampleFile.Path)
	}
	if err != nil {
		return 0.0, false, errors.Trace(err)
	}
	idAlloc := kv.NewPanickingAllocators(0)
	tbl, err := tables.TableFromMeta(idAlloc, tableInfo)
	if err != nil {
		return 0.0, false, errors.Trace(err)
	}
	kvEncoder, err := p.kvEncBuilder.NewEncoder(tbl, &kv.SessionOptions{
		SQLMode:        p.cfg.TiDB.SQLMode,
		Timestamp:      0,
		SysVars:        sysVars,
		AutoRandomSeed: 0,
	})
	if err != nil {
		return 0.0, false, errors.Trace(err)
	}
	blockBufSize := int64(p.cfg.Mydumper.ReadBlockSize)

	var parser mydump.Parser
	switch tableMeta.DataFiles[0].FileMeta.Type {
	case mydump.SourceTypeCSV:
		hasHeader := p.cfg.Mydumper.CSV.Header
		// Create a utf8mb4 convertor to encode and decode data with the charset of CSV files.
		charsetConvertor, err := mydump.NewCharsetConvertor(p.cfg.Mydumper.DataCharacterSet, p.cfg.Mydumper.DataInvalidCharReplace)
		if err != nil {
			return 0.0, false, errors.Trace(err)
		}
		parser, err = mydump.NewCSVParser(ctx, &p.cfg.Mydumper.CSV, reader, blockBufSize, p.ioWorkers, hasHeader, charsetConvertor)
		if err != nil {
			return 0.0, false, errors.Trace(err)
		}
	case mydump.SourceTypeSQL:
		parser = mydump.NewChunkParser(ctx, p.cfg.TiDB.SQLMode, reader, blockBufSize, p.ioWorkers)
	case mydump.SourceTypeParquet:
		parser, err = mydump.NewParquetParser(ctx, p.srcStorage, reader, sampleFile.Path)
		if err != nil {
			return 0.0, false, errors.Trace(err)
		}
	default:
		panic(fmt.Sprintf("file '%s' with unknown source type '%s'", sampleFile.Path, sampleFile.Type.String()))
	}
	defer parser.Close()
	logTask := log.With(zap.String("table", tableMeta.Name)).Begin(zap.InfoLevel, "sample file")
	igCols, err := p.cfg.Mydumper.IgnoreColumns.GetIgnoreColumns(dbName, tableMeta.Name, p.cfg.Mydumper.CaseSensitive)
	if err != nil {
		return 0.0, false, errors.Trace(err)
	}

	initializedColumns := false
	var columnPermutation []int
	var kvSize uint64 = 0
	var rowSize uint64 = 0
	rowCount := 0
	dataKVs := p.kvEncBuilder.MakeEmptyRows()
	indexKVs := p.kvEncBuilder.MakeEmptyRows()
	lastKey := make([]byte, 0)
	isRowOrdered = true
outloop:
	for {
		offset, _ := parser.Pos()
		err = parser.ReadRow()
		columnNames := parser.Columns()

		switch errors.Cause(err) {
		case nil:
			if !initializedColumns {
				if len(columnPermutation) == 0 {
					columnPermutation, err = createColumnPermutation(columnNames, igCols.ColumnsMap(), tableInfo)
					if err != nil {
						return 0.0, false, errors.Trace(err)
					}
				}
				initializedColumns = true
			}
		case io.EOF:
			break outloop
		default:
			err = errors.Annotatef(err, "in file offset %d", offset)
			return 0.0, false, errors.Trace(err)
		}
		lastRow := parser.LastRow()
		rowCount += 1

		var dataChecksum, indexChecksum verification.KVChecksum
		kvs, encodeErr := kvEncoder.Encode(logTask.Logger, lastRow.Row, lastRow.RowID, columnPermutation, sampleFile.Path, offset)
		if encodeErr != nil {
			encodeErr = errMgr.RecordTypeError(ctx, log.L(), tableInfo.Name.O, sampleFile.Path, offset,
				"" /* use a empty string here because we don't actually record */, encodeErr)
			if encodeErr != nil {
				return 0.0, false, errors.Annotatef(encodeErr, "in file at offset %d", offset)
			}
			if rowCount < maxSampleRowCount {
				continue
			} else {
				break
			}
		}
		if isRowOrdered {
			kvs.ClassifyAndAppend(&dataKVs, &dataChecksum, &indexKVs, &indexChecksum)
			for _, kv := range kv.KvPairsFromRows(dataKVs) {
				if len(lastKey) == 0 {
					lastKey = kv.Key
				} else if bytes.Compare(lastKey, kv.Key) > 0 {
					isRowOrdered = false
					break
				}
			}
			dataKVs = dataKVs.Clear()
			indexKVs = indexKVs.Clear()
		}
		kvSize += kvs.Size()
		rowSize += uint64(lastRow.Length)
		parser.RecycleRow(lastRow)

		failpoint.Inject("mock-kv-size", func(val failpoint.Value) {
			kvSize += uint64(val.(int))
		})
		if rowSize > maxSampleDataSize || rowCount > maxSampleRowCount {
			break
		}
	}

	if rowSize > 0 && kvSize > rowSize {
		resultIndexRatio = float64(kvSize) / float64(rowSize)
	}
	log.L().Info("Sample source data", zap.String("table", tableMeta.Name), zap.Float64("IndexRatio", resultIndexRatio), zap.Bool("IsSourceOrder", isRowOrdered))
	return resultIndexRatio, isRowOrdered, nil
}

// GetReplicationConfig gets the replication config on the target.
// It implements the PreRestoreInfoGetter interface.
func (p *PreRestoreInfoGetterImpl) GetReplicationConfig(ctx context.Context) (*pdtypes.ReplicationConfig, error) {
	return p.targetInfoGetter.GetReplicationConfig(ctx)
}

// GetStorageInfo gets the storage information on the target.
// It implements the PreRestoreInfoGetter interface.
func (p *PreRestoreInfoGetterImpl) GetStorageInfo(ctx context.Context) (*pdtypes.StoresInfo, error) {
	return p.targetInfoGetter.GetStorageInfo(ctx)
}

// GetEmptyRegionsInfo gets the region information of all the empty regions on the target.
// It implements the PreRestoreInfoGetter interface.
func (p *PreRestoreInfoGetterImpl) GetEmptyRegionsInfo(ctx context.Context) (*pdtypes.RegionsInfo, error) {
	return p.targetInfoGetter.GetEmptyRegionsInfo(ctx)
}

// DoesTableContainData checks whether the specified table on the target DB contains data or not.
// It implements the PreRestoreInfoGetter interface.
func (p *PreRestoreInfoGetterImpl) DoesTableContainData(ctx context.Context, schemaName string, tableName string) (bool, error) {
	return p.targetInfoGetter.DoesTableContainData(ctx, schemaName, tableName)
}

// FetchRemoteTableModels fetches the table structures from the remote target.
// It implements the PreRestoreInfoGetter interface.
func (p *PreRestoreInfoGetterImpl) FetchRemoteTableModels(ctx context.Context, schemaName string) ([]*model.TableInfo, error) {
	return p.targetInfoGetter.FetchRemoteTableModels(ctx, schemaName)
}

// GetTargetSysVariablesForImport gets some important systam variables for importing on the target.
// It implements the PreRestoreInfoGetter interface.
// It has caching mechanism.
func (p *PreRestoreInfoGetterImpl) GetTargetSysVariablesForImport(ctx context.Context) map[string]string {
	if p.sysVars == nil {
		p.sysVars = p.targetInfoGetter.GetTargetSysVariablesForImport(ctx)
	}
	return p.sysVars
}
