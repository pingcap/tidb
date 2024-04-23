// Copyright 2023 PingCAP, Inc.
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

package mock

import (
	"context"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	ropts "github.com/pingcap/tidb/lightning/pkg/importer/opts"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/filter"
	pdhttp "github.com/tikv/pd/client/http"
)

// SourceFile defines a mock source file.
type SourceFile struct {
	FileName  string
	Data      []byte
	TotalSize int
}

// TableSourceData defines a mock source information for a table.
type TableSourceData struct {
	DBName     string
	TableName  string
	SchemaFile *SourceFile
	DataFiles  []*SourceFile
}

// DBSourceData defines a mock source information for a database.
type DBSourceData struct {
	Name   string
	Tables map[string]*TableSourceData
}

// ImportSource defines a mock import source
type ImportSource struct {
	dbSrcDataMap  map[string]*DBSourceData
	dbFileMetaMap map[string]*mydump.MDDatabaseMeta
	srcStorage    storage.ExternalStorage
}

// NewImportSource creates a ImportSource object.
func NewImportSource(dbSrcDataMap map[string]*DBSourceData) (*ImportSource, error) {
	ctx := context.Background()
	dbFileMetaMap := make(map[string]*mydump.MDDatabaseMeta)
	mapStore := storage.NewMemStorage()
	for dbName, dbData := range dbSrcDataMap {
		dbFileInfo := mydump.FileInfo{
			TableName: filter.Table{
				Schema: dbName,
			},
			FileMeta: mydump.SourceFileMeta{Type: mydump.SourceTypeSchemaSchema},
		}
		dbMeta := mydump.NewMDDatabaseMeta("binary")
		dbMeta.Name = dbName
		dbMeta.SchemaFile = dbFileInfo
		dbMeta.Tables = []*mydump.MDTableMeta{}
		for tblName, tblData := range dbData.Tables {
			tblMeta := mydump.NewMDTableMeta("binary")
			tblMeta.DB = dbName
			tblMeta.Name = tblName
			compression := mydump.CompressionNone
			if strings.HasSuffix(tblData.SchemaFile.FileName, ".gz") {
				compression = mydump.CompressionGZ
			}
			tblMeta.SchemaFile = mydump.FileInfo{
				TableName: filter.Table{
					Schema: dbName,
					Name:   tblName,
				},
				FileMeta: mydump.SourceFileMeta{
					Path:        tblData.SchemaFile.FileName,
					Type:        mydump.SourceTypeTableSchema,
					Compression: compression,
				},
			}
			tblMeta.DataFiles = []mydump.FileInfo{}
			if err := mapStore.WriteFile(ctx, tblData.SchemaFile.FileName, tblData.SchemaFile.Data); err != nil {
				return nil, errors.Trace(err)
			}
			totalFileSize := 0
			for _, tblDataFile := range tblData.DataFiles {
				fileSize := tblDataFile.TotalSize
				if fileSize == 0 {
					fileSize = len(tblDataFile.Data)
				}
				totalFileSize += fileSize
				fileInfo := mydump.FileInfo{
					TableName: filter.Table{
						Schema: dbName,
						Name:   tblName,
					},
					FileMeta: mydump.SourceFileMeta{
						Path:     tblDataFile.FileName,
						FileSize: int64(fileSize),
						RealSize: int64(fileSize),
					},
				}
				fileName := tblDataFile.FileName
				if strings.HasSuffix(fileName, ".gz") {
					fileName = strings.TrimSuffix(tblDataFile.FileName, ".gz")
					fileInfo.FileMeta.Compression = mydump.CompressionGZ
				}
				switch {
				case strings.HasSuffix(fileName, ".csv"):
					fileInfo.FileMeta.Type = mydump.SourceTypeCSV
				case strings.HasSuffix(fileName, ".sql"):
					fileInfo.FileMeta.Type = mydump.SourceTypeSQL
				case strings.HasSuffix(fileName, ".parquet"):
					fileInfo.FileMeta.Type = mydump.SourceTypeParquet
				default:
					return nil, errors.Errorf("unsupported file type: %s", tblDataFile.FileName)
				}
				tblMeta.DataFiles = append(tblMeta.DataFiles, fileInfo)
				if err := mapStore.WriteFile(ctx, tblDataFile.FileName, tblDataFile.Data); err != nil {
					return nil, errors.Trace(err)
				}
			}
			tblMeta.TotalSize = int64(totalFileSize)
			dbMeta.Tables = append(dbMeta.Tables, tblMeta)
		}
		dbFileMetaMap[dbName] = dbMeta
	}
	return &ImportSource{
		dbSrcDataMap:  dbSrcDataMap,
		dbFileMetaMap: dbFileMetaMap,
		srcStorage:    mapStore,
	}, nil
}

// GetStorage gets the External Storage object on the mock source.
func (m *ImportSource) GetStorage() storage.ExternalStorage {
	return m.srcStorage
}

// GetDBMetaMap gets the Mydumper database metadata map on the mock source.
func (m *ImportSource) GetDBMetaMap() map[string]*mydump.MDDatabaseMeta {
	return m.dbFileMetaMap
}

// GetAllDBFileMetas gets all the Mydumper database metadatas on the mock source.
func (m *ImportSource) GetAllDBFileMetas() []*mydump.MDDatabaseMeta {
	result := make([]*mydump.MDDatabaseMeta, len(m.dbFileMetaMap))
	i := 0
	for _, dbMeta := range m.dbFileMetaMap {
		result[i] = dbMeta
		i++
	}
	return result
}

// StorageInfo defines the storage information for a mock target.
type StorageInfo struct {
	TotalSize     uint64
	UsedSize      uint64
	AvailableSize uint64
	RegionCount   int
}

// TableInfo defines a mock table structure information for a mock target.
type TableInfo struct {
	RowCount   int
	TableModel *model.TableInfo
}

// TargetInfo defines a mock target information.
type TargetInfo struct {
	MaxReplicasPerRegion int
	EmptyRegionCountMap  map[uint64]int
	StorageInfos         []StorageInfo
	sysVarMap            map[string]string
	dbTblInfoMap         map[string]map[string]*TableInfo
}

// NewTargetInfo creates a TargetInfo object.
func NewTargetInfo() *TargetInfo {
	return &TargetInfo{
		StorageInfos: []StorageInfo{},
		sysVarMap:    make(map[string]string),
		dbTblInfoMap: make(map[string]map[string]*TableInfo),
	}
}

// SetSysVar sets the system variables of the mock target.
func (t *TargetInfo) SetSysVar(key string, value string) {
	t.sysVarMap[key] = value
}

// SetTableInfo sets the table structure information of the mock target.
func (t *TargetInfo) SetTableInfo(schemaName string, tableName string, tblInfo *TableInfo) {
	if _, ok := t.dbTblInfoMap[schemaName]; !ok {
		t.dbTblInfoMap[schemaName] = make(map[string]*TableInfo)
	}
	t.dbTblInfoMap[schemaName][tableName] = tblInfo
}

// FetchRemoteDBModels implements the TargetInfoGetter interface.
func (t *TargetInfo) FetchRemoteDBModels(_ context.Context) ([]*model.DBInfo, error) {
	resultInfos := []*model.DBInfo{}
	for dbName := range t.dbTblInfoMap {
		resultInfos = append(resultInfos, &model.DBInfo{Name: model.NewCIStr(dbName)})
	}
	return resultInfos, nil
}

// FetchRemoteTableModels fetches the table structures from the remote target.
// It implements the TargetInfoGetter interface.
func (t *TargetInfo) FetchRemoteTableModels(_ context.Context, schemaName string) ([]*model.TableInfo, error) {
	resultInfos := []*model.TableInfo{}
	tblMap, ok := t.dbTblInfoMap[schemaName]
	if !ok {
		dbNotExistErr := dbterror.ClassSchema.NewStd(errno.ErrBadDB).FastGenByArgs(schemaName)
		return nil, errors.Errorf("get xxxxxx http status code != 200, message %s", dbNotExistErr.Error())
	}
	for _, tblInfo := range tblMap {
		resultInfos = append(resultInfos, tblInfo.TableModel)
	}
	return resultInfos, nil
}

// GetTargetSysVariablesForImport gets some important systam variables for importing on the target.
// It implements the TargetInfoGetter interface.
func (t *TargetInfo) GetTargetSysVariablesForImport(_ context.Context, _ ...ropts.GetPreInfoOption) map[string]string {
	result := make(map[string]string)
	for k, v := range t.sysVarMap {
		result[k] = v
	}
	return result
}

// GetMaxReplica implements the TargetInfoGetter interface.
func (t *TargetInfo) GetMaxReplica(context.Context) (uint64, error) {
	replCount := t.MaxReplicasPerRegion
	if replCount <= 0 {
		replCount = 1
	}
	return uint64(replCount), nil
}

// GetStorageInfo gets the storage information on the target.
// It implements the TargetInfoGetter interface.
func (t *TargetInfo) GetStorageInfo(_ context.Context) (*pdhttp.StoresInfo, error) {
	resultStoreInfos := make([]pdhttp.StoreInfo, len(t.StorageInfos))
	for i, storeInfo := range t.StorageInfos {
		resultStoreInfos[i] = pdhttp.StoreInfo{
			Store: pdhttp.MetaStore{
				ID:        int64(i + 1),
				StateName: "Up",
			},
			Status: pdhttp.StoreStatus{
				Capacity:    units.BytesSize(float64(storeInfo.TotalSize)),
				Available:   units.BytesSize(float64(storeInfo.AvailableSize)),
				RegionSize:  int64(storeInfo.UsedSize),
				RegionCount: int64(storeInfo.RegionCount),
			},
		}
	}
	return &pdhttp.StoresInfo{
		Count:  len(resultStoreInfos),
		Stores: resultStoreInfos,
	}, nil
}

// GetEmptyRegionsInfo gets the region information of all the empty regions on the target.
// It implements the TargetInfoGetter interface.
func (t *TargetInfo) GetEmptyRegionsInfo(_ context.Context) (*pdhttp.RegionsInfo, error) {
	totalEmptyRegions := []pdhttp.RegionInfo{}
	totalEmptyRegionCount := 0
	for storeID, storeEmptyRegionCount := range t.EmptyRegionCountMap {
		regions := make([]pdhttp.RegionInfo, storeEmptyRegionCount)
		for i := 0; i < storeEmptyRegionCount; i++ {
			regions[i] = pdhttp.RegionInfo{
				Peers: []pdhttp.RegionPeer{
					{
						StoreID: int64(storeID),
					},
				},
			}
		}
		totalEmptyRegions = append(totalEmptyRegions, regions...)
		totalEmptyRegionCount += storeEmptyRegionCount
	}
	return &pdhttp.RegionsInfo{
		Count:   int64(totalEmptyRegionCount),
		Regions: totalEmptyRegions,
	}, nil
}

// IsTableEmpty checks whether the specified table on the target DB contains data or not.
// It implements the TargetInfoGetter interface.
func (t *TargetInfo) IsTableEmpty(_ context.Context, schemaName string, tableName string) (*bool, error) {
	var result bool
	tblInfoMap, ok := t.dbTblInfoMap[schemaName]
	if !ok {
		result = true
		return &result, nil
	}
	tblInfo, ok := tblInfoMap[tableName]
	if !ok {
		result = true
		return &result, nil
	}
	result = tblInfo.RowCount == 0
	return &result, nil
}

// CheckVersionRequirements performs the check whether the target satisfies the version requirements.
// It implements the TargetInfoGetter interface.
func (*TargetInfo) CheckVersionRequirements(_ context.Context) error {
	return nil
}
