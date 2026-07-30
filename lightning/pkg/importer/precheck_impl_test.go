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

package importer

import (
	"context"
	"fmt"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/lightning/pkg/importer/mock"
	ropts "github.com/pingcap/tidb/lightning/pkg/importer/opts"
	"github.com/pingcap/tidb/lightning/pkg/precheck"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

type precheckImplSuite struct {
	suite.Suite
	cfg           *config.Config
	mockSrc       *mock.ImportSource
	mockTarget    *mock.TargetInfo
	preInfoGetter PreImportInfoGetter
}

func TestPrecheckImplSuite(t *testing.T) {
	suite.Run(t, new(precheckImplSuite))
}

func (s *precheckImplSuite) SetupSuite() {
	cfg := &log.Config{}
	cfg.Adjust()
	log.InitLogger(cfg, "debug")
}

func (s *precheckImplSuite) SetupTest() {
	var err error
	s.Require().NoError(err)
	s.mockTarget = mock.NewTargetInfo()
	s.cfg = config.NewConfig()
	s.cfg.TikvImporter.Backend = config.BackendLocal
	s.Require().NoError(s.setMockImportData(nil))
}

func (s *precheckImplSuite) setMockImportData(mockDataMap map[string]*mock.DBSourceData) error {
	var err error
	s.mockSrc, err = mock.NewImportSource(mockDataMap)
	if err != nil {
		return err
	}
	s.preInfoGetter, err = NewPreImportInfoGetter(s.cfg, s.mockSrc.GetAllDBFileMetas(), s.mockSrc.GetStorage(), s.mockTarget, nil, nil, ropts.WithIgnoreDBNotExist(true))
	if err != nil {
		return err
	}
	return nil
}

func (s *precheckImplSuite) generateMockData(
	dbCount int,
	eachDBTableCount int,
	eachTableFileCount int,
	createSchemaSQLFunc func(dbName string, tblName string) string,
	sizeAndDataAndSuffixFunc func(dbID int, tblID int, fileID int) ([]byte, int, string),
) map[string]*mock.DBSourceData {
	result := make(map[string]*mock.DBSourceData)
	for dbID := 0; dbID < dbCount; dbID++ {
		dbName := fmt.Sprintf("db%d", dbID+1)
		tables := make(map[string]*mock.TableSourceData)
		for tblID := 0; tblID < eachDBTableCount; tblID++ {
			tblName := fmt.Sprintf("tbl%d", tblID+1)
			files := []*mock.SourceFile{}
			for fileID := 0; fileID < eachTableFileCount; fileID++ {
				fileData, totalSize, suffix := sizeAndDataAndSuffixFunc(dbID, tblID, fileID)
				mockSrcFile := &mock.SourceFile{
					FileName:  fmt.Sprintf("/%s/%s/data.%d.%s", dbName, tblName, fileID+1, suffix),
					Data:      fileData,
					TotalSize: totalSize,
				}
				files = append(files, mockSrcFile)
			}
			mockTblSrcData := &mock.TableSourceData{
				DBName:    dbName,
				TableName: tblName,
				SchemaFile: &mock.SourceFile{
					FileName: fmt.Sprintf("/%s/%s/%s.schema.sql", dbName, tblName, tblName),
					Data:     []byte(createSchemaSQLFunc(dbName, tblName)),
				},
				DataFiles: files,
			}
			tables[tblName] = mockTblSrcData
		}
		mockDBSrcData := &mock.DBSourceData{
			Name:   dbName,
			Tables: tables,
		}
		result[dbName] = mockDBSrcData
	}
	return result
}

func (s *precheckImplSuite) TestClusterResourceCheckBasic() {
	var (
		err    error
		ci     precheck.Checker
		result *precheck.CheckResult
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ci = NewClusterResourceCheckItem(s.preInfoGetter)
	s.Require().Equal(precheck.CheckTargetClusterSize, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().Equal(precheck.Warn, result.Severity)
	s.Require().True(result.Passed)

	testMockSrcData := s.generateMockData(1, 1, 1,
		func(dbName string, tblName string) string {
			return fmt.Sprintf("CREATE TABLE %s.%s ( id INTEGER PRIMARY KEY );", dbName, tblName)
		},
		func(dbID int, tblID int, fileID int) ([]byte, int, string) {
			return []byte(nil), 100, "csv"
		},
	)
	s.Require().NoError(s.setMockImportData(testMockSrcData))
	ci = NewClusterResourceCheckItem(s.preInfoGetter)
	s.Require().Equal(precheck.CheckTargetClusterSize, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Warn, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().False(result.Passed)

	s.mockTarget.StorageInfos = append(s.mockTarget.StorageInfos, mock.StorageInfo{
		TotalSize:     1000,
		UsedSize:      100,
		AvailableSize: 900,
	})
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(precheck.CheckTargetClusterSize, result.Item)
	s.Require().Equal(precheck.Warn, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().True(result.Passed)
}

func (s *precheckImplSuite) TestClusterVersionCheckBasic() {
	var (
		err    error
		ci     precheck.Checker
		result *precheck.CheckResult
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ci = NewClusterVersionCheckItem(s.preInfoGetter, s.mockSrc.GetAllDBFileMetas())
	s.Require().Equal(precheck.CheckTargetClusterVersion, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Critical, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().True(result.Passed)
}

func (s *precheckImplSuite) TestEmptyRegionCheckBasic() {
	var (
		err    error
		ci     precheck.Checker
		result *precheck.CheckResult
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ci = NewEmptyRegionCheckItem(s.preInfoGetter, s.mockSrc.GetAllDBFileMetas())
	s.Require().Equal(precheck.CheckTargetClusterEmptyRegion, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Warn, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().True(result.Passed)

	s.mockTarget.StorageInfos = append(s.mockTarget.StorageInfos,
		mock.StorageInfo{
			TotalSize:     1000,
			UsedSize:      100,
			AvailableSize: 900,
		},
		mock.StorageInfo{
			TotalSize:     1000,
			UsedSize:      100,
			AvailableSize: 900,
		},
	)
	s.mockTarget.EmptyRegionCountMap = map[uint64]int{
		1: 5000,
	}
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(precheck.CheckTargetClusterEmptyRegion, result.Item)
	s.Require().Equal(precheck.Warn, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().False(result.Passed)
}

func (s *precheckImplSuite) TestRegionDistributionCheckBasic() {
	var (
		err    error
		ci     precheck.Checker
		result *precheck.CheckResult
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ci = NewRegionDistributionCheckItem(s.preInfoGetter, s.mockSrc.GetAllDBFileMetas())
	s.Require().Equal(precheck.CheckTargetClusterRegionDist, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Warn, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().True(result.Passed)

	s.mockTarget.StorageInfos = append(s.mockTarget.StorageInfos,
		mock.StorageInfo{
			TotalSize:     1000,
			UsedSize:      100,
			AvailableSize: 900,
			RegionCount:   5000,
		},
		mock.StorageInfo{
			TotalSize:     1000,
			UsedSize:      100,
			AvailableSize: 900,
			RegionCount:   500,
		},
	)
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(precheck.CheckTargetClusterRegionDist, result.Item)
	s.Require().Equal(precheck.Warn, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().False(result.Passed)
}

func (s *precheckImplSuite) TestStoragePermissionCheckBasic() {
	var (
		err    error
		ci     precheck.Checker
		result *precheck.CheckResult
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.cfg.Mydumper.SourceDir = "file:///tmp"
	ci = NewStoragePermissionCheckItem(s.cfg)
	s.Require().Equal(precheck.CheckSourcePermission, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Critical, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().True(result.Passed)

	s.cfg.Mydumper.SourceDir = "s3://DUMMY-BUCKET/FAKE-DIR/FAKE-DIR2"
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(precheck.CheckSourcePermission, result.Item)
	s.Require().Equal(precheck.Critical, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().False(result.Passed)
}

func (s *precheckImplSuite) TestLargeFileCheckBasic() {
	var (
		err    error
		ci     precheck.Checker
		result *precheck.CheckResult
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ci = NewLargeFileCheckItem(s.cfg, s.mockSrc.GetAllDBFileMetas())
	s.Require().Equal(precheck.CheckLargeDataFile, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Warn, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().True(result.Passed)

	testMockSrcData := s.generateMockData(1, 1, 1,
		func(dbName string, tblName string) string {
			return fmt.Sprintf("CREATE TABLE %s.%s ( id INTEGER PRIMARY KEY );", dbName, tblName)
		},
		func(dbID int, tblID int, fileID int) ([]byte, int, string) {
			return []byte(nil), 20 * units.GB, "csv"
		},
	)
	s.Require().NoError(s.setMockImportData(testMockSrcData))
	ci = NewLargeFileCheckItem(s.cfg, s.mockSrc.GetAllDBFileMetas())
	s.Require().Equal(precheck.CheckLargeDataFile, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Warn, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().False(result.Passed)
}

func (s *precheckImplSuite) TestLocalDiskPlacementCheckBasic() {
	var (
		err    error
		ci     precheck.Checker
		result *precheck.CheckResult
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.cfg.Mydumper.SourceDir = "file:///dev/"
	s.cfg.TikvImporter.SortedKVDir = "/tmp/"
	ci = NewLocalDiskPlacementCheckItem(s.cfg)
	s.Require().Equal(precheck.CheckLocalDiskPlacement, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Warn, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().True(result.Passed)

	s.cfg.Mydumper.SourceDir = "file:///tmp/"
	s.cfg.TikvImporter.SortedKVDir = "/tmp/"
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(precheck.CheckLocalDiskPlacement, result.Item)
	s.Require().Equal(precheck.Warn, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().False(result.Passed)
}

func (s *precheckImplSuite) TestLocalTempKVDirCheckBasic() {
	var (
		err    error
		ci     precheck.Checker
		result *precheck.CheckResult
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.cfg.TikvImporter.SortedKVDir = "/tmp/"
	ci = NewLocalTempKVDirCheckItem(s.cfg, s.preInfoGetter, s.mockSrc.GetAllDBFileMetas())
	s.Require().Equal(precheck.CheckLocalTempKVDir, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Critical, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().True(result.Passed)

	testMockSrcData := s.generateMockData(1, 1, 1,
		func(dbName string, tblName string) string {
			return fmt.Sprintf("CREATE TABLE %s.%s ( id INTEGER PRIMARY KEY );", dbName, tblName)
		},
		func(dbID int, tblID int, fileID int) ([]byte, int, string) {
			return []byte(nil), 10 * units.TB, "csv"
		},
	)
	s.Require().NoError(s.setMockImportData(testMockSrcData))
	ci = NewLocalTempKVDirCheckItem(s.cfg, s.preInfoGetter, s.mockSrc.GetAllDBFileMetas())
	s.Require().Equal(precheck.CheckLocalTempKVDir, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Critical, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().False(result.Passed)
}

func (s *precheckImplSuite) TestCheckpointCheckBasic() {
	var (
		err    error
		ci     precheck.Checker
		result *precheck.CheckResult
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cpdb := checkpoints.NewNullCheckpointsDB()
	s.cfg.Checkpoint.Enable = true
	ci = NewCheckpointCheckItem(s.cfg, s.preInfoGetter, s.mockSrc.GetAllDBFileMetas(), cpdb)
	s.Require().Equal(precheck.CheckCheckpoints, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Critical, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().True(result.Passed)
}

func (s *precheckImplSuite) TestSchemaCheckBasic() {
	var (
		err    error
		ci     precheck.Checker
		result *precheck.CheckResult
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.cfg.Mydumper.CSV.Header = true

	const testCSVData01 string = `ival,sval
111,"aaa"
222,"bbb"
`
	const testCSVData02 string = `xval,sval
111,"aaa"
222,"bbb"
`
	testMockSrcData := s.generateMockData(1, 1, 1,
		func(dbName string, tblName string) string {
			return fmt.Sprintf("CREATE TABLE %s.%s ( id INTEGER PRIMARY KEY AUTO_INCREMENT, ival INTEGER, sval VARCHAR(64) );", dbName, tblName)
		},
		func(dbID int, tblID int, fileID int) ([]byte, int, string) {
			return []byte(testCSVData01), 100, "csv"
		},
	)
	s.Require().NoError(s.setMockImportData(testMockSrcData))
	ci = NewSchemaCheckItem(s.cfg, s.preInfoGetter, s.mockSrc.GetAllDBFileMetas(), nil)
	s.Require().Equal(precheck.CheckSourceSchemaValid, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Critical, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().True(result.Passed)

	testMockSrcData = s.generateMockData(1, 1, 1,
		func(dbName string, tblName string) string {
			return fmt.Sprintf("CREATE TABLE %s.%s ( id INTEGER PRIMARY KEY AUTO_INCREMENT, ival INTEGER NOT NULL, sval VARCHAR(64) NOT NULL);", dbName, tblName)
		},
		func(dbID int, tblID int, fileID int) ([]byte, int, string) {
			return []byte(testCSVData02), 100, "csv"
		},
	)
	s.Require().NoError(s.setMockImportData(testMockSrcData))
	ci = NewSchemaCheckItem(s.cfg, s.preInfoGetter, s.mockSrc.GetAllDBFileMetas(), nil)
	s.Require().Equal(precheck.CheckSourceSchemaValid, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Critical, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().False(result.Passed)
}

func (s *precheckImplSuite) TestCSVHeaderCheckBasic() {
	var (
		err    error
		ci     precheck.Checker
		result *precheck.CheckResult
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.cfg.Mydumper.CSV.Header = false

	const testCSVData01 string = `111,"aaa"
222,"bbb"
`
	const testCSVData02 string = `ival,sval
111,"aaa"
222,"bbb"
`
	testMockSrcData := s.generateMockData(1, 1, 1,
		func(dbName string, tblName string) string {
			return fmt.Sprintf("CREATE TABLE %s.%s ( id INTEGER PRIMARY KEY AUTO_INCREMENT, ival INTEGER, sval VARCHAR(64) );", dbName, tblName)
		},
		func(dbID int, tblID int, fileID int) ([]byte, int, string) {
			return []byte(testCSVData01), 100, "csv"
		},
	)
	s.Require().NoError(s.setMockImportData(testMockSrcData))
	ci = NewCSVHeaderCheckItem(s.cfg, s.preInfoGetter, s.mockSrc.GetAllDBFileMetas())
	s.Require().Equal(precheck.CheckCSVHeader, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Critical, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().True(result.Passed)

	testMockSrcData = s.generateMockData(1, 1, 1,
		func(dbName string, tblName string) string {
			return fmt.Sprintf("CREATE TABLE %s.%s ( id INTEGER PRIMARY KEY AUTO_INCREMENT, ival INTEGER, sval VARCHAR(64) );", dbName, tblName)
		},
		func(dbID int, tblID int, fileID int) ([]byte, int, string) {
			return []byte(testCSVData02), 100, "csv"
		},
	)
	s.Require().NoError(s.setMockImportData(testMockSrcData))
	ci = NewCSVHeaderCheckItem(s.cfg, s.preInfoGetter, s.mockSrc.GetAllDBFileMetas())
	s.Require().Equal(precheck.CheckCSVHeader, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Critical, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().False(result.Passed)
}

func (s *precheckImplSuite) TestTableEmptyCheckBasic() {
	var (
		err    error
		ci     precheck.Checker
		result *precheck.CheckResult
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testMockSrcData := s.generateMockData(1, 1, 1,
		func(dbName string, tblName string) string {
			return fmt.Sprintf("CREATE TABLE %s.%s ( id INTEGER PRIMARY KEY AUTO_INCREMENT, ival INTEGER, sval VARCHAR(64) );", dbName, tblName)
		},
		func(dbID int, tblID int, fileID int) ([]byte, int, string) {
			return []byte(nil), 100, "csv"
		},
	)
	s.Require().NoError(s.setMockImportData(testMockSrcData))
	ci = NewTableEmptyCheckItem(s.cfg, s.preInfoGetter, s.mockSrc.GetAllDBFileMetas(), nil)
	s.Require().Equal(precheck.CheckTargetTableEmpty, ci.GetCheckItemID())
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Critical, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().True(result.Passed)

	s.mockTarget.SetTableInfo("db1", "tbl1", &mock.TableInfo{
		RowCount: 100,
	})
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(precheck.CheckTargetTableEmpty, result.Item)
	s.Require().Equal(precheck.Critical, result.Severity)
	s.T().Logf("check result message: %s", result.Message)
	s.Require().False(result.Passed)
}

func (s *precheckImplSuite) TestCDCPITRCheckItem() {
	integration.BeforeTestExternal(s.T())
	testEtcdCluster := integration.NewClusterV3(s.T(), &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(s.T())

	ctx := context.Background()
	cfg := &config.Config{
		TikvImporter: config.TikvImporter{
			Backend: config.BackendLocal,
		},
	}
	ci := NewCDCPITRCheckItem(cfg, nil)
	checker := ci.(*CDCPITRCheckItem)
	checker.etcdCli = testEtcdCluster.RandClient()
	result, err := ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().Equal(ci.GetCheckItemID(), result.Item)
	s.Require().Equal(precheck.Critical, result.Severity)
	s.Require().True(result.Passed)
	s.Require().Equal("no CDC or PiTR task found", result.Message)

	cli := testEtcdCluster.RandClient()
	brCli := streamhelper.NewMetaDataClient(cli)
	backend, _ := storage.ParseBackend("noop://", nil)
	taskInfo, err := streamhelper.NewTaskInfo("br_name").
		FromTS(1).
		UntilTS(1000).
		WithTableFilter("*.*", "!mysql").
		ToStorage(backend).
		Check()
	s.Require().NoError(err)
	err = brCli.PutTask(ctx, *taskInfo)
	s.Require().NoError(err)
	checkEtcdPut := func(key string, vals ...string) {
		val := ""
		if len(vals) == 1 {
			val = vals[0]
		}
		_, err := cli.Put(ctx, key, val)
		s.Require().NoError(err)
	}
	// TiCDC >= v6.2
	checkEtcdPut("/tidb/cdc/default/__cdc_meta__/capture/3ecd5c98-0148-4086-adfd-17641995e71f")
	checkEtcdPut("/tidb/cdc/default/__cdc_meta__/meta/meta-version")
	checkEtcdPut("/tidb/cdc/default/__cdc_meta__/meta/ticdc-delete-etcd-key-count")
	checkEtcdPut("/tidb/cdc/default/__cdc_meta__/owner/22318498f4dd6639")
	checkEtcdPut(
		"/tidb/cdc/default/default/changefeed/info/test",
		`{"upstream-id":7195826648407968958,"namespace":"default","changefeed-id":"test-1","sink-uri":"mysql://root@127.0.0.1:3306?time-zone=","create-time":"2023-02-03T15:23:34.773768+08:00","start-ts":439198420741652483,"target-ts":0,"admin-job-type":0,"sort-engine":"unified","sort-dir":"","config":{"memory-quota":1073741824,"case-sensitive":true,"enable-old-value":true,"force-replicate":false,"check-gc-safe-point":true,"enable-sync-point":false,"bdr-mode":false,"sync-point-interval":600000000000,"sync-point-retention":86400000000000,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"event-filters":null},"mounter":{"worker-num":16},"sink":{"transaction-atomicity":"","protocol":"","dispatchers":null,"csv":{"delimiter":",","quote":"\"","null":"\\N","include-commit-ts":false},"column-selectors":null,"schema-registry":"","encoder-concurrency":16,"terminator":"\r\n","date-separator":"none","enable-partition-separator":false},"consistent":{"level":"none","max-log-size":64,"flush-interval":2000,"storage":""},"scheduler":{"region-per-span":0}},"state":"normal","error":null,"creator-version":"v6.5.0-master-dirty"}`,
	)
	checkEtcdPut(
		"/tidb/cdc/default/default/changefeed/info/test-1",
		`{"upstream-id":7195826648407968958,"namespace":"default","changefeed-id":"test-1","sink-uri":"mysql://root@127.0.0.1:3306?time-zone=","create-time":"2023-02-03T15:23:34.773768+08:00","start-ts":439198420741652483,"target-ts":0,"admin-job-type":0,"sort-engine":"unified","sort-dir":"","config":{"memory-quota":1073741824,"case-sensitive":true,"enable-old-value":true,"force-replicate":false,"check-gc-safe-point":true,"enable-sync-point":false,"bdr-mode":false,"sync-point-interval":600000000000,"sync-point-retention":86400000000000,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"event-filters":null},"mounter":{"worker-num":16},"sink":{"transaction-atomicity":"","protocol":"","dispatchers":null,"csv":{"delimiter":",","quote":"\"","null":"\\N","include-commit-ts":false},"column-selectors":null,"schema-registry":"","encoder-concurrency":16,"terminator":"\r\n","date-separator":"none","enable-partition-separator":false},"consistent":{"level":"none","max-log-size":64,"flush-interval":2000,"storage":""},"scheduler":{"region-per-span":0}},"state":"finished","error":null,"creator-version":"v6.5.0-master-dirty"}`,
	)
	checkEtcdPut("/tidb/cdc/default/default/changefeed/status/test")
	checkEtcdPut("/tidb/cdc/default/default/changefeed/status/test-1")
	checkEtcdPut("/tidb/cdc/default/default/task/position/3ecd5c98-0148-4086-adfd-17641995e71f/test-1")
	checkEtcdPut("/tidb/cdc/default/default/upstream/7168358383033671922")

	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().False(result.Passed)
	s.Require().Equal("found PiTR log streaming task(s): [br_name],\n"+
		"found CDC changefeed(s): cluster/namespace: default/default changefeed(s): [test], \n"+
		"local backend is not compatible with them. Please switch to tidb backend then try again.",
		result.Message)

	_, err = cli.Delete(ctx, "/tidb/cdc/", clientv3.WithPrefix())
	s.Require().NoError(err)

	// TiCDC <= v6.1
	checkEtcdPut("/tidb/cdc/capture/f14cb04d-5ba1-410e-a59b-ccd796920e9d")
	checkEtcdPut(
		"/tidb/cdc/changefeed/info/test",
		`{"upstream-id":7195826648407968958,"namespace":"default","changefeed-id":"test-1","sink-uri":"mysql://root@127.0.0.1:3306?time-zone=","create-time":"2023-02-03T15:23:34.773768+08:00","start-ts":439198420741652483,"target-ts":0,"admin-job-type":0,"sort-engine":"unified","sort-dir":"","config":{"memory-quota":1073741824,"case-sensitive":true,"enable-old-value":true,"force-replicate":false,"check-gc-safe-point":true,"enable-sync-point":false,"bdr-mode":false,"sync-point-interval":600000000000,"sync-point-retention":86400000000000,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"event-filters":null},"mounter":{"worker-num":16},"sink":{"transaction-atomicity":"","protocol":"","dispatchers":null,"csv":{"delimiter":",","quote":"\"","null":"\\N","include-commit-ts":false},"column-selectors":null,"schema-registry":"","encoder-concurrency":16,"terminator":"\r\n","date-separator":"none","enable-partition-separator":false},"consistent":{"level":"none","max-log-size":64,"flush-interval":2000,"storage":""},"scheduler":{"region-per-span":0}},"state":"stopped","error":null,"creator-version":"v6.5.0-master-dirty"}`,
	)
	checkEtcdPut("/tidb/cdc/job/test")
	checkEtcdPut("/tidb/cdc/owner/223184ad80a88b0b")
	checkEtcdPut("/tidb/cdc/task/position/f14cb04d-5ba1-410e-a59b-ccd796920e9d/test")

	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().False(result.Passed)
	s.Require().Equal("found PiTR log streaming task(s): [br_name],\n"+
		"found CDC changefeed(s): cluster/namespace: <nil> changefeed(s): [test], \n"+
		"local backend is not compatible with them. Please switch to tidb backend then try again.",
		result.Message)

	checker.cfg.TikvImporter.Backend = config.BackendTiDB
	result, err = ci.Check(ctx)
	s.Require().NoError(err)
	s.Require().True(result.Passed)
	s.Require().Equal("TiDB Lightning is not using local backend, skip this check", result.Message)
}
