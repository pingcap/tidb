// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importintotest

import (
	"fmt"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func (s *mockGCSSuite) TestExtraParamMaxRuntimeSlots() {
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/dxf/framework/storage/beforeSubmitTask",
		func(requiredSlots *int, params *proto.ExtraParams) {
			if kerneltype.IsClassic() {
				*requiredSlots = 16
			}
			params.MaxRuntimeSlots = 12
		},
	)
	if kerneltype.IsNextGen() {
		testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/dxf/importinto/afterPrepare",
			func(task *proto.Task) {
				task.RequiredSlots = 16
			},
		)
	}
	var callCnt int
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool/NewWorkerPool", func(numWorkers int) {
		s.EqualValues(12, numWorkers)
		callCnt++
	})
	s.prepareAndUseDB("extra_params")
	s.tk.MustExec("CREATE TABLE t (i INT PRIMARY KEY, s varchar(32));")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "snappy", Name: "t.01.csv.snappy"},
		Content:     s.getCompressedData(mydump.CompressionSnappy, []byte("1,test1\n2,test2")),
	})

	sortStorageURI := fmt.Sprintf("gs://snappy/temp?endpoint=%s&access-key=aaaaaa&secret-access-key=bbbbbb", gcsEndpoint)
	sql := fmt.Sprintf(`IMPORT INTO t FROM 'gs://snappy/t.*?endpoint=%s'
		WITH __force_merge_step, cloud_storage_uri='%s';`, gcsEndpoint, sortStorageURI)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows(
		"1 test1", "2 test2",
	))
	// encode/merge-sort/ingest step all create worker pools
	s.EqualValues(3, callCnt)
}

func (s *mockGCSSuite) TestStarterMaxImportDataSize() {
	if !kerneltype.IsNextGen() {
		s.T().Skip("starter deploy mode is only supported in nextgen")
	}

	originDeployMode := deploymode.Get()
	originGlobalConfig := config.GetGlobalConfig()
	require.NoError(s.T(), deploymode.Set(deploymode.Starter))
	config.UpdateGlobal(func(conf *config.Config) {
		conf.DeployMode = deploymode.Starter
		conf.StarterParams.MaxImportDataSize = 128
	})
	s.T().Cleanup(func() {
		config.StoreGlobalConfig(originGlobalConfig)
		require.NoError(s.T(), deploymode.Set(originDeployMode))
	})

	content := []byte("1,1\n2,2")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "starter-max-import-data-size-source", Name: "under.csv"},
		Content:     content,
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "starter-max-import-data-size-source", Name: "over.csv"},
		Content:     content,
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "starter-max-import-data-size-sort", Name: "seed"},
		Content:     []byte("seed"),
	})
	s.prepareAndUseDB("starter_max_import_data_size")
	s.tk.MustExec("create table under_limit (a int, b int);")
	s.tk.MustExec("create table over_limit (a int, b int);")

	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/executor/importer/amplifyRealSize", "return(10)")
	underLimitSQL := fmt.Sprintf(`IMPORT INTO under_limit
		FROM 'gs://starter-max-import-data-size-source/under.csv?endpoint=%s'
		WITH cloud_storage_uri='gs://starter-max-import-data-size-sort/under?endpoint=%s'`,
		gcsEndpoint,
		gcsEndpoint,
	)
	s.tk.MustQuery(underLimitSQL)
	s.tk.MustQuery("select * from under_limit order by a").Check(testkit.Rows("1 1", "2 2"))

	config.UpdateGlobal(func(conf *config.Config) {
		conf.DeployMode = deploymode.Starter
		conf.StarterParams.MaxImportDataSize = 64
	})
	overLimitSQL := fmt.Sprintf(`IMPORT INTO over_limit
		FROM 'gs://starter-max-import-data-size-source/over.csv?endpoint=%s'
		WITH cloud_storage_uri='gs://starter-max-import-data-size-sort/over?endpoint=%s'`,
		gcsEndpoint,
		gcsEndpoint,
	)
	err := s.tk.QueryToErr(overLimitSQL)
	require.ErrorContains(s.T(), err, "total real import data size 70B exceeds maximum import size limit 64B (total file size 7B)")
	s.tk.MustQuery("select * from over_limit").Check(testkit.Rows())
}
