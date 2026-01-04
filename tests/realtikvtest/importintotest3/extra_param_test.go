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
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
)

func (s *mockGCSSuite) TestExtraParamMaxRuntimeSlots() {
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/dxf/framework/storage/beforeSubmitTask",
		func(requiredSlots *int, params *proto.ExtraParams) {
			*requiredSlots = 16
			params.MaxRuntimeSlots = 12
		},
	)
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
