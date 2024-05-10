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

package importintotest

import (
	"context"
	"fmt"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	brpb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func (s *mockGCSSuite) TestPreCheckTotalFileSize0() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "precheck-file-empty",
			Name:       "empty.csv",
		},
		Content: []byte(``),
	})
	s.prepareAndUseDB("load_data")
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint primary key, b varchar(100), c int);")
	s.tk.MustExec("insert into t values(9, 'test9', 99);")
	sql := fmt.Sprintf(`IMPORT INTO t FROM 'gs://precheck-file-empty/non-exist/file-*.csv?endpoint=%s'`, gcsEndpoint)
	err := s.tk.QueryToErr(sql)
	require.ErrorIs(s.T(), err, exeerrors.ErrLoadDataPreCheckFailed)

	sql = fmt.Sprintf(`IMPORT INTO t FROM 'gs://precheck-file-empty/empty.csv?endpoint=%s'`, gcsEndpoint)
	err = s.tk.QueryToErr(sql)
	require.ErrorIs(s.T(), err, exeerrors.ErrLoadDataPreCheckFailed)
}

func (s *mockGCSSuite) TestPreCheckTableNotEmpty() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "precheck-tbl-empty",
			Name:       "file.csv",
		},
		Content: []byte(`1,test1,11
2,test2,22
3,test3,33`),
	})
	s.prepareAndUseDB("load_data")
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint primary key, b varchar(100), c int);")
	s.tk.MustExec("insert into t values(9, 'test9', 99);")
	sql := fmt.Sprintf(`IMPORT INTO t FROM 'gs://precheck-tbl-empty/file.csv?endpoint=%s'`, gcsEndpoint)
	err := s.tk.QueryToErr(sql)
	require.ErrorIs(s.T(), err, exeerrors.ErrLoadDataPreCheckFailed)
}

func (s *mockGCSSuite) TestPreCheckCDCPiTRTasks() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "precheck-cdc-pitr", Name: "file.csv"},
		Content:     []byte(`1,test1,11`),
	})
	s.prepareAndUseDB("load_data")
	s.tk.MustExec("drop table if exists t;")
	s.tk.MustExec("create table t (a bigint primary key, b varchar(100), c int);")
	s.tk.MustExec("create table dst (a bigint primary key, b varchar(100), c int);")

	client, err := importer.GetEtcdClient()
	s.NoError(err)
	s.T().Cleanup(func() {
		_ = client.Close()
	})

	// Note: br has a background worker listening on this key, after this test
	// there'll be keys like "/tidb/br-stream/checkpoint/dummy-task/store/1" left in etcd
	// see OwnerManagerForLogBackup for more details
	pitrKey := streamhelper.PrefixOfTask() + "dummy-task"
	pitrTaskInfo := brpb.StreamBackupTaskInfo{Name: "dummy-task"}
	data, err := pitrTaskInfo.Marshal()
	s.NoError(err)
	_, err = client.GetClient().Put(context.Background(), pitrKey, string(data))
	s.NoError(err)
	s.T().Cleanup(func() {
		_, err2 := client.GetClient().Delete(context.Background(), pitrKey)
		s.NoError(err2)
	})
	sql := fmt.Sprintf(`IMPORT INTO t FROM 'gs://precheck-cdc-pitr/file.csv?endpoint=%s'`, gcsEndpoint)
	err = s.tk.QueryToErr(sql)
	log.Error("error", zap.Error(err))
	s.ErrorIs(err, exeerrors.ErrLoadDataPreCheckFailed)
	s.ErrorContains(err, "found PiTR log streaming task(s): [dummy-task],")
	// disable precheck
	s.tk.MustQuery(sql + " WITH disable_precheck")
	s.tk.MustQuery("select * from t").Check(testkit.Rows("1 test1 11"))

	// test import from select
	err = s.tk.ExecToErr("import into dst from select * from t")
	log.Error("error", zap.Error(err))
	s.ErrorIs(err, exeerrors.ErrLoadDataPreCheckFailed)
	s.ErrorContains(err, "found PiTR log streaming task(s): [dummy-task],")
	// disable precheck
	s.tk.MustExec("import into dst from select * from t with disable_precheck")
	s.tk.MustQuery("select * from dst").Check(testkit.Rows("1 test1 11"))

	_, err2 := client.GetClient().Delete(context.Background(), pitrKey)
	s.NoError(err2)
	cdcKey := "/tidb/cdc/cluster-123/test/changefeed/info/feed-test"
	_, err = client.GetClient().Put(context.Background(), cdcKey, `{"state": "normal"}`)
	s.NoError(err)
	s.T().Cleanup(func() {
		_, err2 := client.GetClient().Delete(context.Background(), cdcKey)
		s.NoError(err2)
	})
	s.tk.MustExec("truncate table t")
	err = s.tk.QueryToErr(sql)
	s.ErrorIs(err, exeerrors.ErrLoadDataPreCheckFailed)
	s.ErrorContains(err, "found CDC changefeed(s): cluster/namespace: cluster-123/test changefeed(s): [feed-test]")
}
