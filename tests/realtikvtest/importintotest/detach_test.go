// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"strconv"
	"sync"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

type detachedCase struct {
	tableCols        string
	physicalModeData string
}

var detachedCases = []detachedCase{
	{
		tableCols:        "(dt DATETIME, ts TIMESTAMP);",
		physicalModeData: "2019-01-01 00:00:00,2019-01-01 00:00:00",
	},
	{
		tableCols:        "(c INT NOT NULL, c2 TINYINT);",
		physicalModeData: "1,100",
	},
}

func (s *mockGCSSuite) TestSameBehaviourDetachedOrNot() {
	s.tk.MustExec("SET SESSION TIME_ZONE = '+08:00';")
	for _, ca := range detachedCases {
		s.tk.MustExec("DROP DATABASE IF EXISTS test_detached;")
		s.tk.MustExec("CREATE DATABASE test_detached;")
		s.tk.MustExec("CREATE TABLE test_detached.t1 " + ca.tableCols)
		s.tk.MustExec("CREATE TABLE test_detached.t2 " + ca.tableCols)

		s.server.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-detached",
				Name:       "1.txt",
			},
			Content: []byte(ca.physicalModeData),
		})
		s.tk.MustQuery(fmt.Sprintf(`IMPORT INTO test_detached.t1 FROM 'gs://test-detached/1.txt?endpoint=%s' WITH thread=1;`,
			gcsEndpoint))
		rows := s.tk.MustQuery(fmt.Sprintf(`IMPORT INTO test_detached.t2 FROM 'gs://test-detached/1.txt?endpoint=%s' WITH DETACHED, thread=1;`,
			gcsEndpoint)).Rows()
		require.Len(s.T(), rows, 1)
		jobID, err := strconv.Atoi(rows[0][0].(string))
		s.NoError(err)
		ctx := context.Background()
		ctx = util.WithInternalSourceType(ctx, "taskManager")
		require.Eventually(s.T(), func() bool {
			task := s.getTaskByJobID(ctx, int64(jobID))
			return task.State == proto.TaskStateSucceed
		}, maxWaitTime, time.Second)

		r1 := s.tk.MustQuery("SELECT * FROM test_detached.t1").Sort().Rows()
		s.tk.MustQuery("SELECT * FROM test_detached.t2").Sort().Check(r1)
	}
}

func (s *mockGCSSuite) TestReadOnlyTransitionDuringImport() {
	if !kerneltype.IsClassic() {
		s.T().Skip("physical import uses table mode in classic kernel")
	}
	// These hooks use the local transaction view rather than cluster discovery in this test.
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/ddl/mockModifySchemaReadOnlyDDL", "return")
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/executor/mockModifySchemaReadOnlyDDL", "return")
	s.tk.MustExec("create database read_only_import")
	s.tk.MustExec("create table read_only_import.t(id int primary key)")
	s.tk.MustExec("create table read_only_import.authorized(id int primary key)")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "read-only-import", Name: "t.csv"},
		Content:     []byte("1\n2\n"),
	})
	started := make(chan struct{})
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	var once sync.Once
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/ReadyForImportEngine", func() {
		once.Do(func() { close(started); <-resume })
	})
	rows := s.tk.MustQuery(fmt.Sprintf("import into read_only_import.t from 'gs://read-only-import/t.csv?endpoint=%s' with detached, thread=1", gcsEndpoint)).Rows()
	jobID, err := strconv.ParseInt(rows[0][0].(string), 10, 64)
	s.Require().NoError(err)
	select {
	case <-started:
	case <-time.After(maxWaitTime):
		s.T().Fatal("background import did not reach SST ingestion")
	}
	// Entry-only checks let read-only succeed while physical ingestion is still paused.
	s.tk.MustExec("alter database read_only_import read only = 1")
	db, ok := domain.GetDomain(s.tk.Session()).InfoSchema().SchemaByName(ast.NewCIStr("read_only_import"))
	s.Require().True(ok)
	s.Require().True(db.ReadOnly)
	release()
	ctx := util.WithInternalSourceType(context.Background(), "taskManager")
	s.Require().Eventually(func() bool {
		return s.getTaskByJobID(ctx, jobID).State == proto.TaskStateSucceed
	}, maxWaitTime, 100*time.Millisecond)
	// Successful SELECT also verifies that the internal completion DDL restored normal mode.
	s.tk.MustQuery("select * from read_only_import.t order by id").Check(testkit.Rows("1", "2"))
	s.tk.MustGetErrCode("import into read_only_import.t from ''", 3989)
	s.tk.MustGetErrCode("insert into read_only_import.t values(3)", 3989)
	// A caller's explicit bypass must survive the trusted internal mode claim.
	s.Require().NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	s.tk.MustExec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'root'@'%'")
	defer s.tk.MustExec("revoke RESTRICTED_REPLICA_WRITER_ADMIN on *.* from 'root'@'%'")
	s.tk.MustQuery(fmt.Sprintf("import into read_only_import.authorized from 'gs://read-only-import/t.csv?endpoint=%s' with thread=1", gcsEndpoint))
	s.tk.MustQuery("select * from read_only_import.authorized order by id").Check(testkit.Rows("1", "2"))
	s.tk.MustExec("alter database read_only_import read only = 0")
	s.tk.MustExec("drop database read_only_import")
}
