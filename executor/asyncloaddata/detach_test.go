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

package asyncloaddata_test

import (
	"fmt"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	. "github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/stretchr/testify/require"
)

type detachedCase struct {
	tableCols string
	data      string
}

var detachedCases = []detachedCase{
	{
		tableCols: "(dt DATETIME, ts TIMESTAMP);",
		data:      "2019-01-01 00:00:00\t2019-01-01 00:00:00",
	},
	{
		tableCols: "(c INT NOT NULL, c2 TINYINT);",
		data:      "\\N\t100000000000000",
	},
	{
		tableCols: "(ts TIMESTAMP);",
		data:      "0000-00-00 00:00:00\n9999-99-99 00:00:00\n2000-02-30 00:00:00",
	},
}

func (s *mockGCSSuite) TestSameBehaviourDetachedOrNot() {
	backup := HeartBeatInSec
	HeartBeatInSec = 1
	s.T().Cleanup(func() {
		HeartBeatInSec = backup
	})
	user := &auth.UserIdentity{
		AuthUsername: "test-detached",
		AuthHostname: "test-host",
	}
	s.tk.Session().GetSessionVars().User = user

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
			Content: []byte(ca.data),
		})
		s.tk.MustExec(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-detached/1.txt?endpoint=%s'
			IGNORE INTO TABLE test_detached.t1;`, gcsEndpoint))
		rows := s.tk.MustQuery(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-detached/1.txt?endpoint=%s'
			IGNORE INTO TABLE test_detached.t2 WITH DETACHED;`, gcsEndpoint)).Rows()
		require.Len(s.T(), rows, 1)
		jobID := rows[0][0].(string)
		require.Eventually(s.T(), func() bool {
			rows = s.tk.MustQuery("SHOW LOAD DATA JOB " + jobID).Rows()
			require.Len(s.T(), rows, 1)
			return rows[0][9] == "finished"
		}, 5*time.Second, time.Second)

		r1 := s.tk.MustQuery("SELECT * FROM test_detached.t1").Sort().Rows()
		s.tk.MustQuery("SELECT * FROM test_detached.t2").Sort().Check(r1)
	}
}
