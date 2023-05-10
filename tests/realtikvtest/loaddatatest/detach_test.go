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

package loaddatatest

import (
	"fmt"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/executor/asyncloaddata"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

type detachedCase struct {
	tableCols        string
	logicalModeData  string
	physicalModeData string
}

var detachedCases = []detachedCase{
	{
		tableCols:        "(dt DATETIME, ts TIMESTAMP);",
		logicalModeData:  "2019-01-01 00:00:00\t2019-01-01 00:00:00",
		physicalModeData: "2019-01-01 00:00:00\t2019-01-01 00:00:00",
	},
	{
		tableCols:        "(c INT NOT NULL, c2 TINYINT);",
		logicalModeData:  "\\N\t100000000000000",
		physicalModeData: "1\t100",
	},
	{
		tableCols:       "(ts TIMESTAMP);",
		logicalModeData: "0000-00-00 00:00:00\n9999-99-99 00:00:00\n2000-02-30 00:00:00",
		// todo: physical mode don't support restrictive now.
		physicalModeData: "2000-02-18 01:02:03",
	},
}

func (s *mockGCSSuite) TestSameBehaviourDetachedOrNot() {
	s.testSameBehaviourDetachedOrNot(importer.LogicalImportMode)
	s.testSameBehaviourDetachedOrNot(importer.PhysicalImportMode)
}

func (s *mockGCSSuite) testSameBehaviourDetachedOrNot(importMode string) {
	withOptions := fmt.Sprintf("WITH thread=1, import_mode='%s'", importMode)
	detachedWithOptions := fmt.Sprintf("WITH DETACHED, thread=1, import_mode='%s'", importMode)
	backup := asyncloaddata.HeartBeatInSec
	asyncloaddata.HeartBeatInSec = 1
	s.T().Cleanup(func() {
		asyncloaddata.HeartBeatInSec = backup
	})
	user := &auth.UserIdentity{
		AuthUsername: "test-detached",
		AuthHostname: "test-host",
	}
	tk2 := testkit.NewTestKit(s.T(), s.store)
	tk2.Session().GetSessionVars().User = user

	tk2.MustExec("SET SESSION TIME_ZONE = '+08:00';")
	for _, ca := range detachedCases {
		tk2.MustExec("DROP DATABASE IF EXISTS test_detached;")
		tk2.MustExec("CREATE DATABASE test_detached;")
		tk2.MustExec("CREATE TABLE test_detached.t1 " + ca.tableCols)
		tk2.MustExec("CREATE TABLE test_detached.t2 " + ca.tableCols)

		data := ca.logicalModeData
		if importMode == importer.PhysicalImportMode {
			data = ca.physicalModeData
		}
		s.server.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-detached",
				Name:       "1.txt",
			},
			Content: []byte(data),
		})
		tk2.MustExec(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-detached/1.txt?endpoint=%s'
			IGNORE INTO TABLE test_detached.t1 %s;`, gcsEndpoint, withOptions))
		rows := tk2.MustQuery(fmt.Sprintf(`LOAD DATA INFILE 'gs://test-detached/1.txt?endpoint=%s'
			IGNORE INTO TABLE test_detached.t2 %s;`, gcsEndpoint, detachedWithOptions)).Rows()
		require.Len(s.T(), rows, 1)
		jobID := rows[0][0].(string)
		require.Eventually(s.T(), func() bool {
			rows = tk2.MustQuery("SHOW LOAD DATA JOB " + jobID).Rows()
			require.Len(s.T(), rows, 1)
			return rows[0][9] == "finished"
		}, 5*time.Second, time.Second)

		r1 := tk2.MustQuery("SELECT * FROM test_detached.t1").Sort().Rows()
		tk2.MustQuery("SELECT * FROM test_detached.t2").Sort().Check(r1)
	}
}
