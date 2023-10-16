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
	"fmt"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/stretchr/testify/require"
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
	s.T().Cleanup(func() {
		executor.TestDetachedTaskFinished.Store(false)
	})

	s.enableFailpoint("github.com/pingcap/tidb/pkg/executor/testDetachedTaskFinished", "return(true)")
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
		executor.TestDetachedTaskFinished.Store(false)
		s.tk.MustQuery(fmt.Sprintf(`IMPORT INTO test_detached.t1 FROM 'gs://test-detached/1.txt?endpoint=%s' WITH thread=1;`,
			gcsEndpoint))
		rows := s.tk.MustQuery(fmt.Sprintf(`IMPORT INTO test_detached.t2 FROM 'gs://test-detached/1.txt?endpoint=%s' WITH DETACHED, thread=1;`,
			gcsEndpoint)).Rows()
		require.Len(s.T(), rows, 1)
		require.Eventually(s.T(), func() bool {
			return executor.TestDetachedTaskFinished.Load()
		}, maxWaitTime, time.Second)

		r1 := s.tk.MustQuery("SELECT * FROM test_detached.t1").Sort().Rows()
		s.tk.MustQuery("SELECT * FROM test_detached.t2").Sort().Check(r1)
	}
}
