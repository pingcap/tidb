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

package loaddatatest

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/docker/go-units"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
)

func (s *mockGCSSuite) compareJobInfoWithoutTime(jobInfo *importer.JobInfo, row []interface{}) {
	s.Equal(strconv.Itoa(int(jobInfo.ID)), row[0])

	urlExpected, err := url.Parse(jobInfo.Parameters.FileLocation)
	s.NoError(err)
	urlGot, err := url.Parse(fmt.Sprintf("%v", row[1]))
	s.NoError(err)
	// order of query parameters might change
	s.Equal(urlExpected.Query(), urlGot.Query())
	urlExpected.RawQuery, urlGot.RawQuery = "", ""
	s.Equal(urlExpected.String(), urlGot.String())

	s.Equal(utils.EncloseDBAndTable(jobInfo.TableSchema, jobInfo.TableName), row[2])
	s.Equal(strconv.Itoa(int(jobInfo.TableID)), row[3])
	s.Equal(jobInfo.Step, row[4])
	s.Equal(jobInfo.Status, row[5])
	s.Equal(units.HumanSize(float64(jobInfo.SourceFileSize)), row[6])
	s.Equal(strconv.Itoa(int(jobInfo.Summary.ImportedRows)), row[7])
	s.Equal(jobInfo.ErrorMessage, row[8])
	s.Equal(jobInfo.CreatedBy, row[12])
}

func (s *mockGCSSuite) TestShowJob() {
	s.prepareAndUseDB("test_show_job")
	s.tk.MustExec("CREATE TABLE t1 (i INT PRIMARY KEY);")
	s.tk.MustExec("CREATE TABLE t2 (i INT PRIMARY KEY);")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "test-show-job", Name: "t.csv"},
		Content:     []byte("1\n2"),
	})
	s.T().Cleanup(func() {
		_ = s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil)
	})
	s.tk.MustExec(`DROP USER IF EXISTS 'test_show_job1'@'localhost';`)
	s.tk.MustExec(`CREATE USER 'test_show_job1'@'localhost';`)
	s.tk.MustExec(`GRANT SELECT,UPDATE,INSERT,DELETE,ALTER on *.* to 'test_show_job1'@'localhost'`)
	s.tk.MustExec(`DROP USER IF EXISTS 'test_show_job2'@'localhost';`)
	s.tk.MustExec(`CREATE USER 'test_show_job2'@'localhost';`)
	s.tk.MustExec(`GRANT SELECT,UPDATE,INSERT,DELETE,ALTER on *.* to 'test_show_job2'@'localhost'`)
	do, err := session.GetDomain(s.store)
	s.NoError(err)
	tableID1 := do.MustGetTableID(s.T(), "test_show_job", "t1")
	//tableID2 := do.MustGetTableID(s.T(), "test_show_job", "t2")

	s.enableFailpoint("github.com/pingcap/tidb/executor/importer/setLastImportJobID", `return(true)`)
	s.enableFailpoint("github.com/pingcap/tidb/disttask/framework/storage/testSetLastTaskID", "return(true)")
	s.enableFailpoint("github.com/pingcap/tidb/br/pkg/storage/forceRedactURL", "return(true)")
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "test_show_job1", Hostname: "localhost"}, nil, nil, nil))
	result1 := s.tk.MustQuery(fmt.Sprintf(`import into t1 FROM 'gs://test-show-job/t.csv?access-key=aaaaaa&secret-access-key=bbbbbb&endpoint=%s'`,
		gcsEndpoint)).Rows()
	s.Len(result1, 1)
	s.tk.MustQuery("select * from t1").Check(testkit.Rows("1", "2"))
	rows := s.tk.MustQuery(fmt.Sprintf("show import job %d", importer.TestLastImportJobID.Load())).Rows()
	s.Len(rows, 1)
	s.Equal(result1, rows)
	jobInfo := &importer.JobInfo{
		ID:          importer.TestLastImportJobID.Load(),
		TableSchema: "test_show_job",
		TableName:   "t1",
		TableID:     tableID1,
		CreatedBy:   "test_show_job1@localhost",
		Parameters: importer.ImportParameters{
			FileLocation: fmt.Sprintf(`gs://test-show-job/t.csv?access-key=redacted&secret-access-key=redacted&endpoint=%s`, gcsEndpoint),
			Format:       importer.DataFormatCSV,
		},
		SourceFileSize: 3,
		Status:         "finished",
		Step:           "",
		Summary: &importer.JobSummary{
			ImportedRows: 2,
		},
		ErrorMessage: "",
	}
	s.compareJobInfoWithoutTime(jobInfo, rows[0])

	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "test_show_job2", Hostname: "localhost"}, nil, nil, nil))
	result2 := s.tk.MustQuery(fmt.Sprintf(`import into t2 FROM 'gs://test-show-job/t.csv?endpoint=%s'`, gcsEndpoint)).Rows()
	s.tk.MustQuery("select * from t2").Check(testkit.Rows("1", "2"))
	rows = s.tk.MustQuery(fmt.Sprintf("show import job %d", importer.TestLastImportJobID.Load())).Rows()
	s.Len(rows, 1)
	s.Equal(result2, rows)
	// if this case run twice, it might fail, start another cluster to solve this problem.
	rows = s.tk.MustQuery("show import jobs").Rows()
	s.Len(rows, 1)
	s.Equal(result2, rows)

	// test with root
	checkJobsMatch := func(rows [][]interface{}) {
		s.GreaterOrEqual(len(rows), 2) // other cases may create import jobs
		var matched int
		for _, r := range rows {
			if r[0] == result1[0][0] {
				s.Equal(result1[0], r)
				matched++
			}
			if r[0] == result2[0][0] {
				s.Equal(result2[0], r)
				matched++
			}
		}
		s.Equal(2, matched)
	}
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	rows = s.tk.MustQuery("show import jobs").Rows()
	checkJobsMatch(rows)

	// grant SUPER to test_show_job2, now it can see all jobs
	s.tk.MustExec(`GRANT SUPER on *.* to 'test_show_job2'@'localhost'`)
	s.NoError(s.tk.Session().Auth(&auth.UserIdentity{Username: "test_show_job2", Hostname: "localhost"}, nil, nil, nil))
	rows = s.tk.MustQuery("show import jobs").Rows()
	checkJobsMatch(rows)
}
