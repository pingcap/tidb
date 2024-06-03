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
	"fmt"
	"slices"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
)

type caseResult struct {
	insertedData     string
	nextGlobalAutoID []int64
}

type writeAfterImportCase struct {
	createTableSQL string
	insertSQL      string
	caseResults    map[importer.DataSourceType]caseResult
	autoIDCache1   bool
}

func (s *mockGCSSuite) testWriteAfterImport(importSQL string, sourceType importer.DataSourceType) {
	cases := []writeAfterImportCase{
		// with auto_increment
		{
			createTableSQL: "CREATE TABLE t (id int AUTO_INCREMENT PRIMARY KEY CLUSTERED, v varchar(64))",
			insertSQL:      "insert into t(v) values(1)",
			caseResults: map[importer.DataSourceType]caseResult{
				importer.DataSourceTypeFile: {
					insertedData:     "8 1",
					nextGlobalAutoID: []int64{8},
				},
				importer.DataSourceTypeQuery: {
					insertedData:     "8 1",
					nextGlobalAutoID: []int64{8},
				},
			},
		},
		{
			createTableSQL: "CREATE TABLE t (id int AUTO_INCREMENT PRIMARY KEY CLUSTERED, v varchar(64)) AUTO_ID_CACHE 1",
			insertSQL:      "insert into t(v) values(1)",
			caseResults: map[importer.DataSourceType]caseResult{
				importer.DataSourceTypeFile: {
					insertedData:     "8 1",
					nextGlobalAutoID: []int64{8, 1},
				},
				importer.DataSourceTypeQuery: {
					insertedData:     "8 1",
					nextGlobalAutoID: []int64{8, 1},
				},
			},
			autoIDCache1: true,
		},
		{
			createTableSQL: "CREATE TABLE t (id int AUTO_INCREMENT PRIMARY KEY NONCLUSTERED, v varchar(64))",
			insertSQL:      "insert into t(v) values(1)",
			caseResults: map[importer.DataSourceType]caseResult{
				importer.DataSourceTypeFile: {
					insertedData:     "12 1",
					nextGlobalAutoID: []int64{12},
				},
				importer.DataSourceTypeQuery: {
					insertedData:     "8 1",
					nextGlobalAutoID: []int64{8},
				},
			},
		},
		{
			createTableSQL: "CREATE TABLE t (id int AUTO_INCREMENT PRIMARY KEY NONCLUSTERED, v varchar(64)) AUTO_ID_CACHE 1",
			insertSQL:      "insert into t(v) values(1)",
			caseResults: map[importer.DataSourceType]caseResult{
				importer.DataSourceTypeFile: {
					insertedData:     "12 1",
					nextGlobalAutoID: []int64{8, 12},
				},
				importer.DataSourceTypeQuery: {
					insertedData:     "8 1",
					nextGlobalAutoID: []int64{8, 5},
				},
			},
			autoIDCache1: true,
		},
		// without auto_increment
		{
			createTableSQL: "CREATE TABLE t (id int PRIMARY KEY CLUSTERED, v varchar(64))",
			insertSQL:      "insert into t values(1,1)",
			caseResults: map[importer.DataSourceType]caseResult{
				importer.DataSourceTypeFile: {
					insertedData: "1 1",
				},
				importer.DataSourceTypeQuery: {
					insertedData: "1 1",
				},
			},
		},
		{
			createTableSQL: "CREATE TABLE t (id int PRIMARY KEY CLUSTERED, v varchar(64)) AUTO_ID_CACHE 1",
			insertSQL:      "insert into t values(1,1)",
			caseResults: map[importer.DataSourceType]caseResult{
				importer.DataSourceTypeFile: {
					insertedData: "1 1",
				},
				importer.DataSourceTypeQuery: {
					insertedData: "1 1",
				},
			},
			autoIDCache1: true,
		},
		{
			createTableSQL: "CREATE TABLE t (id int, v varchar(64))",
			insertSQL:      "insert into t values(1,1)",
			caseResults: map[importer.DataSourceType]caseResult{
				importer.DataSourceTypeFile: {
					insertedData:     "1 1",
					nextGlobalAutoID: []int64{12},
				},
				importer.DataSourceTypeQuery: {
					insertedData:     "1 1",
					nextGlobalAutoID: []int64{5},
				},
			},
		},
		{
			createTableSQL: "CREATE TABLE t (id int, v varchar(64)) AUTO_ID_CACHE 1",
			insertSQL:      "insert into t values(1,1)",
			caseResults: map[importer.DataSourceType]caseResult{
				importer.DataSourceTypeFile: {
					insertedData:     "1 1",
					nextGlobalAutoID: []int64{12},
				},
				importer.DataSourceTypeQuery: {
					insertedData:     "1 1",
					nextGlobalAutoID: []int64{5},
				},
			},
			autoIDCache1: true,
		},
		{
			createTableSQL: "CREATE TABLE t (id int PRIMARY KEY NONCLUSTERED, v varchar(64))",
			insertSQL:      "insert into t values(1,1)",
			caseResults: map[importer.DataSourceType]caseResult{
				importer.DataSourceTypeFile: {
					insertedData:     "1 1",
					nextGlobalAutoID: []int64{12},
				},
				importer.DataSourceTypeQuery: {
					insertedData:     "1 1",
					nextGlobalAutoID: []int64{5},
				},
			},
		},
		{
			createTableSQL: "CREATE TABLE t (id int PRIMARY KEY NONCLUSTERED, v varchar(64)) AUTO_ID_CACHE 1",
			insertSQL:      "insert into t values(1,1)",
			caseResults: map[importer.DataSourceType]caseResult{
				importer.DataSourceTypeFile: {
					insertedData:     "1 1",
					nextGlobalAutoID: []int64{12},
				},
				importer.DataSourceTypeQuery: {
					insertedData:     "1 1",
					nextGlobalAutoID: []int64{5},
				},
			},
			autoIDCache1: true,
		},
		// with auto_random
		{
			createTableSQL: "CREATE TABLE t (id bigint PRIMARY KEY auto_random, v varchar(64))",
			insertSQL:      "insert into t(v) values(1)",
			caseResults: map[importer.DataSourceType]caseResult{
				importer.DataSourceTypeFile: {
					insertedData:     "8 1",
					nextGlobalAutoID: []int64{8},
				},
				importer.DataSourceTypeQuery: {
					insertedData:     "8 1",
					nextGlobalAutoID: []int64{8},
				},
			},
			autoIDCache1: true,
		},
	}

	allData := []string{"4 aaaaaa", "5 bbbbbb", "6 cccccc", "7 dddddd"}
	s.prepareAndUseDB("write_after_import")
	s.T().Cleanup(func() {
		s.tk.MustExec("drop table if exists t;")
	})
	for i, c := range cases {
		s.Run(fmt.Sprintf("case-%d", i), func() {
			fmt.Println("current case ", c.createTableSQL)
			s.tk.MustExec("drop table if exists t;")
			s.tk.MustExec(c.createTableSQL)
			if sourceType == importer.DataSourceTypeFile {
				s.tk.MustQuery(importSQL)
			} else {
				s.tk.MustExec(importSQL)
			}
			querySQL := "SELECT * FROM t;"
			s.tk.MustQuery(querySQL).Check(testkit.Rows(allData...))

			is := s.tk.Session().GetDomainInfoSchema().(infoschema.InfoSchema)
			dbInfo, ok := is.SchemaByName(model.NewCIStr("write_after_import"))
			s.True(ok)
			tableObj, err := is.TableByName(model.NewCIStr("write_after_import"), model.NewCIStr("t"))
			s.NoError(err)
			if common.TableHasAutoID(tableObj.Meta()) {
				allocators, err := common.GetGlobalAutoIDAlloc(domain.GetDomain(s.tk.Session()), dbInfo.ID, tableObj.Meta())
				s.NoError(err)
				var nextGlobalAutoID []int64
				for _, alloc := range allocators {
					id, err := alloc.NextGlobalAutoID()
					s.NoError(err)
					nextGlobalAutoID = append(nextGlobalAutoID, id)
				}
				s.Equal(c.caseResults[sourceType].nextGlobalAutoID, nextGlobalAutoID)
			}

			// when autoIDCache1=true, the id service is not started in real-tikv-test, cannot insert.
			if !c.autoIDCache1 {
				s.tk.MustExec(c.insertSQL)
				newAllData := append(allData, c.caseResults[sourceType].insertedData)
				slices.Sort(newAllData)
				s.tk.MustQuery(querySQL).Sort().Check(testkit.Rows(newAllData...))
			}
		})
	}
}
func (s *mockGCSSuite) TestWriteAfterImportFromFile() {
	// 2 files, each with 18 bytes, divide by column count 2, the calculated id
	// range is [1, 9], [10, 18], the max id if it's used during encoding will be 11.
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "write_after_import", Name: "1.csv"},
		Content:     []byte("4,aaaaaa\n5,bbbbbb\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "write_after_import", Name: "2.csv"},
		Content:     []byte("6,cccccc\n7,dddddd\n"),
	})
	importSQL := fmt.Sprintf(`import into t FROM 'gs://write_after_import/*.csv?endpoint=%s'`, gcsEndpoint)
	s.testWriteAfterImport(importSQL, importer.DataSourceTypeFile)
}
