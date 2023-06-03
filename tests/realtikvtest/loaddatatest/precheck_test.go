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

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

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
	loadDataSQL := fmt.Sprintf(`IMPORT INTO t FROM 'gs://precheck-tbl-empty/file.csv?endpoint=%s'`, gcsEndpoint)
	err := s.tk.ExecToErr(loadDataSQL)
	require.ErrorIs(s.T(), err, exeerrors.ErrLoadDataPreCheckFailed)
}
