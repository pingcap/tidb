// Copyright 2025 PingCAP, Inc.
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

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/testkit"
)

func (s *mockGCSSuite) TestCSVFieldsEncodedByBase64() {
	s.prepareAndUseDB("base64")
	s.tk.MustExec("CREATE TABLE t (a varchar(32), v int);")

	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "base64", Name: "base64.csv"},
		Content:     []byte("YWFhYQ==,MQ==\nYmJiYg==,\n,Mg==\n"),
	})
	s.tk.MustExec("set sql_mode=''")
	sql := fmt.Sprintf(`IMPORT INTO t FROM 'gs://base64/base64.csv?endpoint=%s'
		WITH fields_encoded_by='base64', fields_enclosed_by='', fields_escaped_by='', character_set='binary';`, gcsEndpoint)
	s.tk.MustQuery(sql)
	s.tk.MustQuery("SELECT * FROM t;").Check(testkit.Rows(
		"aaaa 1", "bbbb 0", " 2",
	))
}
