// Copyright 2024 PingCAP, Inc.
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

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestChecksum(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec(`
		CREATE TABLE t (
			c1 INT PRIMARY KEY,
			c2 INT,
			INDEX idx(c2)
		) PARTITION BY RANGE(c1) (
			PARTITION p0 VALUES LESS THAN (10),
			PARTITION p1 VALUES LESS THAN MAXVALUE
		);`)
	// for unistore, each checksum request will return
	// Checksum: 1, TotalKvs: 1, TotalBytes: 1,
	// see cophandler.handleCopChecksumRequest
	// here we only check 2 (index) * 3 (partition) requests are sent
	tk.MustQuery("ADMIN CHECKSUM TABLE t").Check(testkit.Rows("test t 0 6 6"))
}
