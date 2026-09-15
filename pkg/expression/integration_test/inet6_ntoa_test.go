// Copyright 2026 PingCAP, Inc.
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

package integration_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestInet6NtoaInvalidArg(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Invalid arguments: not a binary string of exactly 4 or 16 bytes. MySQL
	// returns NULL with warning 1411 rather than rendering a bogus address, even
	// when the byte length is 4 or 16 but the value is not a binary string.
	// See https://github.com/pingcap/tidb/issues/59461.
	tk.MustQuery("select inet6_ntoa(1234)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1411 Incorrect string value: '1234' for function inet6_ntoa"))

	tk.MustQuery("select inet6_ntoa('abcdefghijklmnop')").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1411 Incorrect string value: 'abcdefghijklmnop' for function inet6_ntoa"))

	// A valid binary string produced by inet6_aton / unhex still renders.
	tk.MustQuery("select inet6_ntoa(inet6_aton('1.2.3.4'))").Check(testkit.Rows("1.2.3.4"))
	tk.MustQuery("select inet6_ntoa(unhex('0A000509'))").Check(testkit.Rows("10.0.5.9"))

	// A genuinely binary string (unhex) whose length is not 4 or 16 is rejected
	// too, exercising the length-only failure path (charset is already binary).
	tk.MustQuery("select inet6_ntoa(unhex('3132333435'))").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1411 Incorrect string value: '12345' for function inet6_ntoa"))

	// Exercise the vectorized path against real column-backed values: a binary
	// VARBINARY column renders, a non-binary VARCHAR column is rejected to NULL
	// with the same warning.
	tk.MustExec("create table t (v varbinary(20), s varchar(20))")
	tk.MustExec("insert into t values (inet6_aton('10.0.5.9'), 'abcdefghijklmnop')")
	tk.MustQuery("select inet6_ntoa(v) from t").Check(testkit.Rows("10.0.5.9"))
	tk.MustQuery("select inet6_ntoa(s) from t").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1411 Incorrect string value: 'abcdefghijklmnop' for function inet6_ntoa"))
}
