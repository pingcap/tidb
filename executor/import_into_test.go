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

package executor_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/sem"
	"github.com/stretchr/testify/require"
)

func TestImportIntoExplicitTransaction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int);")
	tk.MustExec(`BEGIN`)
	err := tk.ExecToErr("IMPORT INTO t FROM '/file.csv'")
	require.Error(t, err)
	require.Regexp(t, "cannot run IMPORT INTO in explicit transaction", err.Error())
	tk.MustExec("commit")
}

func TestSecurityEnhancedMode(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	sem.Enable()
	defer sem.Disable()
	tk.MustExec("create table test.t (id int);")

	// When SEM is enabled these features are restricted to all users
	// regardless of what privileges they have available.
	tk.MustGetErrMsg("IMPORT INTO test.t FROM '/file.csv'", "[planner:8132]Feature 'IMPORT INTO from server disk' is not supported when security enhanced mode is enabled")
}

func TestImportIntoOptionsNegativeCase(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int);")

	cases := []struct {
		OptionStr string
		Err       error
	}{
		{OptionStr: "xx=1", Err: exeerrors.ErrUnknownOption},
		{OptionStr: "detached=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "character_set", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "detached, detached", Err: exeerrors.ErrDuplicateOption},

		{OptionStr: "character_set=true", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "character_set=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "character_set=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "character_set=true", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "character_set=''", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "character_set='aa'", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: "fields_terminated_by=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "fields_terminated_by=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "fields_terminated_by=true", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "fields_terminated_by=''", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: "fields_enclosed_by=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "fields_enclosed_by='aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "fields_enclosed_by=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "fields_enclosed_by=true", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: "fields_escaped_by=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "fields_escaped_by='aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "fields_escaped_by=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "fields_escaped_by=true", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: "fields_defined_null_by=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "fields_defined_null_by=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "fields_defined_null_by=true", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: "lines_terminated_by=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "lines_terminated_by=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "lines_terminated_by=true", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "lines_terminated_by=''", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: "skip_rows=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "skip_rows=''", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "skip_rows=-1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "skip_rows=true", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: "split_file='aa'", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: "disk_quota='aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "disk_quota='220MiBxxx'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "disk_quota=1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "disk_quota=false", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "disk_quota=null", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: "thread='aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "thread=0", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "thread=false", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "thread=-100", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "thread=null", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: "max_write_speed='aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "max_write_speed='11aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "max_write_speed=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "max_write_speed=-1", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "max_write_speed=false", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: "checksum_table=''", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "checksum_table=123", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "checksum_table=false", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "checksum_table=null", Err: exeerrors.ErrInvalidOptionVal},

		{OptionStr: "record_errors='aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "record_errors='111aa'", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "record_errors=-123", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "record_errors=null", Err: exeerrors.ErrInvalidOptionVal},
		{OptionStr: "record_errors=true", Err: exeerrors.ErrInvalidOptionVal},
	}

	sqlTemplate := "import into t from '/file.csv' with %s"
	for _, c := range cases {
		sql := fmt.Sprintf(sqlTemplate, c.OptionStr)
		err := tk.ExecToErr(sql)
		require.ErrorIs(t, err, c.Err, sql)
	}

	nonCSVCases := []struct {
		OptionStr string
		Err       error
	}{
		{OptionStr: "character_set='utf8'", Err: exeerrors.ErrLoadDataUnsupportedOption},
		{OptionStr: "fields_terminated_by='a'", Err: exeerrors.ErrLoadDataUnsupportedOption},
		{OptionStr: "fields_enclosed_by='a'", Err: exeerrors.ErrLoadDataUnsupportedOption},
		{OptionStr: "fields_escaped_by='a'", Err: exeerrors.ErrLoadDataUnsupportedOption},
		{OptionStr: "fields_defined_null_by='a'", Err: exeerrors.ErrLoadDataUnsupportedOption},
		{OptionStr: "lines_terminated_by='a'", Err: exeerrors.ErrLoadDataUnsupportedOption},
		{OptionStr: "skip_rows=1", Err: exeerrors.ErrLoadDataUnsupportedOption},
		{OptionStr: "split_file", Err: exeerrors.ErrLoadDataUnsupportedOption},
	}

	sqlTemplate = "import into t from '/file.csv' format '%s' with %s"
	for _, c := range nonCSVCases {
		for _, format := range []string{importer.DataFormatParquet, importer.DataFormatSQL} {
			sql := fmt.Sprintf(sqlTemplate, format, c.OptionStr)
			err := tk.ExecToErr(sql)
			require.ErrorIs(t, err, c.Err, sql)
		}
	}

	parameterCheck := []struct {
		sql string
		Err error
	}{
		{sql: "import into t from ''", Err: exeerrors.ErrLoadDataEmptyPath},
		{sql: "import into t from '/a.csv' format 'xx'", Err: exeerrors.ErrLoadDataUnsupportedFormat},
	}

	for _, c := range parameterCheck {
		err := tk.ExecToErr(c.sql)
		require.ErrorIs(t, err, c.Err, c.sql)
	}
}
