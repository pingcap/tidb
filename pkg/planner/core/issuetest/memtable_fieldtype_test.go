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

package issuetest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// Regression test for https://github.com/pingcap/tidb/issues/73151:
// buildMemTable used to hand out the process-global infoschema FieldType
// directly, so planner code that adjusts a column type in place (window
// function type inference, projection elimination) permanently cleared the
// NOT NULL flag of memory-table columns for every later session.
func TestMemTableColumnFieldTypeNotMutated(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	assertNotNull := func(tk *testkit.TestKit) {
		for _, col := range []string{"TABLE_NAME", "ORDINAL_POSITION"} {
			rows := tk.MustQuery("select is_nullable from information_schema.columns" +
				" where table_schema = 'information_schema' and table_name = 'KEY_COLUMN_USAGE'" +
				" and column_name = '" + col + "'").Rows()
			require.Len(t, rows, 1)
			require.Equal(t, "NO", rows[0][0], "column %s lost its NOT NULL flag", col)
		}
	}

	assertNotNull(tk)

	// The two read-only trigger queries from the issue: FIRST_VALUE makes
	// window-function type inference delete the NOT NULL flag in place, and
	// the IF query makes projection elimination write the IF nullability
	// back onto the column type.
	tk.MustQuery("select first_value(table_name) over () from information_schema.key_column_usage limit 1")
	tk.MustQuery("select if(x, ordinal_position, position_in_unique_constraint) as y" +
		" from (select 1 as x, ordinal_position, position_in_unique_constraint" +
		" from information_schema.key_column_usage) s limit 1")

	assertNotNull(tk)

	// The corruption was process-global: a fresh session must still see the
	// original NOT NULL definitions.
	tk2 := testkit.NewTestKit(t, store)
	assertNotNull(tk2)
}
