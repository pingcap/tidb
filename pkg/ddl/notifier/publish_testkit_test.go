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

package notifier_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestPublishToTableStore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec("DROP TABLE IF EXISTS ddl_notifier")
	tk.MustExec(`
CREATE TABLE ddl_notifier (
	ddl_job_id BIGINT,
	multi_schema_change_seq BIGINT COMMENT '-1 if the schema change does not belong to a multi-schema change DDL. 0 or positive numbers representing the sub-job index of a multi-schema change DDL',
	schema_change LONGBLOB COMMENT 'SchemaChangeEvent at rest',
	processed_by_flag BIGINT UNSIGNED DEFAULT 0 COMMENT 'flag to mark which subscriber has processed the event',
	PRIMARY KEY(ddl_job_id, multi_schema_change_seq)
)
`)

	ctx := context.Background()
	s := notifier.OpenTableStore("test", "ddl_notifier")
	se := sess.NewSession(tk.Session())
	event1 := notifier.NewCreateTableEvent(&model.TableInfo{ID: 1000, Name: pmodel.NewCIStr("t1")})
	err := notifier.PubSchemeChangeToStore(ctx, se, 1, -1, event1, s)
	require.NoError(t, err)
	event2 := notifier.NewDropTableEvent(&model.TableInfo{ID: 1001, Name: pmodel.NewCIStr("t2")})
	err = notifier.PubSchemeChangeToStore(ctx, se, 2, -1, event2, s)
	require.NoError(t, err)
	got, err := s.List(ctx, se, 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	got, err = s.List(ctx, se, 2)
	require.NoError(t, err)
	require.Len(t, got, 2)
	got, err = s.List(ctx, se, 3)
	require.NoError(t, err)
	require.Len(t, got, 2)
}
