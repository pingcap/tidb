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
	"io"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

const tableStructure = `
CREATE TABLE ddl_notifier (
	ddl_job_id BIGINT,
	multi_schema_change_seq BIGINT COMMENT '-1 if the schema change does not belong to a multi-schema change DDL. 0 or positive numbers representing the sub-job index of a multi-schema change DDL',
	schema_change LONGBLOB COMMENT 'SchemaChangeEvent at rest',
	processed_by_flag BIGINT UNSIGNED DEFAULT 0 COMMENT 'flag to mark which subscriber has processed the event',
	PRIMARY KEY(ddl_job_id, multi_schema_change_seq)
)
`

func TestPublishToTableStore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec("DROP TABLE IF EXISTS ddl_notifier")
	tk.MustExec(tableStructure)

	ctx := context.Background()
	s := notifier.OpenTableStore("test", "ddl_notifier")
	se := sess.NewSession(tk.Session())
	event1 := notifier.NewCreateTableEvent(&model.TableInfo{ID: 1000, Name: pmodel.NewCIStr("t1")})
	err := notifier.PubSchemeChangeToStore(ctx, se, 1, -1, event1, s)
	require.NoError(t, err)
	event2 := notifier.NewDropTableEvent(&model.TableInfo{ID: 1001, Name: pmodel.NewCIStr("t2")})
	err = notifier.PubSchemeChangeToStore(ctx, se, 2, -1, event2, s)
	require.NoError(t, err)
	got, err := s.List(ctx, se)
	require.NoError(t, err)
	require.Len(t, got, 2)
}

func TestBasicPubSub(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec("DROP TABLE IF EXISTS ddl_notifier")
	tk.MustExec(tableStructure)

	ctx, cancel := context.WithCancel(context.Background())
	s := notifier.OpenTableStore("test", "ddl_notifier")

	notifier.InitDDLNotifier(tk.Session(), s, 50*time.Millisecond)
	t.Cleanup(notifier.ResetDDLNotifier)

	var seenChangesMu sync.Mutex
	seenChanges := make([]*notifier.SchemaChangeEvent, 0, 8)
	injectedErrors := []error{
		nil,                            // received event1
		notifier.ErrNotReadyRetryLater, // event2 will be retried
		nil,                            // received event2 (should not receive event3)
		notifier.ErrNotReadyRetryLater,
		io.EOF,
	}
	testHandler := func(_ context.Context, _ sessionctx.Context, c *notifier.SchemaChangeEvent) error {
		var err error
		if len(injectedErrors) > 0 {
			err = injectedErrors[0]
			injectedErrors = injectedErrors[1:]
		}
		if err != nil {
			return err
		}

		seenChangesMu.Lock()
		defer seenChangesMu.Unlock()
		seenChanges = append(seenChanges, c)
		return nil
	}
	notifier.RegisterHandler(notifier.TestHandlerID, testHandler)

	done := make(chan struct{})
	go func() {
		notifier.StartDDLNotifier(ctx)
		close(done)
	}()

	tk2 := testkit.NewTestKit(t, store)
	se := sess.NewSession(tk2.Session())
	event1 := notifier.NewCreateTableEvent(&model.TableInfo{ID: 1000, Name: pmodel.NewCIStr("t1")})
	err := notifier.PubSchemeChangeToStore(ctx, se, 1, -1, event1, s)
	require.NoError(t, err)
	event2 := notifier.NewDropTableEvent(&model.TableInfo{ID: 1001, Name: pmodel.NewCIStr("t2")})
	err = notifier.PubSchemeChangeToStore(ctx, se, 2, -1, event2, s)
	require.NoError(t, err)
	event3 := notifier.NewDropTableEvent(&model.TableInfo{ID: 1002, Name: pmodel.NewCIStr("t3")})
	err = notifier.PubSchemeChangeToStore(ctx, se, 3, -1, event3, s)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		seenChangesMu.Lock()
		defer seenChangesMu.Unlock()
		return len(seenChanges) == 3
	}, time.Second, 25*time.Millisecond)

	require.Equal(t, event1, seenChanges[0])
	require.Equal(t, event2, seenChanges[1])
	require.Equal(t, event3, seenChanges[2])

	cancel()
	<-done
}
