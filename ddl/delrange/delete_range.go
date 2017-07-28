// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package delrange

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/delrange/sql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/util/sqlexec"
	goctx "golang.org/x/net/context"
)

// DelRangeWorker deals with DDL jobs which can speed up by delete-range.
type DelRangeWorker struct {
	executor sqlexec.SQLExecutor
	ReqCh    <-chan *model.Job
	RspCh    chan<- struct{}
}

func NewAndStartDelRangeWorker(store kv.Storage) error {
	s, err := tidb.CreateSession(store)
	if err != nil {
		return errors.Trace(err)
	}
	privilege.BindPrivilegeManager(s, nil)

	worker := DelRangeWorker{
		executor: s.(sqlexec.SQLExecutor),
		ReqCh:    ddl.DelRangeReqCh,
		RspCh:    ddl.DelRangeRspCh,
	}
	go worker.start(s.GoCtx())
	return nil
}

func (worker *DelRangeWorker) start(ctx goctx.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-worker.ReqCh:
			err := delrangesql.InsertBgJobIntoDeleteRangeTable(worker.executor, job)
			if err != nil {
				log.Errorf("[ddl] handle delete-range job err %v", errors.ErrorStack(err))
			}
			worker.RspCh <- struct{}{}
		}
	}
}
