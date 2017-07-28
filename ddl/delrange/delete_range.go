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
)

// DelRangeWorker deals with DDL jobs which can speed up by delete-range.
type DelRangeWorker struct {
	executor sqlexec.SQLExecutor
	reqCh    <-chan *model.Job
	rspCh    chan<- struct{}
	stopCh   <-chan struct{}
}

// NewAndStartDelRangeWorker starts a new DelRangeWorker, which gets tasks
// from ddl.DelRangereqCh, and run them with a session without privileges.
func NewAndStartDelRangeWorker(store kv.Storage) error {
	s, err := tidb.CreateSession(store)
	if err != nil {
		return errors.Trace(err)
	}
	privilege.BindPrivilegeManager(s, nil)

	worker := DelRangeWorker{
		executor: s.(sqlexec.SQLExecutor),
		reqCh:    ddl.DelRangeReqCh,
		rspCh:    ddl.DelRangeRspCh,
	}
	go worker.start()
	log.Infof("[ddl] DelRangeWorker started")
	return nil
}

func (worker *DelRangeWorker) start() {
	for {
		select {
		case job := <-worker.reqCh:
			worker.executor.Execute("BEGIN")
			err := delrangesql.InsertBgJobIntoDeleteRangeTable(worker.executor, job)
			if err != nil {
				worker.executor.Execute("ROLLBACK")
				log.Errorf("[ddl] handle delete-range job err %v", errors.ErrorStack(err))
			}
			worker.executor.Execute("COMMIT")
			worker.rspCh <- struct{}{}
		case <-worker.stopCh:
			close(worker.rspCh)
			return
		}
	}
}
