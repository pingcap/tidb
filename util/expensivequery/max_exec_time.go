// Copyright 2019 PingCAP, Inc.
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

package expensivequery

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// InterruptableQuery is the interface that for queries to be monitored
type InterruptableQuery interface {
	MaxExecTimeExceeded() bool
	CancelOnTimeout()
	QueryID() uint32
}

// AddQuery adds a query to be monitored
func (mntr *Handle) AddQuery(query InterruptableQuery) error {
	id := query.QueryID()
	mntr.mu.Lock()
	defer mntr.mu.Unlock()
	if mntr.queries[id] != nil {
		logutil.Logger(context.Background()).Info("Duplicate query", zap.Uint32(" id:", id))
		return errors.New("duplicate query id")
	}
	mntr.queries[query.QueryID()] = query
	return nil
}

// RemoveQuery removes a query
func (mntr *Handle) RemoveQuery(query InterruptableQuery) {
	mntr.mu.Lock()
	defer mntr.mu.Unlock()
	id := query.QueryID()
	delete(mntr.queries, id)
}

func (mntr *Handle) checkAll() {
	mntr.mu.RLock()
	queries := mntr.queries
	mntr.mu.RUnlock()

	for _, query := range queries {
		if query.MaxExecTimeExceeded() {
			logutil.Logger(context.Background()).Debug("Cancel query", zap.Uint32(" id:", query.QueryID()))
			query.CancelOnTimeout()
			mntr.RemoveQuery(query)
		}
	}
}
