// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importer

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

// The read TS identifies the query: every IMPORT INTO ... FROM SELECT reads at
// the start TS of its own statement, and the TS is persisted in the task meta.
func queryGCBarrierID(readTS uint64) string {
	return "import-into-query-" + strconv.FormatUint(readTS, 10)
}

// SetQueryGCBarrier keeps GC of the store's keyspace from advancing past readTS
// for ttl, so the query of IMPORT INTO ... FROM SELECT can read at readTS on any
// node until its task finishes.
func SetQueryGCBarrier(ctx context.Context, store kv.Storage, readTS uint64, ttl time.Duration) error {
	cli, err := gcStatesClient(store)
	if err != nil {
		return err
	}
	_, err = cli.SetGCBarrier(ctx, queryGCBarrierID(readTS), readTS, ttl)
	return errors.Trace(err)
}

// DeleteQueryGCBarrier removes the barrier set by SetQueryGCBarrier.
func DeleteQueryGCBarrier(ctx context.Context, store kv.Storage, readTS uint64) error {
	cli, err := gcStatesClient(store)
	if err != nil {
		return err
	}
	_, err = cli.DeleteGCBarrier(ctx, queryGCBarrierID(readTS))
	return errors.Trace(err)
}

func gcStatesClient(store kv.Storage) (pdgc.GCStatesClient, error) {
	s, ok := store.(kv.StorageWithPD)
	if !ok {
		return nil, errors.New("GC barrier requires a storage with PD")
	}
	return s.GetPDClient().GetGCStatesClient(uint32(store.GetCodec().GetKeyspaceID())), nil
}
