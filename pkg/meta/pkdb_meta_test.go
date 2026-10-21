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

package meta_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
)

func TestGetAllNameToIDAndUpdateTS(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	if err != nil {
		t.Fatalf("NewMockStore() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	txn, err := store.Begin()
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	mutator := meta.NewMutator(txn)
	dbInfo := &model.DBInfo{ID: 1, Name: pmodel.NewCIStr("test")}
	if err := mutator.CreateDatabase(dbInfo); err != nil {
		t.Fatalf("CreateDatabase() error = %v", err)
	}
	tableInfo := &model.TableInfo{
		ID:       2,
		Name:     pmodel.NewCIStr("t"),
		State:    model.StatePublic,
		UpdateTS: 123456,
	}
	if err := mutator.CreateTableOrView(dbInfo.ID, tableInfo); err != nil {
		t.Fatalf("CreateTableOrView() error = %v", err)
	}

	nameToID, updateTSByID, _, err := mutator.GetAllNameToIDAndTheMustLoadedTableInfo(dbInfo.ID)
	if err != nil {
		t.Fatalf("GetAllNameToIDAndTheMustLoadedTableInfo() error = %v", err)
	}
	if got := nameToID[tableInfo.Name.O]; got != tableInfo.ID {
		t.Fatalf("table ID = %d, want %d", got, tableInfo.ID)
	}
	if got := updateTSByID[tableInfo.ID]; got != tableInfo.UpdateTS {
		t.Fatalf("UpdateTS = %d, want %d", got, tableInfo.UpdateTS)
	}
}
