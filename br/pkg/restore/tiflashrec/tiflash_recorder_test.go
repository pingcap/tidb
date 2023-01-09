// Copyright 2022-present PingCAP, Inc.
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

package tiflashrec_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/br/pkg/restore/tiflashrec"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
)

type op func(*tiflashrec.TiFlashRecorder)

func add(tableID int64, replica int) op {
	return func(tfr *tiflashrec.TiFlashRecorder) {
		tfr.AddTable(tableID, model.TiFlashReplicaInfo{
			Count: uint64(replica),
		})
	}
}

func rewrite(tableID, newTableID int64) op {
	return func(tfr *tiflashrec.TiFlashRecorder) {
		tfr.Rewrite(tableID, newTableID)
	}
}

func del(tableID int64) op {
	return func(tfr *tiflashrec.TiFlashRecorder) {
		tfr.DelTable(tableID)
	}
}

func ops(ops ...op) op {
	return func(tfr *tiflashrec.TiFlashRecorder) {
		for _, op := range ops {
			op(tfr)
		}
	}
}

type table struct {
	id      int64
	replica int
}

func t(id int64, replica int) table {
	return table{
		id:      id,
		replica: replica,
	}
}

func TestRecorder(tCtx *testing.T) {
	type Case struct {
		o  op
		ts []table
	}
	cases := []Case{
		{
			o: ops(
				add(42, 1),
				add(43, 2),
			),
			ts: []table{
				t(42, 1),
				t(43, 2),
			},
		},
		{
			o: ops(
				add(42, 3),
				add(43, 1),
				del(42),
			),
			ts: []table{
				t(43, 1),
			},
		},
		{
			o: ops(
				add(41, 4),
				add(42, 8),
				rewrite(42, 1890),
				rewrite(1890, 43),
				rewrite(41, 100),
			),
			ts: []table{
				t(43, 8),
				t(100, 4),
			},
		},
	}

	check := func(t *testing.T, c Case) {
		rec := tiflashrec.New()
		req := require.New(t)
		c.o(rec)
		tmap := map[int64]int{}
		for _, t := range c.ts {
			tmap[t.id] = t.replica
		}

		rec.Iterate(func(tableID int64, replicaReal model.TiFlashReplicaInfo) {
			replica, ok := tmap[tableID]
			req.True(ok, "the key %d not recorded", tableID)
			req.EqualValues(replica, replicaReal.Count, "the replica mismatch")
			delete(tmap, tableID)
		})
		req.Empty(tmap, "not all required are recorded")
	}

	for i, c := range cases {
		tCtx.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			check(t, c)
		})
	}
}

func TestGenSql(t *testing.T) {
	tInfo := func(id int, name string) *model.TableInfo {
		return &model.TableInfo{
			ID:   int64(id),
			Name: model.NewCIStr(name),
		}
	}
	fakeInfo := infoschema.MockInfoSchema([]*model.TableInfo{
		tInfo(1, "fruits"),
		tInfo(2, "whisper"),
		tInfo(3, "woods"),
		tInfo(4, "evils"),
	})
	rec := tiflashrec.New()
	rec.AddTable(1, model.TiFlashReplicaInfo{
		Count: 1,
	})
	rec.AddTable(2, model.TiFlashReplicaInfo{
		Count:          2,
		LocationLabels: []string{"climate"},
	})
	rec.AddTable(3, model.TiFlashReplicaInfo{
		Count:          3,
		LocationLabels: []string{"leaf", "seed"},
	})
	rec.AddTable(4, model.TiFlashReplicaInfo{
		Count:          1,
		LocationLabels: []string{`kIll'; OR DROP DATABASE test --`, `dEaTh with \"quoting\"`},
	})

	sqls := rec.GenerateAlterTableDDLs(fakeInfo)
	require.ElementsMatch(t, sqls, []string{
		"ALTER TABLE `test`.`whisper` SET TIFLASH REPLICA 2 LOCATION LABELS 'climate'",
		"ALTER TABLE `test`.`woods` SET TIFLASH REPLICA 3 LOCATION LABELS 'leaf', 'seed'",
		"ALTER TABLE `test`.`fruits` SET TIFLASH REPLICA 1",
		"ALTER TABLE `test`.`evils` SET TIFLASH REPLICA 1 LOCATION LABELS 'kIll''; OR DROP DATABASE test --', 'dEaTh with " + `\\"quoting\\"` + "'",
	})
}

func TestGenResetSql(t *testing.T) {
	tInfo := func(id int, name string) *model.TableInfo {
		return &model.TableInfo{
			ID:   int64(id),
			Name: model.NewCIStr(name),
		}
	}
	fakeInfo := infoschema.MockInfoSchema([]*model.TableInfo{
		tInfo(1, "fruits"),
		tInfo(2, "whisper"),
	})
	rec := tiflashrec.New()
	rec.AddTable(1, model.TiFlashReplicaInfo{
		Count: 1,
	})
	rec.AddTable(2, model.TiFlashReplicaInfo{
		Count:          2,
		LocationLabels: []string{"climate"},
	})

	sqls := rec.GenerateResetAlterTableDDLs(fakeInfo)
	require.ElementsMatch(t, sqls, []string{
		"ALTER TABLE `test`.`whisper` SET TIFLASH REPLICA 0",
		"ALTER TABLE `test`.`whisper` SET TIFLASH REPLICA 2 LOCATION LABELS 'climate'",
		"ALTER TABLE `test`.`fruits` SET TIFLASH REPLICA 0",
		"ALTER TABLE `test`.`fruits` SET TIFLASH REPLICA 1",
	})
}
