// Copyright 2016 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/placement"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/gcutil"
	"strconv"
	"strings"
	"time"
)

type PollTiFlashReplicaStatusContext struct {
	ID           int64
	Count   uint64
	LocationLabels []string
	Available      bool
	HighPriority   bool
	//TableInfo    *model.TableInfo
}

type TiFlashReplicaStatusResult struct {
	ID               int64
	RegionCount      int64
	FlashRegionCount int64
	ReplicaDetail    map[int64]int
}

func MakeBaseRule() placement.Rule {
	return placement.Rule{
		GroupID:  "tiflash",
		ID:       "",
		Index:    0,
		Override: true,
		Role:     placement.Learner,
		Count:    2,
		Constraints: []placement.Constraint{
			{
				Key:    "engine",
				Op:     placement.In,
				Values: []string{"tiflash"},
			},
		},
	}
}

func (d *ddl) PollTiFlashReplicaStatus(ctx sessionctx.Context) error {
	// TODO lastHandledSchemaVersionTso checking can be removed.
	ddlGlobalSchemaVersion := 0

	resp, err := d.etcdCli.Get(d.ctx, ddlutil.DDLGlobalSchemaVersion)
	if err == nil {
		if len(resp.Kvs) > 0 {
			ddlGlobalSchemaVersion, err = strconv.Atoi(string(resp.Kvs[0].Value))
			if err != nil {
				errors.Trace(err)
			}
		}
	}

	lastHandledSchemaVersionTso := "0_tso_0"
	resp, err = d.etcdCli.Get(d.ctx, ddlutil.TiFlashLastHandledSchemaVersion)
	if err == nil {
		if len(resp.Kvs) > 0 {
			lastHandledSchemaVersionTso = string(resp.Kvs[0].Value)
		}
	}

	splitVec := strings.Split(lastHandledSchemaVersionTso, "_tso_")
	lastHandledSchemaVersion, err := strconv.Atoi(splitVec[0])
	if err != nil {
		return errors.Trace(err)
	}
	lastHandledSchemaTso, err := strconv.Atoi(splitVec[1])
	if err != nil {
		return errors.Trace(err)
	}

	curTso := time.Now().Unix()

	if true || ddlGlobalSchemaVersion != lastHandledSchemaVersion || int64(lastHandledSchemaTso+300) <= curTso {
		// Need to update table
		allUpdate, err := d.TiFlashReplicaTableUpdate(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if allUpdate {
			// Need to update pd's schema version
			v := fmt.Sprintf("%v_tso_%v", ddlGlobalSchemaVersion, curTso)
			d.etcdCli.Put(d.ctx, "/tiflash/cluster/last_handled_schema_version", v)
		}

	}

	return nil
}

// Compare supposed rule, and we actually get from TableInfo
func isRuleMatch(rule placement.Rule, tb PollTiFlashReplicaStatusContext) (bool, *placement.Rule) {
	// Compute startKey
	startKey := tablecodec.GenTableRecordPrefix(tb.ID)
	endKey := tablecodec.EncodeTablePrefix(tb.ID + 1)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)

	isMatch := true

	if rule.Override && rule.StartKeyHex == startKey.String() && rule.EndKeyHex == endKey.String() {
		ok := false
		for _, c := range rule.Constraints {
			if c.Key == "engine" && len(c.Values) == 1 && c.Values[0] == "tiflash" && c.Op == placement.In {
				ok = true
				break
			}
		}
		if !ok {
			isMatch = false
		}

		if len(rule.LocationLabels) != len(tb.LocationLabels) {
			isMatch = false
		} else {
			for i, lb := range tb.LocationLabels {
				if lb != rule.LocationLabels[i] {
					isMatch = false
					break
				}
			}
		}

		if isMatch && uint64(rule.Count) != tb.Count {
			isMatch = false
		}
		if isMatch && rule.Role != placement.Learner {
			isMatch = false
		}
	} else {
		isMatch = false
	}
	if isMatch {
		return true, nil
	} else {
		return false, MakeNewRule(tb.ID, tb.Count, tb.LocationLabels)
	}
}

func MakeNewRule(ID int64, Count uint64, LocationLabels []string) *placement.Rule {
	ruleId := fmt.Sprintf("table-%v-r", ID)
	startKey := tablecodec.GenTableRecordPrefix(ID)
	endKey := tablecodec.EncodeTablePrefix(ID + 1)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)

	ruleNew := MakeBaseRule()
	ruleNew.ID = ruleId
	ruleNew.StartKeyHex = startKey.String()
	ruleNew.EndKeyHex = endKey.String()
	ruleNew.Count = int(Count)
	ruleNew.LocationLabels = LocationLabels

	return &ruleNew
}

func GetTiflashHttpAddr(host string, statusAddr string) (string, error) {
	configURL := fmt.Sprintf("%s://%s/config",
		util.InternalHTTPSchema(),
		statusAddr,
	)
	resp, err := util.InternalHTTPClient().Get(configURL)
	if err != nil {
		return "", errors.Trace(err)
	}

	defer func() {
		resp.Body.Close()
	}()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		return "", errors.Trace(err)
	}

	var j map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &j)
	if err != nil {
		return "", errors.Trace(err)
	}

	engineStore, ok := j["engine-store"].(map[string]interface{})
	if !ok {
		return "", errors.New("Error json")
	}
	port64, ok := engineStore["http_port"].(float64)
	if !ok {
		return "", errors.New("Error json")
	}
	port := int(port64)

	addr := fmt.Sprintf("%v:%v", host, port)
	return addr, nil
}



func GetTiFlashReplicaInfo(tblInfo *model.TableInfo, tableList *[]PollTiFlashReplicaStatusContext) {
	if tblInfo.TiFlashReplica == nil {
		// reject tables that has no tiflash replica such like `INFORMATION_SCHEMA`
		return
	}
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, p := range pi.Definitions {
			*tableList = append(*tableList, PollTiFlashReplicaStatusContext{p.ID,
				tblInfo.TiFlashReplica.Count, tblInfo.TiFlashReplica.LocationLabels, tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID), false})
		}
		// partitions that in adding mid-state
		for _, p := range pi.AddingDefinitions {
			*tableList = append(*tableList, PollTiFlashReplicaStatusContext{p.ID, tblInfo.TiFlashReplica.Count, tblInfo.TiFlashReplica.LocationLabels, tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID), true})
		}
	} else {
		*tableList = append(*tableList, PollTiFlashReplicaStatusContext{tblInfo.ID, tblInfo.TiFlashReplica.Count, tblInfo.TiFlashReplica.LocationLabels, tblInfo.TiFlashReplica.Available, false})
	}
}

// TODO _update_http_port
func (d *ddl) UpdateTiFlashHttpAddress(store *helper.StoreStat) error {
	addrAndPort := strings.Split(store.Store.StatusAddress, ":")
	if len(addrAndPort) < 2 {
		return errors.New("Can't get TiFlash Address from PD")
	}
	httpAddr, err := GetTiflashHttpAddr(addrAndPort[0], store.Store.StatusAddress)
	if err != nil {
		return errors.Trace(err)
	}
	// report to pd
	key := fmt.Sprintf("/tiflash/cluster/http_port/%v", store.Store.Address)
	resp, err := d.etcdCli.Get(d.ctx, key)
	origin := ""
	for _, kv := range resp.Kvs {
		if string(kv.Key) == key {
			origin = string(kv.Value)
			break
		}
	}
	fmt.Printf("!!!! httpAddr key %v origin %v \n", httpAddr, origin)
	if origin != httpAddr {
		// TODO add lease ttl
		d.etcdCli.Put(d.ctx, key, httpAddr)
	}

	return nil
}

func (d *ddl) TiFlashReplicaTableUpdate(ctx sessionctx.Context) (bool, error) {
	// Should we escape table update?
	allReplicaReady := true
	tikvStore, ok := ctx.GetStore().(helper.Storage)
	if !ok {
		return allReplicaReady, errors.New("Can not get Helper")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	// _update_cluster
	tikvStats, err := tikvHelper.GetStoresStat()
	if err != nil {
		return allReplicaReady, errors.Trace(err)
	}
	tiflashStores := make(map[int64]helper.StoreStat)
	for _, store := range tikvStats.Stores {
		for _, l := range store.Store.Labels {
			if l.Key == "engine" && l.Value == "tiflash" {
				tiflashStores[store.Store.ID] = store
				fmt.Printf("!!!! tiflashStores has tiflash %v %v\n", store.Store.ID, store.Store.Address)
			}
		}
	}

	for _, store := range tiflashStores {
		d.UpdateTiFlashHttpAddress(&store)
	}


	// main body of table_update
	schema := d.GetInfoSchemaWithInterceptor(ctx)

	// compute table_list
	var tableList []PollTiFlashReplicaStatusContext = make([]PollTiFlashReplicaStatusContext, 0)

	for _, db := range schema.AllSchemas() {
		tbls := schema.SchemaTables(db.Name)
		for _, tbl := range tbls {
			tblInfo := tbl.Meta()
			GetTiFlashReplicaInfo(tblInfo, &tableList)
		}
	}

	// Removed pd rule handling to somewhere else
	HandlePlacementRuleRoutine(ctx, d, tableList)

	for _, tb := range tableList {
		// for every region in each table, if it has one replica, we reckon it ready
		// TODO Can we batch request table?

		fmt.Printf("!!!! Table %v Available is %v\n", tb.ID, tb.Available)
		if !tb.Available {
			allReplicaReady = false

			// set_accelerate_schedule
			if tb.HighPriority {
				tikvHelper.PostAccelerateSchedule(tb.ID)
			}

			// compute_sync_data_process
			regionReplica := make(map[int64]int)
			for _, store := range tiflashStores {
				statURL := fmt.Sprintf("%s://%s/tiflash/sync-status/%d",
					util.InternalHTTPSchema(),
					store.Store.StatusAddress,
					tb.ID,
				)
				fmt.Printf("!!!! startUrl %v\n", statURL)
				resp, err := util.InternalHTTPClient().Get(statURL)
				if err != nil {
					continue
				}

				defer func() {
					resp.Body.Close()
				}()

				reader := bufio.NewReader(resp.Body)
				ns, _, _ := reader.ReadLine()
				n, err := strconv.ParseInt(string(ns), 10, 64)
				if err != nil {
					return allReplicaReady, errors.Trace(err)
				}
				fmt.Printf("!!!! find total n %v\n", n)
				for i := int64(0); i < n; i++ {
					rs, _, _ := reader.ReadLine()
					// for (`table`, `store`), has region `r`
					r, err := strconv.ParseInt(strings.Trim(string(rs), "\r\n \t"), 10, 32)
					if err != nil {
						return allReplicaReady, errors.Trace(err)
					}
					fmt.Printf("!!!! find replica %v\n", r)
					fmt.Printf("!!!! find replica %v\n", r)
					if i, ok := regionReplica[r]; ok {
						regionReplica[r] = i + 1
					} else {
						regionReplica[r] = 1
					}
				}
			}


			// TODO Is it necessary, or we can get from TiDB?
			var stats helper.PDRegionStats
			if err = tikvHelper.GetPDRegionStats2(tb.ID, &stats); err != nil {
				fmt.Printf("!!!! err %v", err)
			}

			regionCount := stats.Count
			flashRegionCount := len(regionReplica)
			available := regionCount == flashRegionCount
			fmt.Printf("GetPDRegionStats output table %v RegionCount %v FlashRegionCount %v %v\n", tb.ID, regionCount, flashRegionCount, stats)

			d.UpdateTableReplicaInfo(ctx, tb.ID, available)
		}
	}

	return allReplicaReady, nil
}

// AlterTableSetTiFlashReplica sets the TiFlash replicas info.
func (d *ddl) AlterTableSetTiFlashReplica(ctx sessionctx.Context, ident ast.Ident, replicaInfo *ast.TiFlashReplicaSpec) error {
	schema, tb, err := d.getSchemaAndTableByIdent(ctx, ident)
	if err != nil {
		return errors.Trace(err)
	}
	// Ban setting replica count for tables in system database.
	if util.IsMemOrSysDB(schema.Name.L) {
		return errors.Trace(errUnsupportedAlterReplicaForSysTable)
	} else if tb.Meta().TempTableType != model.TempTableNone {
		return ErrOptOnTemporaryTable.GenWithStackByArgs("set tiflash replica")
	}

	tikvStore, ok := ctx.GetStore().(helper.Storage)
	if !ok {
		return errors.New("Can not get Helper")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}

	tbReplicaInfo := tb.Meta().TiFlashReplica
	if tbReplicaInfo != nil && tbReplicaInfo.Count == replicaInfo.Count &&
		len(tbReplicaInfo.LocationLabels) == len(replicaInfo.Labels) {
		changed := false
		for i, label := range tbReplicaInfo.LocationLabels {
			if replicaInfo.Labels[i] != label {
				changed = true
				break
			}
		}
		if !changed {
			return nil
		}
	}

	err = checkTiFlashReplicaCount(ctx, replicaInfo.Count)
	if err != nil {
		return errors.Trace(err)
	}

	// TODO maybe we should move into `updateVersionAndTableInfo`, since it can fail, and we shall rollback
	setRuleOk := false
	for retry := 0; retry < 2; retry++ {
		ruleNew := MakeNewRule(tb.Meta().ID, replicaInfo.Count, replicaInfo.Labels)
		fmt.Printf("Set new rule %v\n", ruleNew)
		if tikvHelper.SetPlacementRule(*ruleNew) == nil {
			setRuleOk = true
			break
		}
	}
	if setRuleOk == false {
		return errors.New("Can not set placement rule for TiFlash")
	}

	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tb.Meta().ID,
		SchemaName: schema.Name.L,
		Type:       model.ActionSetTiFlashReplica,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{*replicaInfo},
	}
	err = d.doDDLJob(ctx, job)
	err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

// GetDropOrTruncateTableInfoFromJobs gets the dropped/truncated table information from DDL jobs,
// it will use the `start_ts` of DDL job as snapshot to get the dropped/truncated table information.
func GetDropOrTruncateTableInfoFromJobsByStore(jobs []*model.Job, gcSafePoint uint64, store *kv.Storage, fn func(*model.Job, *model.TableInfo) (bool, error)) (bool, error) {
	for _, job := range jobs {
		// Check GC safe point for getting snapshot infoSchema.
		err := gcutil.ValidateSnapshotWithGCSafePoint(job.StartTS, gcSafePoint)
		if err != nil {
			return false, err
		}
		if job.Type != model.ActionDropTable && job.Type != model.ActionTruncateTable {
			continue
		}

		snapMeta := meta.NewSnapshotMeta((*store).GetSnapshot(kv.NewVersion(job.StartTS)))
		tbl, err := snapMeta.GetTable(job.SchemaID, job.TableID)
		if err != nil {
			if meta.ErrDBNotExists.Equal(err) {
				// The dropped/truncated DDL maybe execute failed that caused by the parallel DDL execution,
				// then can't find the table from the snapshot info-schema. Should just ignore error here,
				// see more in TestParallelDropSchemaAndDropTable.
				continue
			}
			return false, err
		}
		if tbl == nil {
			// The dropped/truncated DDL maybe execute failed that caused by the parallel DDL execution,
			// then can't find the table from the snapshot info-schema. Should just ignore error here,
			// see more in TestParallelDropSchemaAndDropTable.
			continue
		}
		finish, err := fn(job, tbl)
		if err != nil || finish {
			return finish, err
		}
	}
	return false, nil
}

func GetDropOrTruncateTableTiflash(ctx sessionctx.Context, currentSchema infoschema.InfoSchema, tikvHelper *helper.Helper, replicaInfos *[]PollTiFlashReplicaStatusContext) error {
	//store := domain.GetDomain(s).Store()
	store := tikvHelper.Store.(kv.Storage)

	txn, err := store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	gcSafePoint, err := gcutil.GetGCSafePoint(ctx)
	if err != nil {
		return err
	}
	uniqueIDMap := make(map[int64]struct{})
	handleJobAndTableInfo := func(job *model.Job, tblInfo *model.TableInfo) (bool, error) {
		// Avoid duplicate table ID info.
		if _, ok := currentSchema.TableByID(tblInfo.ID); ok {
			return false, nil
		}
		if _, ok := uniqueIDMap[tblInfo.ID]; ok {
			return false, nil
		}
		uniqueIDMap[tblInfo.ID] = struct{}{}
		GetTiFlashReplicaInfo(tblInfo, replicaInfos)
		return false, nil
	}
	fn := func(jobs []*model.Job) (bool, error) {
		return GetDropOrTruncateTableInfoFromJobsByStore(jobs, gcSafePoint, &store, handleJobAndTableInfo)
	}

	err = admin.IterAllDDLJobs(txn, fn)
	if err != nil {
		if terror.ErrorEqual(variable.ErrSnapshotTooOld, err) {
			// The err indicate that current ddl job and remain DDL jobs was been deleted by GC,
			// just ignore the error and return directly.
			return nil
		}
		return err
	}
	return nil
}

func HandlePlacementRuleRoutine(ctx sessionctx.Context, d *ddl, tableList []PollTiFlashReplicaStatusContext) error {
	// compute all_rules
	// TODO Need async remove allRules

	currentSchema := d.GetInfoSchemaWithInterceptor(ctx)

	tikvStore, ok := ctx.GetStore().(helper.Storage)
	if !ok {
		return errors.New("Can not get Helper")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}

	allRulesArr, err := tikvHelper.GetGroupRules("tiflash")
	if err != nil {
		return errors.Trace(err)
	}
	allRules := make(map[string]placement.Rule)
	for _, r := range allRulesArr {
		allRules[r.ID] = r
	}

	fmt.Printf("!!!! allRules is %v\n", allRules)

	// Cover getDropOrTruncateTableTiflash
	GetDropOrTruncateTableTiflash(ctx, currentSchema, tikvHelper, &tableList)
	for _, tb := range tableList {
		// for every region in each table, if it has one replica, we reckon it ready
		// TODO Can we batch request table?
		// implement _check_and_make_rule
		ruleId := fmt.Sprintf("table-%v-r", tb.ID)
		rule, ok := allRules[ruleId]
		if ok {
			match, ruleNew := isRuleMatch(rule, tb)
			if !match {
				fmt.Printf("!!!! Set rule %v\n", ruleNew)
				//tikvHelper.SetPlacementRule(*ruleNew)
			}
			delete(allRules, ruleId)
		} else {
			ruleNew := MakeNewRule(tb.ID, tb.Count, tb.LocationLabels)
			fmt.Printf("!!!! Set new rule %v\n", ruleNew)
			tikvHelper.SetPlacementRule(*ruleNew)
		}
	}

	// remove rules of non-existing table
	for _, v := range allRules {
		fmt.Printf("!!!! Remove rule %v\n", v.ID)
		//tikvHelper.DeletePlacementRule("tiflash", v.ID)
	}

	return nil
}
