// Copyright 2022 PingCAP, Inc.
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

package core

import (
	"context"
	"encoding/binary"
	"maps"
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// getSavedAnalyzeOpts gets the analyze options which are saved in mysql.analyze_options.
func (b *PlanBuilder) getSavedAnalyzeOpts(physicalID int64, tblInfo *model.TableInfo) (map[ast.AnalyzeOptionType]uint64, ast.ColumnChoice, []*model.ColumnInfo, error) {
	analyzeOptions := map[ast.AnalyzeOptionType]uint64{}
	exec := b.ctx.GetRestrictedSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, "select sample_num,sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id = %?", physicalID)
	if err != nil {
		return nil, ast.DefaultChoice, nil, err
	}
	if len(rows) <= 0 {
		return analyzeOptions, ast.DefaultChoice, nil, nil
	}

	row := rows[0]
	sampleNum := row.GetInt64(0)
	if sampleNum > 0 {
		analyzeOptions[ast.AnalyzeOptNumSamples] = uint64(sampleNum)
	}
	sampleRate := row.GetFloat64(1)
	if sampleRate > 0 {
		analyzeOptions[ast.AnalyzeOptSampleRate] = math.Float64bits(sampleRate)
	}
	buckets := row.GetInt64(2)
	if buckets > 0 {
		analyzeOptions[ast.AnalyzeOptNumBuckets] = uint64(buckets)
	}
	topn := row.GetInt64(3)
	if topn >= 0 {
		analyzeOptions[ast.AnalyzeOptNumTopN] = uint64(topn)
	}
	colType := row.GetEnum(4)
	switch colType.Name {
	case "ALL":
		return analyzeOptions, ast.AllColumns, tblInfo.Columns, nil
	case "LIST":
		colIDStrs := strings.Split(row.GetString(5), ",")
		colList := make([]*model.ColumnInfo, 0, len(colIDStrs))
		for _, colIDStr := range colIDStrs {
			colID, _ := strconv.ParseInt(colIDStr, 10, 64)
			colInfo := model.FindColumnInfoByID(tblInfo.Columns, colID)
			if colInfo != nil {
				colList = append(colList, colInfo)
			}
		}
		return analyzeOptions, ast.ColumnList, colList, nil
	case "PREDICATE":
		return analyzeOptions, ast.PredicateColumns, nil, nil
	default:
		return analyzeOptions, ast.DefaultChoice, nil, nil
	}
}

func mergeAnalyzeOptions(stmtOpts map[ast.AnalyzeOptionType]uint64, savedOpts map[ast.AnalyzeOptionType]uint64) map[ast.AnalyzeOptionType]uint64 {
	merged := map[ast.AnalyzeOptionType]uint64{}
	for optType := range ast.AnalyzeOptionString {
		if stmtOpt, ok := stmtOpts[optType]; ok {
			merged[optType] = stmtOpt
		} else if savedOpt, ok := savedOpts[optType]; ok {
			merged[optType] = savedOpt
		}
	}
	return merged
}

// pickColumnList picks the column list to be analyzed.
// If the column list is specified in the statement, we will use it.
func pickColumnList(astColChoice ast.ColumnChoice, astColList []*model.ColumnInfo, tblSavedColChoice ast.ColumnChoice, tblSavedColList []*model.ColumnInfo) (ast.ColumnChoice, []*model.ColumnInfo) {
	if astColChoice != ast.DefaultChoice {
		return astColChoice, astColList
	}
	return tblSavedColChoice, tblSavedColList
}

// buildAnalyzeTable constructs analyze tasks for each table.
func (b *PlanBuilder) buildAnalyzeTable(as *ast.AnalyzeTableStmt, opts map[ast.AnalyzeOptionType]uint64, version int) (base.Plan, error) {
	p := &Analyze{Opts: opts}
	p.OptionsMap = make(map[int64]V2AnalyzeOptions)
	usePersistedOptions := vardef.PersistAnalyzeOptions.Load()

	// Construct tasks for each table.
	for _, tbl := range as.TableNames {
		tnW := b.resolveCtx.GetTableName(tbl)
		if tnW.TableInfo.IsView() {
			return nil, errors.Errorf("analyze view %s is not supported now", tbl.Name.O)
		}
		if tnW.TableInfo.IsSequence() {
			return nil, errors.Errorf("analyze sequence %s is not supported now", tbl.Name.O)
		}

		idxInfo, colInfo := b.getColsInfo(tbl)
		physicalIDs, partitionNames, err := GetPhysicalIDsAndPartitionNames(tnW.TableInfo, as.PartitionNames)
		if err != nil {
			return nil, err
		}
		var commonHandleInfo *model.IndexInfo
		if version == statistics.Version2 {
			err = b.buildAnalyzeFullSamplingTask(as, p, physicalIDs, partitionNames, tnW, version, usePersistedOptions)
			if err != nil {
				return nil, err
			}
			continue
		}

		// Version 1 analyze.
		if as.ColumnChoice == ast.PredicateColumns {
			return nil, errors.Errorf("Only the version 2 of analyze supports analyzing predicate columns")
		}
		if as.ColumnChoice == ast.ColumnList {
			return nil, errors.Errorf("Only the version 2 of analyze supports analyzing the specified columns")
		}
		for _, idx := range idxInfo {
			// For prefix common handle. We don't use analyze mixed to handle it with columns. Because the full value
			// is read by coprocessor, the prefix index would get wrong stats in this case.
			if idx.Primary && tnW.TableInfo.IsCommonHandle && !idx.HasPrefixIndex() {
				commonHandleInfo = idx
				continue
			}
			if idx.MVIndex {
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("analyzing multi-valued indexes is not supported, skip %s", idx.Name.L))
				continue
			}
			if idx.IsColumnarIndex() {
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("analyzing columnar index is not supported, skip %s", idx.Name.L))
				continue
			}
			p.IdxTasks = append(p.IdxTasks, generateIndexTasks(idx, as, tnW.TableInfo, partitionNames, physicalIDs, version)...)
		}
		handleCols := BuildHandleColsForAnalyze(b.ctx, tnW.TableInfo, true, nil)
		if len(colInfo) > 0 || handleCols != nil {
			for i, id := range physicalIDs {
				if id == tnW.TableInfo.ID {
					id = statistics.NonPartitionTableID
				}
				info := AnalyzeInfo{
					DBName:        tbl.Schema.O,
					TableName:     tbl.Name.O,
					PartitionName: partitionNames[i],
					TableID:       statistics.AnalyzeTableID{TableID: tnW.TableInfo.ID, PartitionID: id},
					StatsVersion:  version,
				}
				p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{
					HandleCols:       handleCols,
					CommonHandleInfo: commonHandleInfo,
					ColsInfo:         colInfo,
					AnalyzeInfo:      info,
					TblInfo:          tnW.TableInfo,
				})
			}
		}
	}

	return p, nil
}

func (b *PlanBuilder) buildAnalyzeIndex(as *ast.AnalyzeTableStmt, opts map[ast.AnalyzeOptionType]uint64, version int) (base.Plan, error) {
	p := &Analyze{Opts: opts}
	statsHandle := domain.GetDomain(b.ctx).StatsHandle()
	if statsHandle == nil {
		return nil, errors.Errorf("statistics hasn't been initialized, please try again later")
	}
	tnW := b.resolveCtx.GetTableName(as.TableNames[0])
	tblInfo := tnW.TableInfo
	physicalIDs, names, err := GetPhysicalIDsAndPartitionNames(tblInfo, as.PartitionNames)
	if err != nil {
		return nil, err
	}
	versionIsSame := statsHandle.CheckAnalyzeVersion(tblInfo, physicalIDs, &version)
	if !versionIsSame {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead"))
	}
	if version == statistics.Version2 {
		return b.buildAnalyzeTable(as, opts, version)
	}
	for _, idxName := range as.IndexNames {
		if isPrimaryIndex(idxName) {
			handleCols := BuildHandleColsForAnalyze(b.ctx, tblInfo, true, nil)
			// FIXME: How about non-int primary key?
			if handleCols != nil && handleCols.IsInt() {
				for i, id := range physicalIDs {
					if id == tblInfo.ID {
						id = statistics.NonPartitionTableID
					}
					info := AnalyzeInfo{
						DBName:        as.TableNames[0].Schema.O,
						TableName:     as.TableNames[0].Name.O,
						PartitionName: names[i], TableID: statistics.AnalyzeTableID{TableID: tblInfo.ID, PartitionID: id},
						StatsVersion: version,
					}
					p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{HandleCols: handleCols, AnalyzeInfo: info, TblInfo: tblInfo})
				}
				continue
			}
		}
		idx := tblInfo.FindIndexByName(idxName.L)
		if idx == nil || idx.State != model.StatePublic {
			return nil, plannererrors.ErrAnalyzeMissIndex.GenWithStackByArgs(idxName.O, tblInfo.Name.O)
		}
		if idx.MVIndex {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("analyzing multi-valued indexes is not supported, skip %s", idx.Name.L))
			continue
		}
		if idx.IsColumnarIndex() {
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("analyzing columnar index is not supported, skip %s", idx.Name.L))
			continue
		}
		p.IdxTasks = append(p.IdxTasks, generateIndexTasks(idx, as, tblInfo, names, physicalIDs, version)...)
	}
	return p, nil
}

func (b *PlanBuilder) buildAnalyzeAllIndex(as *ast.AnalyzeTableStmt, opts map[ast.AnalyzeOptionType]uint64, version int) (base.Plan, error) {
	p := &Analyze{Opts: opts}
	statsHandle := domain.GetDomain(b.ctx).StatsHandle()
	if statsHandle == nil {
		return nil, errors.Errorf("statistics hasn't been initialized, please try again later")
	}
	tnW := b.resolveCtx.GetTableName(as.TableNames[0])
	tblInfo := tnW.TableInfo
	physicalIDs, names, err := GetPhysicalIDsAndPartitionNames(tblInfo, as.PartitionNames)
	if err != nil {
		return nil, err
	}
	versionIsSame := statsHandle.CheckAnalyzeVersion(tblInfo, physicalIDs, &version)
	if !versionIsSame {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("The analyze version from the session is not compatible with the existing statistics of the table. Use the existing version instead"))
	}
	if version == statistics.Version2 {
		return b.buildAnalyzeTable(as, opts, version)
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			if idx.MVIndex {
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("analyzing multi-valued indexes is not supported, skip %s", idx.Name.L))
				continue
			}
			if idx.IsColumnarIndex() {
				b.ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("analyzing columnar index is not supported, skip %s", idx.Name.L))
				continue
			}

			p.IdxTasks = append(p.IdxTasks, generateIndexTasks(idx, as, tblInfo, names, physicalIDs, version)...)
		}
	}
	handleCols := BuildHandleColsForAnalyze(b.ctx, tblInfo, true, nil)
	if handleCols != nil {
		for i, id := range physicalIDs {
			if id == tblInfo.ID {
				id = statistics.NonPartitionTableID
			}
			info := AnalyzeInfo{
				DBName:        as.TableNames[0].Schema.O,
				TableName:     as.TableNames[0].Name.O,
				PartitionName: names[i],
				TableID:       statistics.AnalyzeTableID{TableID: tblInfo.ID, PartitionID: id},
				StatsVersion:  version,
			}
			p.ColTasks = append(p.ColTasks, AnalyzeColumnsTask{HandleCols: handleCols, AnalyzeInfo: info, TblInfo: tblInfo})
		}
	}
	return p, nil
}

func generateIndexTasks(idx *model.IndexInfo, as *ast.AnalyzeTableStmt, tblInfo *model.TableInfo, names []string, physicalIDs []int64, version int) []AnalyzeIndexTask {
	if idx.Global {
		info := AnalyzeInfo{
			DBName:        as.TableNames[0].Schema.O,
			TableName:     as.TableNames[0].Name.O,
			PartitionName: "",
			TableID:       statistics.AnalyzeTableID{TableID: tblInfo.ID, PartitionID: statistics.NonPartitionTableID},
			StatsVersion:  version,
		}
		return []AnalyzeIndexTask{{IndexInfo: idx, AnalyzeInfo: info, TblInfo: tblInfo}}
	}

	indexTasks := make([]AnalyzeIndexTask, 0, len(physicalIDs))
	for i, id := range physicalIDs {
		if id == tblInfo.ID {
			id = statistics.NonPartitionTableID
		}
		info := AnalyzeInfo{
			DBName:        as.TableNames[0].Schema.O,
			TableName:     as.TableNames[0].Name.O,
			PartitionName: names[i],
			TableID:       statistics.AnalyzeTableID{TableID: tblInfo.ID, PartitionID: id},
			StatsVersion:  version,
		}
		indexTasks = append(indexTasks, AnalyzeIndexTask{IndexInfo: idx, AnalyzeInfo: info, TblInfo: tblInfo})
	}
	return indexTasks
}

// CMSketchSizeLimit indicates the size limit of CMSketch.
var CMSketchSizeLimit = kv.TxnEntrySizeLimit.Load() / binary.MaxVarintLen32

var analyzeOptionLimit = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets:    100000,
	ast.AnalyzeOptNumTopN:       100000,
	ast.AnalyzeOptCMSketchWidth: CMSketchSizeLimit,
	ast.AnalyzeOptCMSketchDepth: CMSketchSizeLimit,
	ast.AnalyzeOptNumSamples:    5000000,
	ast.AnalyzeOptSampleRate:    math.Float64bits(1),
}

// TODO(0xPoe): give some explanation about the default value.
var analyzeOptionDefault = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets:    256,
	ast.AnalyzeOptNumTopN:       20,
	ast.AnalyzeOptCMSketchWidth: 2048,
	ast.AnalyzeOptCMSketchDepth: 5,
	ast.AnalyzeOptNumSamples:    10000,
	ast.AnalyzeOptSampleRate:    math.Float64bits(0),
}

// TopN reduced from 500 to 100 due to concerns over large number of TopN values collected for customers with many tables.
// 100 is more inline with other databases. 100-256 is also common for NumBuckets with other databases.
var analyzeOptionDefaultV2 = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets:    statistics.DefaultHistogramBuckets,
	ast.AnalyzeOptNumTopN:       statistics.DefaultTopNValue,
	ast.AnalyzeOptCMSketchWidth: 2048,
	ast.AnalyzeOptCMSketchDepth: 5,
	ast.AnalyzeOptNumSamples:    0,
	ast.AnalyzeOptSampleRate:    math.Float64bits(-1),
}

// GetAnalyzeOptionDefaultV2ForTest returns the default analyze options for test.
func GetAnalyzeOptionDefaultV2ForTest() map[ast.AnalyzeOptionType]uint64 {
	return analyzeOptionDefaultV2
}

// This function very similar to handleAnalyzeOptions, but it's used for analyze version 2.
// Remove this function after we remove the support of analyze version 1.
func handleAnalyzeOptionsV2(opts []ast.AnalyzeOpt) (map[ast.AnalyzeOptionType]uint64, error) {
	optMap := make(map[ast.AnalyzeOptionType]uint64, len(analyzeOptionDefault))
	sampleNum, sampleRate := uint64(0), 0.0
	for _, opt := range opts {
		datumValue := opt.Value.(*driver.ValueExpr).Datum
		switch opt.Type {
		case ast.AnalyzeOptNumTopN:
			v := datumValue.GetUint64()
			if v > analyzeOptionLimit[opt.Type] {
				return nil, errors.Errorf("Value of analyze option %s should not be larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
			}
			optMap[opt.Type] = v
		case ast.AnalyzeOptSampleRate:
			// Only Int/Float/decimal is accepted, so pass nil here is safe.
			fVal, err := datumValue.ToFloat64(types.DefaultStmtNoWarningContext)
			if err != nil {
				return nil, err
			}
			limit := math.Float64frombits(analyzeOptionLimit[opt.Type])
			if fVal <= 0 || fVal > limit {
				return nil, errors.Errorf("Value of analyze option %s should not larger than %f, and should be greater than 0", ast.AnalyzeOptionString[opt.Type], limit)
			}
			sampleRate = fVal
			optMap[opt.Type] = math.Float64bits(fVal)
		default:
			v := datumValue.GetUint64()
			if opt.Type == ast.AnalyzeOptNumSamples {
				sampleNum = v
			}
			if v == 0 || v > analyzeOptionLimit[opt.Type] {
				return nil, errors.Errorf("Value of analyze option %s should be positive and not larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
			}
			optMap[opt.Type] = v
		}
	}
	if sampleNum > 0 && sampleRate > 0 {
		return nil, errors.Errorf("You can only either set the value of the sample num or set the value of the sample rate. Don't set both of them")
	}

	return optMap, nil
}

func fillAnalyzeOptionsV2(optMap map[ast.AnalyzeOptionType]uint64) map[ast.AnalyzeOptionType]uint64 {
	filledMap := make(map[ast.AnalyzeOptionType]uint64, len(analyzeOptionDefault))
	for key, defaultVal := range analyzeOptionDefaultV2 {
		if val, ok := optMap[key]; ok {
			filledMap[key] = val
		} else {
			filledMap[key] = defaultVal
		}
	}
	return filledMap
}

func handleAnalyzeOptions(opts []ast.AnalyzeOpt, statsVer int) (map[ast.AnalyzeOptionType]uint64, error) {
	optMap := make(map[ast.AnalyzeOptionType]uint64, len(analyzeOptionDefault))
	if statsVer == statistics.Version1 {
		maps.Copy(optMap, analyzeOptionDefault)
	} else {
		maps.Copy(optMap, analyzeOptionDefaultV2)
	}
	sampleNum, sampleRate := uint64(0), 0.0
	for _, opt := range opts {
		datumValue := opt.Value.(*driver.ValueExpr).Datum
		switch opt.Type {
		case ast.AnalyzeOptNumTopN:
			v := datumValue.GetUint64()
			if v > analyzeOptionLimit[opt.Type] {
				return nil, errors.Errorf("Value of analyze option %s should not be larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
			}
			optMap[opt.Type] = v
		case ast.AnalyzeOptSampleRate:
			// Only Int/Float/decimal is accepted, so pass nil here is safe.
			fVal, err := datumValue.ToFloat64(types.DefaultStmtNoWarningContext)
			if err != nil {
				return nil, err
			}
			if fVal > 0 && statsVer == statistics.Version1 {
				return nil, errors.Errorf("Version 1's statistics doesn't support the SAMPLERATE option, please set tidb_analyze_version to 2")
			}
			limit := math.Float64frombits(analyzeOptionLimit[opt.Type])
			if fVal <= 0 || fVal > limit {
				return nil, errors.Errorf("Value of analyze option %s should not larger than %f, and should be greater than 0", ast.AnalyzeOptionString[opt.Type], limit)
			}
			sampleRate = fVal
			optMap[opt.Type] = math.Float64bits(fVal)
		default:
			v := datumValue.GetUint64()
			if opt.Type == ast.AnalyzeOptNumSamples {
				sampleNum = v
			}
			if v == 0 || v > analyzeOptionLimit[opt.Type] {
				return nil, errors.Errorf("Value of analyze option %s should be positive and not larger than %d", ast.AnalyzeOptionString[opt.Type], analyzeOptionLimit[opt.Type])
			}
			optMap[opt.Type] = v
		}
	}
	if sampleNum > 0 && sampleRate > 0 {
		return nil, errors.Errorf("You can only either set the value of the sample num or set the value of the sample rate. Don't set both of them")
	}
	// Only version 1 has cmsketch.
	if statsVer == statistics.Version1 && optMap[ast.AnalyzeOptCMSketchWidth]*optMap[ast.AnalyzeOptCMSketchDepth] > CMSketchSizeLimit {
		return nil, errors.Errorf("cm sketch size(depth * width) should not larger than %d", CMSketchSizeLimit)
	}
	return optMap, nil
}
