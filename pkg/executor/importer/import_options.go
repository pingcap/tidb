// Copyright 2023 PingCAP, Inc.
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

package importer

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/expression"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pformat "github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/naming"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
	"go.uber.org/zap"
)

func (p *Plan) initDefaultOptions(ctx context.Context, targetNodeCPUCnt int, store tidbkv.Storage) {
	var threadCnt int
	threadCnt = int(math.Max(1, float64(targetNodeCPUCnt)*0.5))
	if p.DataSourceType == DataSourceTypeQuery {
		threadCnt = 2
	}
	p.Checksum = config.OpLevelRequired
	p.ThreadCnt = threadCnt
	p.MaxWriteSpeed = unlimitedWriteSpeed
	p.SplitFile = false
	p.MaxRecordedErrors = 100
	p.Detached = false
	p.DisableTiKVImportMode = false
	p.MaxEngineSize = getDefMaxEngineSize()
	p.CloudStorageURI = handle.GetCloudStorageURI(ctx, store)

	v := defaultCharacterSet
	p.Charset = &v
}

func getDefMaxEngineSize() config.ByteSize {
	if kerneltype.IsNextGen() {
		return config.DefaultBatchSize
	}
	return config.ByteSize(defaultMaxEngineSize)
}

func (p *Plan) initOptions(ctx context.Context, seCtx sessionctx.Context, options []*plannercore.LoadDataOpt) error {
	targetNodeCPUCnt, err := GetTargetNodeCPUCnt(ctx, p.DataSourceType, p.Path)
	if err != nil {
		return err
	}
	p.initDefaultOptions(ctx, targetNodeCPUCnt, seCtx.GetStore())

	specifiedOptions := map[string]*plannercore.LoadDataOpt{}
	for _, opt := range options {
		hasValue, ok := supportedOptions[opt.Name]
		if !ok {
			return exeerrors.ErrUnknownOption.FastGenByArgs(opt.Name)
		}
		if hasValue && opt.Value == nil || !hasValue && opt.Value != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if _, ok = specifiedOptions[opt.Name]; ok {
			return exeerrors.ErrDuplicateOption.FastGenByArgs(opt.Name)
		}
		specifiedOptions[opt.Name] = opt
	}
	p.specifiedOptions = specifiedOptions

	if kerneltype.IsNextGen() && sem.IsEnabled() {
		if p.DataSourceType == DataSourceTypeQuery {
			return plannererrors.ErrNotSupportedWithSem.GenWithStackByArgs("IMPORT INTO from select")
		}
		// we put the check here, not in planner, to make sure the cloud_storage_uri
		// won't change in between.
		if p.IsLocalSort() {
			return plannererrors.ErrNotSupportedWithSem.GenWithStackByArgs("IMPORT INTO with local sort")
		}
		for k := range disallowedOptionsOfNextGen {
			if _, ok := specifiedOptions[k]; ok {
				return exeerrors.ErrLoadDataUnsupportedOption.GenWithStackByArgs(k, "nextgen kernel")
			}
		}
	}

	if sem.IsEnabled() {
		for k := range disallowedOptionsForSEM {
			if _, ok := specifiedOptions[k]; ok {
				return exeerrors.ErrLoadDataUnsupportedOption.GenWithStackByArgs(k, "SEM enabled")
			}
		}
	}

	// DataFormatAuto means format is unspecified from stmt,
	// will validate below CSV options when init data files.
	if p.Format != DataFormatCSV && p.Format != DataFormatAuto {
		for k := range csvOnlyOptions {
			if _, ok := specifiedOptions[k]; ok {
				return exeerrors.ErrLoadDataUnsupportedOption.FastGenByArgs(k, "non-CSV format")
			}
		}
	}
	if p.DataSourceType == DataSourceTypeQuery {
		for k := range specifiedOptions {
			if _, ok := allowedOptionsOfImportFromQuery[k]; !ok {
				return exeerrors.ErrLoadDataUnsupportedOption.FastGenByArgs(k, "import from query")
			}
		}
	}

	optAsString := func(opt *plannercore.LoadDataOpt) (string, error) {
		if opt.Value.GetType(seCtx.GetExprCtx().GetEvalCtx()).GetType() != mysql.TypeVarString {
			return "", exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		val, isNull, err2 := opt.Value.EvalString(seCtx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err2 != nil || isNull {
			return "", exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		return val, nil
	}
	optAsInt64 := func(opt *plannercore.LoadDataOpt) (int64, error) {
		// current parser takes integer and bool as mysql.TypeLonglong
		if opt.Value.GetType(seCtx.GetExprCtx().GetEvalCtx()).GetType() != mysql.TypeLonglong || mysql.HasIsBooleanFlag(opt.Value.GetType(seCtx.GetExprCtx().GetEvalCtx()).GetFlag()) {
			return 0, exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		val, isNull, err2 := opt.Value.EvalInt(seCtx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err2 != nil || isNull {
			return 0, exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		return val, nil
	}
	if opt, ok := specifiedOptions[characterSetOption]; ok {
		v, err := optAsString(opt)
		if err != nil || v == "" {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		_, err = config.ParseCharset(v)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.Charset = &v
	}
	if opt, ok := specifiedOptions[fieldsTerminatedByOption]; ok {
		v, err := optAsString(opt)
		if err != nil || v == "" {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.FieldsTerminatedBy = v
	}
	if opt, ok := specifiedOptions[fieldsEnclosedByOption]; ok {
		v, err := optAsString(opt)
		if err != nil || len(v) > 1 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.FieldsEnclosedBy = v
	}
	if opt, ok := specifiedOptions[fieldsEscapedByOption]; ok {
		v, err := optAsString(opt)
		if err != nil || len(v) > 1 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.FieldsEscapedBy = v
	}
	if opt, ok := specifiedOptions[fieldsDefinedNullByOption]; ok {
		v, err := optAsString(opt)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.FieldNullDef = []string{v}
	}
	if opt, ok := specifiedOptions[linesTerminatedByOption]; ok {
		v, err := optAsString(opt)
		// cannot set terminator to empty string explicitly
		if err != nil || v == "" {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.LinesTerminatedBy = v
	}
	if opt, ok := specifiedOptions[skipRowsOption]; ok {
		vInt, err := optAsInt64(opt)
		if err != nil || vInt < 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.IgnoreLines = uint64(vInt)
	}
	if _, ok := specifiedOptions[splitFileOption]; ok {
		p.SplitFile = true
	}
	if opt, ok := specifiedOptions[diskQuotaOption]; ok {
		v, err := optAsString(opt)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = p.DiskQuota.UnmarshalText([]byte(v)); err != nil || p.DiskQuota <= 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[threadOption]; ok {
		vInt, err := optAsInt64(opt)
		if err != nil || vInt <= 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.ThreadCnt = int(vInt)
	}
	if opt, ok := specifiedOptions[maxWriteSpeedOption]; ok {
		v, err := optAsString(opt)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = p.MaxWriteSpeed.UnmarshalText([]byte(v)); err != nil || p.MaxWriteSpeed < 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[checksumTableOption]; ok {
		v, err := optAsString(opt)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = p.Checksum.FromStringValue(v); err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if opt, ok := specifiedOptions[groupKeyOption]; ok {
		v, err := optAsString(opt)
		if err != nil || v == "" || naming.CheckWithMaxLen(v, 256) != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.GroupKey = v
	}
	if opt, ok := specifiedOptions[recordErrorsOption]; ok {
		vInt, err := optAsInt64(opt)
		if err != nil || vInt < -1 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		p.MaxRecordedErrors = vInt
	}
	if _, ok := specifiedOptions[detachedOption]; ok {
		p.Detached = true
	}
	if _, ok := specifiedOptions[disableTiKVImportModeOption]; ok {
		p.DisableTiKVImportMode = true
	}
	if opt, ok := specifiedOptions[cloudStorageURIOption]; ok {
		v, err := optAsString(opt)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		// set cloud storage uri to empty string to force uses local sort when
		// the global variable is set.
		if v != "" {
			b, err := objstore.ParseBackend(v, nil)
			if err != nil {
				return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
			}
			// only support s3 and gcs now.
			if b.GetS3() == nil && b.GetGcs() == nil {
				return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
			}
		}
		p.CloudStorageURI = v
	}
	if opt, ok := specifiedOptions[maxEngineSizeOption]; ok {
		v, err := optAsString(opt)
		if err != nil {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
		if err = p.MaxEngineSize.UnmarshalText([]byte(v)); err != nil || p.MaxEngineSize < 0 {
			return exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
		}
	}
	if _, ok := specifiedOptions[disablePrecheckOption]; ok {
		p.DisablePrecheck = true
	}
	if _, ok := specifiedOptions[forceMergeStep]; ok {
		p.ForceMergeStep = true
	}
	if _, ok := specifiedOptions[manualRecoveryOption]; ok {
		p.ManualRecovery = true
	}

	if kerneltype.IsClassic() {
		if sv, ok := seCtx.GetSessionVars().GetSystemVar(vardef.TiDBMaxDistTaskNodes); ok {
			p.MaxNodeCnt = variable.TidbOptInt(sv, 0)
			if p.MaxNodeCnt == -1 { // -1 means calculate automatically
				p.MaxNodeCnt = scheduler.CalcMaxNodeCountByStoresNum(ctx, seCtx.GetStore())
			}
		}
	}

	// when split-file is set, data file will be split into chunks of 256 MiB.
	// skip_rows should be 0 or 1, we add this restriction to simplify skip_rows
	// logic, so we only need to skip on the first chunk for each data file.
	// CSV parser limit each row size to LargestEntryLimit(120M), the first row
	// will NOT cross file chunk.
	if p.SplitFile && p.IgnoreLines > 1 {
		return exeerrors.ErrInvalidOptionVal.FastGenByArgs("skip_rows, should be <= 1 when split-file is enabled")
	}

	if p.SplitFile && len(p.LinesTerminatedBy) == 0 {
		return exeerrors.ErrInvalidOptionVal.FastGenByArgs("lines_terminated_by, should not be empty when use split_file")
	}

	p.adjustOptions(targetNodeCPUCnt)
	return nil
}

func (p *Plan) adjustOptions(targetNodeCPUCnt int) {
	limit := targetNodeCPUCnt
	if p.DataSourceType == DataSourceTypeQuery {
		// for query, row is produced using 1 thread, the max cpu used is much
		// lower than import from file, so we set limit to 2*targetNodeCPUCnt.
		// TODO: adjust after spec is ready.
		limit *= 2
	}
	// max value is cpu-count
	if p.ThreadCnt > limit {
		log.L().Info("adjust IMPORT INTO thread count",
			zap.Int("before", p.ThreadCnt), zap.Int("after", limit))
		p.ThreadCnt = limit
	}
	if p.IsGlobalSort() {
		p.DisableTiKVImportMode = true
	}
}

func (p *Plan) initParameters(plan *plannercore.ImportInto) error {
	redactURL := ast.RedactURL(p.Path)
	var columnsAndVars, setClause string
	var sb strings.Builder
	formatCtx := pformat.NewRestoreCtx(pformat.DefaultRestoreFlags, &sb)
	if len(plan.ColumnsAndUserVars) > 0 {
		sb.WriteString("(")
		for i, col := range plan.ColumnsAndUserVars {
			if i > 0 {
				sb.WriteString(", ")
			}
			_ = col.Restore(formatCtx)
		}
		sb.WriteString(")")
		columnsAndVars = sb.String()
	}
	if len(plan.ColumnAssignments) > 0 {
		sb.Reset()
		for i, assign := range plan.ColumnAssignments {
			if i > 0 {
				sb.WriteString(", ")
			}
			_ = assign.Restore(formatCtx)
		}
		setClause = sb.String()
	}
	optionMap := make(map[string]any, len(plan.Options))
	for _, opt := range plan.Options {
		if opt.Value != nil {
			// The option attached to the import statement here are all
			// parameters entered by the user. TiDB will process the
			// parameters entered by the user as constant. so we can
			// directly convert it to constant.
			cons := opt.Value.(*expression.Constant)
			val := fmt.Sprintf("%v", cons.Value.GetValue())
			if opt.Name == cloudStorageURIOption {
				val = ast.RedactURL(val)
			}
			optionMap[opt.Name] = val
		} else {
			optionMap[opt.Name] = nil
		}
	}
	p.Parameters = &ImportParameters{
		ColumnsAndVars: columnsAndVars,
		SetClause:      setClause,
		FileLocation:   redactURL,
		Format:         p.Format,
		Options:        optionMap,
	}
	return nil
}
