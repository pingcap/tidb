// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	litconfig "github.com/pingcap/tidb/pkg/lightning/config"
	lightningmetric "github.com/pingcap/tidb/pkg/lightning/metric"
	lightningtikv "github.com/pingcap/tidb/pkg/lightning/tikv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	statshandle "github.com/pingcap/tidb/pkg/statistics/handle"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/chunk"
	utilcodec "github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/generatedexpr"
	"github.com/pingcap/tidb/pkg/util/intest"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	kvutil "github.com/tikv/client-go/v2/util"
	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	sd "github.com/tikv/pd/client/servicediscovery"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// MaxCommentLength is exported for testing.
	MaxCommentLength = 1024
)

var telemetryAddIndexIngestUsage = metrics.TelemetryAddIndexIngestCnt

const (
	tikvCapacitySourcePDGRPCStoreStats         = "pd_grpc_store_stats"
	tikvCapacitySourcePDHTTPCapacityMinusAvail = "pd_http_capacity_minus_available"
	pdStoreStatsRequestTimeout                 = 3 * time.Second

	defaultTiKVReplicaCount                         = 3
	tikvReplicaCountSourceInfoSchemaPlacementBundle = "infoschema_placement_bundle"
	tikvReplicaCountSourcePDMaxReplicas             = "pd_replicate_config_max_replicas"
	tikvReplicaCountSourceFallbackDefault           = "fallback_default_3"
	blockSamplePeakPredictionFactor                 = 3

	observedTiKVUsagePhaseTaskEnd  = "task_end"
	observedTiKVUsagePhasePostTask = "post_task"
)

var addIndexPostTaskObservationDelays = []struct {
	durationMultiplierNumerator   int
	durationMultiplierDenominator int
	minDelay                      time.Duration
}{
	{durationMultiplierNumerator: 1, durationMultiplierDenominator: 2, minDelay: 2 * time.Minute},
	{durationMultiplierNumerator: 1, durationMultiplierDenominator: 1, minDelay: 4 * time.Minute},
	{durationMultiplierNumerator: 2, durationMultiplierDenominator: 1, minDelay: 8 * time.Minute},
	{durationMultiplierNumerator: 3, durationMultiplierDenominator: 1, minDelay: 12 * time.Minute},
}

type observedTiKVCapacityIncrease struct {
	increase int64
	reliable bool
	reason   string
}

type observedTiKVUsageLogOptions struct {
	phase          string
	sequence       int
	scheduledDelay time.Duration
	observedAt     time.Time
}

type pdStoreStatsClient interface {
	GetClusterID(context.Context) uint64
	GetServiceDiscovery() sd.ServiceDiscovery
}

// DefaultCumulativeTimeout is the default cumulative timeout for analyze operation.
// exported for testing.
var DefaultCumulativeTimeout = 1 * time.Minute

// DefaultAnalyzeCheckInterval is the interval for checking analyze status.
// exported for testing.
var DefaultAnalyzeCheckInterval = 10 * time.Second

func buildIndexColumns(ctx *metabuild.Context, columns []*model.ColumnInfo, indexPartSpecifications []*ast.IndexPartSpecification, columnarIndexType model.ColumnarIndexType) ([]*model.IndexColumn, bool, error) {
	// Build offsets.
	idxParts := make([]*model.IndexColumn, 0, len(indexPartSpecifications))
	var col *model.ColumnInfo
	var mvIndex bool
	maxIndexLength := config.GetGlobalConfig().MaxIndexLength
	// The sum of length of all index columns.
	sumLength := 0
	for _, ip := range indexPartSpecifications {
		col = model.FindColumnInfo(columns, ip.Column.Name.L)
		if col == nil {
			return nil, false, dbterror.ErrKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ip.Column.Name)
		}
		if columnarIndexType == model.ColumnarIndexTypeVector && col.FieldType.GetType() != mysql.TypeTiDBVectorFloat32 {
			return nil, false, dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(fmt.Sprintf("only support vector type, but this is type: %s", col.FieldType.String()))
		}
		if columnarIndexType == model.ColumnarIndexTypeInverted && !types.IsTypeStoredAsInteger(col.FieldType.GetType()) {
			return nil, false, dbterror.ErrUnsupportedAddColumnarIndex.FastGenByArgs(fmt.Sprintf("only support integer type, but this is type: %s", col.FieldType.String()))
		}
		if columnarIndexType == model.ColumnarIndexTypeFulltext && !types.IsString(col.FieldType.GetType()) {
			return nil, false, dbterror.ErrUnsupportedAddColumnarIndex.FastGenByArgs(fmt.Sprintf("only support string type, but this is type: %s", col.FieldType.String()))
		}

		// return error in strict sql mode
		if columnarIndexType == model.ColumnarIndexTypeNA {
			if err := checkIndexColumn(col, ip.Length, ctx != nil && (!ctx.GetSQLMode().HasStrictMode() || ctx.SuppressTooLongIndexErr())); err != nil {
				return nil, false, err
			}
		}
		if col.FieldType.IsArray() {
			if mvIndex {
				return nil, false, dbterror.ErrNotSupportedYet.GenWithStackByArgs("more than one multi-valued key part per index")
			}
			mvIndex = true
		}
		indexColLen := ip.Length
		if indexColLen != types.UnspecifiedLength &&
			types.IsTypeChar(col.FieldType.GetType()) &&
			indexColLen == col.FieldType.GetFlen() {
			indexColLen = types.UnspecifiedLength
		}
		indexColumnLength, err := getIndexColumnLength(col, indexColLen, columnarIndexType)
		if err != nil {
			return nil, false, err
		}
		sumLength += indexColumnLength

		if (ctx == nil || !ctx.SuppressTooLongIndexErr()) && sumLength > maxIndexLength {
			// The sum of all lengths must be shorter than the max length for prefix.

			// The multiple column index and the unique index in which the length sum exceeds the maximum size
			// will return an error instead produce a warning.
			if ctx == nil || ctx.GetSQLMode().HasStrictMode() || mysql.HasUniKeyFlag(col.GetFlag()) || len(indexPartSpecifications) > 1 {
				return nil, false, dbterror.ErrTooLongKey.GenWithStackByArgs(sumLength, maxIndexLength)
			}
			// truncate index length and produce warning message in non-restrict sql mode.
			colLenPerUint, err := getIndexColumnLength(col, 1, columnarIndexType)
			if err != nil {
				return nil, false, err
			}
			indexColLen = maxIndexLength / colLenPerUint
			// produce warning message
			ctx.AppendWarning(dbterror.ErrTooLongKey.FastGenByArgs(sumLength, maxIndexLength))
		}

		idxParts = append(idxParts, &model.IndexColumn{
			Name:   col.Name,
			Offset: col.Offset,
			Length: indexColLen,
		})
	}

	return idxParts, mvIndex, nil
}

// CheckPKOnGeneratedColumn checks the specification of PK is valid.
func CheckPKOnGeneratedColumn(tblInfo *model.TableInfo, indexPartSpecifications []*ast.IndexPartSpecification) (*model.ColumnInfo, error) {
	var lastCol *model.ColumnInfo
	for _, colName := range indexPartSpecifications {
		lastCol = tblInfo.FindPublicColumnByName(colName.Column.Name.L)
		if lastCol == nil {
			return nil, dbterror.ErrKeyColumnDoesNotExits.GenWithStackByArgs(colName.Column.Name)
		}
		// Virtual columns cannot be used in primary key.
		if lastCol.IsVirtualGenerated() {
			if lastCol.Hidden {
				return nil, dbterror.ErrFunctionalIndexPrimaryKey
			}
			return nil, dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Defining a virtual generated column as primary key")
		}
	}

	return lastCol, nil
}

func checkIndexPrefixLength(columns []*model.ColumnInfo, idxColumns []*model.IndexColumn, columnarIndexType model.ColumnarIndexType) error {
	idxLen, err := indexColumnsLen(columns, idxColumns, columnarIndexType)
	if err != nil {
		return err
	}
	if idxLen > config.GetGlobalConfig().MaxIndexLength {
		return dbterror.ErrTooLongKey.GenWithStackByArgs(idxLen, config.GetGlobalConfig().MaxIndexLength)
	}
	return nil
}

func indexColumnsLen(cols []*model.ColumnInfo, idxCols []*model.IndexColumn, columnarIndexType model.ColumnarIndexType) (colLen int, err error) {
	for _, idxCol := range idxCols {
		col := model.FindColumnInfo(cols, idxCol.Name.L)
		if col == nil {
			err = dbterror.ErrKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", idxCol.Name.L)
			return
		}
		var l int
		l, err = getIndexColumnLength(col, idxCol.Length, columnarIndexType)
		if err != nil {
			return
		}
		colLen += l
	}
	return
}

// checkIndexColumn will be run for all non-columnar indexes.
func checkIndexColumn(col *model.ColumnInfo, indexColumnLen int, suppressTooLongKeyErr bool) error {
	if col.FieldType.GetType() == mysql.TypeNull || (col.GetFlen() == 0 && (types.IsTypeChar(col.FieldType.GetType()) || types.IsTypeVarchar(col.FieldType.GetType()))) {
		if col.Hidden {
			return errors.Trace(dbterror.ErrWrongKeyColumnFunctionalIndex.GenWithStackByArgs(col.GeneratedExprString))
		}
		return errors.Trace(dbterror.ErrWrongKeyColumn.GenWithStackByArgs(col.Name))
	}

	// JSON column cannot index.
	if col.FieldType.GetType() == mysql.TypeJSON && !col.FieldType.IsArray() {
		if col.Hidden {
			return dbterror.ErrFunctionalIndexOnJSONOrGeometryFunction
		}
		return errors.Trace(dbterror.ErrJSONUsedAsKey.GenWithStackByArgs(col.Name.O))
	}

	if col.FieldType.GetType() == mysql.TypeTiDBVectorFloat32 {
		return dbterror.ErrUnsupportedAddColumnarIndex.FastGen("only VECTOR INDEX can be added to vector column")
	}

	// Length must be specified and non-zero for BLOB and TEXT column indexes.
	if types.IsTypeBlob(col.FieldType.GetType()) {
		if indexColumnLen == types.UnspecifiedLength {
			if col.Hidden {
				return dbterror.ErrFunctionalIndexOnBlob
			}
			return errors.Trace(dbterror.ErrBlobKeyWithoutLength.GenWithStackByArgs(col.Name.O))
		}
		if indexColumnLen == types.ErrorLength {
			return errors.Trace(dbterror.ErrKeyPart0.GenWithStackByArgs(col.Name.O))
		}
	}

	// Length can only be specified for specifiable types.
	if indexColumnLen != types.UnspecifiedLength && !types.IsTypePrefixable(col.FieldType.GetType()) {
		return errors.Trace(dbterror.ErrIncorrectPrefixKey)
	}

	// Key length must be shorter or equal to the column length.
	if indexColumnLen != types.UnspecifiedLength &&
		types.IsTypeChar(col.FieldType.GetType()) {
		if col.GetFlen() < indexColumnLen {
			return errors.Trace(dbterror.ErrIncorrectPrefixKey)
		}
		// Length must be non-zero for char.
		if indexColumnLen == types.ErrorLength {
			return errors.Trace(dbterror.ErrKeyPart0.GenWithStackByArgs(col.Name.O))
		}
	}

	if types.IsString(col.FieldType.GetType()) {
		desc, err := charset.GetCharsetInfo(col.GetCharset())
		if err != nil {
			return err
		}
		indexColumnLen *= desc.Maxlen
	}
	// Specified length must be shorter than the max length for prefix.
	maxIndexLength := config.GetGlobalConfig().MaxIndexLength
	if indexColumnLen > maxIndexLength {
		if !suppressTooLongKeyErr {
			return dbterror.ErrTooLongKey.GenWithStackByArgs(indexColumnLen, maxIndexLength)
		}
	}
	return nil
}

// getIndexColumnLength calculate the bytes number required in an index column.
func getIndexColumnLength(col *model.ColumnInfo, colLen int, columnarIndexType model.ColumnarIndexType) (int, error) {
	if columnarIndexType != model.ColumnarIndexTypeNA {
		// Columnar index does not actually create KV index, so it has length of 0.
		// however 0 may cause some issues in other calculations, so we use 1 here.
		// 1 is also minimal enough anyway.
		return 1, nil
	}

	length := types.UnspecifiedLength
	if colLen != types.UnspecifiedLength {
		length = colLen
	} else if col.GetFlen() != types.UnspecifiedLength {
		length = col.GetFlen()
	}

	switch col.GetType() {
	case mysql.TypeBit:
		return (length + 7) >> 3, nil
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		// Different charsets occupy different numbers of bytes on each character.
		desc, err := charset.GetCharsetInfo(col.GetCharset())
		if err != nil {
			return 0, dbterror.ErrUnsupportedCharset.GenWithStackByArgs(col.GetCharset(), col.GetCollate())
		}
		return desc.Maxlen * length, nil
	case mysql.TypeTiny, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeShort:
		return mysql.DefaultLengthOfMysqlTypes[col.GetType()], nil
	case mysql.TypeFloat:
		if length <= mysql.MaxFloatPrecisionLength {
			return mysql.DefaultLengthOfMysqlTypes[mysql.TypeFloat], nil
		}
		return mysql.DefaultLengthOfMysqlTypes[mysql.TypeDouble], nil
	case mysql.TypeNewDecimal:
		return calcBytesLengthForDecimal(length), nil
	case mysql.TypeYear, mysql.TypeDate, mysql.TypeDuration, mysql.TypeDatetime, mysql.TypeTimestamp:
		return mysql.DefaultLengthOfMysqlTypes[col.GetType()], nil
	default:
		return length, nil
	}
}

// Set global index version for new global indexes.
// Version 1 is needed for non-clustered tables to prevent collisions after
// EXCHANGE PARTITION due to duplicate _tidb_rowid values.
// For non-unique indexes, the handle is always encoded in the key.
// For unique indexes with NULL values, the handle is also encoded in the key
// (since NULL != NULL, multiple NULLs are allowed).
// In both cases, we need the partition ID in the key to distinguish rows
// from different partitions that may have the same _tidb_rowid.
// Clustered tables don't have this issue and use version 0.
func setGlobalIndexVersion(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) {
	idxInfo.GlobalIndexVersion = 0
	if !model.GetGlobalIndexV1Supported() {
		return
	}
	if idxInfo.Global && !tblInfo.HasClusteredIndex() {
		needPartitionInKey := !idxInfo.Unique
		if !needPartitionInKey {
			nullCols := getNullColInfos(tblInfo, idxInfo.Columns)
			if len(nullCols) > 0 {
				needPartitionInKey = true
			}
		}
		if needPartitionInKey {
			idxInfo.GlobalIndexVersion = model.GlobalIndexVersionV1
			failpoint.Inject("SetGlobalIndexVersion", func(val failpoint.Value) {
				if valInt, ok := val.(int); ok {
					idxInfo.GlobalIndexVersion = uint8(valInt)
				}
			})
		}
	}
}

// decimal using a binary format that packs nine decimal (base 10) digits into four bytes.
func calcBytesLengthForDecimal(m int) int {
	return (m / 9 * 4) + ((m%9)+1)/2
}

// BuildIndexInfo builds a new IndexInfo according to the index information.
func BuildIndexInfo(
	ctx *metabuild.Context,
	tblInfo *model.TableInfo,
	indexName ast.CIStr,
	isPrimary, isUnique bool,
	columnarIndexType model.ColumnarIndexType,
	indexPartSpecifications []*ast.IndexPartSpecification,
	indexOption *ast.IndexOption,
	state model.SchemaState,
) (*model.IndexInfo, error) {
	if err := checkTooLongIndex(indexName); err != nil {
		return nil, errors.Trace(err)
	}

	// Create index info.
	idxInfo := &model.IndexInfo{
		Name:    indexName,
		State:   state,
		Primary: isPrimary,
		Unique:  isUnique,
	}

	switch columnarIndexType {
	case model.ColumnarIndexTypeVector:
		vectorInfo, _, err := buildVectorInfoWithCheck(indexPartSpecifications, tblInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		idxInfo.VectorInfo = vectorInfo
	case model.ColumnarIndexTypeInverted:
		invertedInfo, err := buildInvertedInfoWithCheck(indexPartSpecifications, tblInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		idxInfo.InvertedInfo = invertedInfo
	case model.ColumnarIndexTypeFulltext:
		ftsInfo, err := buildFullTextInfoWithCheck(indexPartSpecifications, indexOption, tblInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		idxInfo.FullTextInfo = ftsInfo
	}

	var err error
	allTableColumns := tblInfo.Columns
	idxInfo.Columns, idxInfo.MVIndex, err = buildIndexColumns(ctx, allTableColumns, indexPartSpecifications, columnarIndexType)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if indexOption != nil {
		idxInfo.Comment = indexOption.Comment
		if indexOption.Visibility == ast.IndexVisibilityInvisible {
			idxInfo.Invisible = true
		}
		if indexOption.Tp == ast.IndexTypeInvalid {
			// Use btree as default index type.
			idxInfo.Tp = ast.IndexTypeBtree
		} else {
			idxInfo.Tp = indexOption.Tp
		}
		idxInfo.Global = indexOption.Global
		setGlobalIndexVersion(tblInfo, idxInfo)

		conditionString, err := CheckAndBuildIndexConditionString(tblInfo, indexOption.Condition)
		if err != nil {
			return nil, errors.Trace(err)
		}
		idxInfo.ConditionExprString = conditionString
		idxInfo.AffectColumn, err = buildAffectColumn(idxInfo, tblInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		// Use btree as default index type.
		idxInfo.Tp = ast.IndexTypeBtree
	}

	return idxInfo, nil
}

func buildVectorInfoWithCheck(indexPartSpecifications []*ast.IndexPartSpecification,
	tblInfo *model.TableInfo) (*model.VectorIndexInfo, string, error) {
	if len(indexPartSpecifications) != 1 {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("unsupported no function")
	}

	idxPart := indexPartSpecifications[0]
	f, ok := idxPart.Expr.(*ast.FuncCallExpr)
	if !ok {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(fmt.Sprintf("unsupported function: %v", idxPart.Expr))
	}
	distanceMetric, ok := model.IndexableFnNameToDistanceMetric[f.FnName.L]
	if !ok {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("currently only L2 and Cosine distance is indexable")
	}
	colExpr, ok := f.Args[0].(*ast.ColumnNameExpr)
	if !ok {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(fmt.Sprintf("unsupported function args: %v", f.Args[0]))
	}
	colInfo := findColumnByName(colExpr.Name.Name.L, tblInfo)
	if colInfo == nil {
		return nil, "", infoschema.ErrColumnNotExists.GenWithStackByArgs(colExpr.Name.Name, tblInfo.Name)
	}

	// check duplicated function on the same column
	for _, idx := range tblInfo.Indices {
		if idx.VectorInfo == nil {
			continue
		}
		if idxCol := idx.FindColumnByName(colInfo.Name.L); idxCol == nil {
			continue
		}
		if idx.VectorInfo.DistanceMetric == distanceMetric {
			return nil, "", dbterror.ErrDupKeyName.GenWithStack(
				fmt.Sprintf("vector index %s function %s already exist on column %s",
					idx.Name, f.FnName, colInfo.Name))
		}
	}
	if colInfo.FieldType.GetFlen() <= 0 {
		return nil, "", errors.Errorf("add vector index can only be defined on fixed-dimension vector columns")
	}

	exprStr, err := restoreFuncCall(f)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	// It's used for build buildIndexColumns.
	idxPart.Column = &ast.ColumnName{Name: colInfo.Name}
	idxPart.Length = types.UnspecifiedLength

	return &model.VectorIndexInfo{
		Dimension:      uint64(colInfo.FieldType.GetFlen()),
		DistanceMetric: distanceMetric,
	}, exprStr, nil
}

func buildInvertedInfoWithCheck(indexPartSpecifications []*ast.IndexPartSpecification,
	tblInfo *model.TableInfo) (*model.InvertedIndexInfo, error) {
	if len(indexPartSpecifications) != 1 {
		return nil, dbterror.ErrUnsupportedAddColumnarIndex.FastGenByArgs("only support one column")
	}

	idxPart := indexPartSpecifications[0]
	if idxPart.Column == nil {
		return nil, dbterror.ErrUnsupportedAddColumnarIndex.FastGenByArgs("unsupported no column")
	}
	colInfo := findColumnByName(idxPart.Column.Name.L, tblInfo)
	if colInfo == nil {
		return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(idxPart.Column.Name, tblInfo.Name)
	}

	// check duplicated columnar index on the same column
	for _, idx := range tblInfo.Indices {
		if idx.InvertedInfo == nil {
			continue
		}
		if idxCol := idx.FindColumnByName(colInfo.Name.L); idxCol == nil {
			continue
		}
		if idx.Tp == ast.IndexTypeInverted {
			return nil, dbterror.ErrDupKeyName.GenWithStack(fmt.Sprintf("inverted columnar index %s already exist on column %s", idx.Name, colInfo.Name))
		}
	}

	// It's used for build buildIndexColumns.
	idxPart.Column = &ast.ColumnName{Name: colInfo.Name}
	idxPart.Length = types.UnspecifiedLength

	return model.FieldTypeToInvertedIndexInfo(colInfo.FieldType, colInfo.ID), nil
}

func buildFullTextInfoWithCheck(indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption,
	tblInfo *model.TableInfo) (*model.FullTextIndexInfo, error) {
	if len(indexPartSpecifications) != 1 {
		return nil, dbterror.ErrUnsupportedAddColumnarIndex.FastGen("FULLTEXT index only support one column")
	}
	idxPart := indexPartSpecifications[0]
	if idxPart.Column == nil {
		return nil, dbterror.ErrUnsupportedAddColumnarIndex.FastGen("FULLTEXT index only support one column")
	}
	if idxPart.Length != types.UnspecifiedLength {
		return nil, dbterror.ErrUnsupportedAddColumnarIndex.FastGen("FULLTEXT index does not support prefix length")
	}
	if idxPart.Desc {
		return nil, dbterror.ErrUnsupportedAddColumnarIndex.FastGen("FULLTEXT index does not support DESC order")
	}
	// The Default parser is STANDARD
	parser := model.FullTextParserTypeStandardV1
	if indexOption != nil && indexOption.ParserName.L != "" {
		parser = model.GetFullTextParserTypeBySQLName(indexOption.ParserName.L)
		if parser == model.FullTextParserTypeInvalid {
			// Actually indexOption must be valid. It is already checked in preprocessor.
			return nil, dbterror.ErrUnsupportedAddColumnarIndex.FastGen("fulltext index must specify a valid parser")
		}
	}
	colInfo := findColumnByName(idxPart.Column.Name.L, tblInfo)
	if colInfo == nil {
		return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(idxPart.Column.Name.L, tblInfo.Name)
	}
	for _, idx := range tblInfo.Indices {
		if idx.FullTextInfo == nil {
			continue
		}
		if idxCol := idx.FindColumnByName(colInfo.Name.L); idxCol == nil {
			continue
		}
		return nil, dbterror.ErrDupKeyName.GenWithStack(
			fmt.Sprintf("fulltext index '%s' already exist on column %s",
				idx.Name, colInfo.Name))
	}
	return &model.FullTextIndexInfo{
		ParserType: parser,
	}, nil
}

// AddIndexColumnFlag aligns the column flags of columns in TableInfo to IndexInfo.
func AddIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	if indexInfo.Primary {
		for _, col := range indexInfo.Columns {
			tblInfo.Columns[col.Offset].AddFlag(mysql.PriKeyFlag)
		}
		return
	}

	col := indexInfo.Columns[0]
	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[col.Offset].AddFlag(mysql.UniqueKeyFlag)
	} else {
		tblInfo.Columns[col.Offset].AddFlag(mysql.MultipleKeyFlag)
	}
}

// DropIndexColumnFlag drops the column flag of columns in TableInfo according to the IndexInfo.
func DropIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	if indexInfo.Primary {
		for _, col := range indexInfo.Columns {
			tblInfo.Columns[col.Offset].DelFlag(mysql.PriKeyFlag)
		}
	} else if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[indexInfo.Columns[0].Offset].DelFlag(mysql.UniqueKeyFlag)
	} else {
		tblInfo.Columns[indexInfo.Columns[0].Offset].DelFlag(mysql.MultipleKeyFlag)
	}

	col := indexInfo.Columns[0]
	// other index may still cover this col
	for _, index := range tblInfo.Indices {
		if index.Name.L == indexInfo.Name.L {
			continue
		}

		if index.Columns[0].Name.L != col.Name.L {
			continue
		}

		AddIndexColumnFlag(tblInfo, index)
	}
}

// ValidateRenameIndex checks if index name is ok to be renamed.
func ValidateRenameIndex(from, to ast.CIStr, tbl *model.TableInfo) (ignore bool, err error) {
	if fromIdx := tbl.FindIndexByName(from.L); fromIdx == nil {
		return false, errors.Trace(infoschema.ErrKeyNotExists.GenWithStackByArgs(from.O, tbl.Name))
	}
	// Take case-sensitivity into account, if `FromKey` and  `ToKey` are the same, nothing need to be changed
	if from.O == to.O {
		return true, nil
	}
	// If spec.FromKey.L == spec.ToKey.L, we operate on the same index(case-insensitive) and change its name (case-sensitive)
	// e.g: from `inDex` to `IndEX`. Otherwise, we try to rename an index to another different index which already exists,
	// that's illegal by rule.
	if toIdx := tbl.FindIndexByName(to.L); toIdx != nil && from.L != to.L {
		return false, errors.Trace(infoschema.ErrKeyNameDuplicate.GenWithStackByArgs(toIdx.Name.O))
	}
	return false, nil
}

func onRenameIndex(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	tblInfo, from, to, err := checkRenameIndex(jobCtx.metaMut, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Rename Index"))
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		// Store the mark and enter the next DDL handling loop.
		return updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, false)
	}

	renameIndexes(tblInfo, from, to)
	renameHiddenColumns(tblInfo, from, to)

	if ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func validateAlterIndexVisibility(ctx sessionctx.Context, indexName ast.CIStr, invisible bool, tbl *model.TableInfo) (bool, error) {
	var idx *model.IndexInfo
	if idx = tbl.FindIndexByName(indexName.L); idx == nil || idx.State != model.StatePublic {
		return false, errors.Trace(infoschema.ErrKeyNotExists.GenWithStackByArgs(indexName.O, tbl.Name))
	}
	if ctx == nil || ctx.GetSessionVars() == nil || ctx.GetSessionVars().StmtCtx.MultiSchemaInfo == nil {
		// Early return.
		if idx.Invisible == invisible {
			return true, nil
		}
	}
	if invisible && idx.IsColumnarIndex() {
		return false, dbterror.ErrUnsupportedIndexType.FastGen("INVISIBLE can not be used in %s INDEX", idx.Tp)
	}
	return false, nil
}

func onAlterIndexVisibility(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	tblInfo, from, invisible, err := checkAlterIndexVisibility(jobCtx.metaMut, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, false)
	}

	setIndexVisibility(tblInfo, from, invisible)
	if ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func setIndexVisibility(tblInfo *model.TableInfo, name ast.CIStr, invisible bool) {
	for _, idx := range tblInfo.Indices {
		if idx.Name.L == name.L || idx.GetChangingOriginName() == name.O {
			idx.Invisible = invisible
		}
	}
}

func getNullColInfos(tblInfo *model.TableInfo, cols []*model.IndexColumn) []*model.ColumnInfo {
	nullCols := make([]*model.ColumnInfo, 0, len(cols))
	for _, colName := range cols {
		col := model.FindColumnInfo(tblInfo.Columns, colName.Name.L)
		if !mysql.HasNotNullFlag(col.GetFlag()) || mysql.HasPreventNullInsertFlag(col.GetFlag()) {
			nullCols = append(nullCols, col)
		}
	}
	return nullCols
}

func checkPrimaryKeyNotNull(jobCtx *jobContext, w *worker, job *model.Job,
	tblInfo *model.TableInfo, indexInfo *model.IndexInfo) (warnings []string, err error) {
	if !indexInfo.Primary {
		return nil, nil
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(jobCtx.metaMut, job)
	if err != nil {
		return nil, err
	}
	nullCols := getNullColInfos(tblInfo, indexInfo.Columns)
	if len(nullCols) == 0 {
		return nil, nil
	}

	err = modifyColsFromNull2NotNull(
		jobCtx.stepCtx,
		w,
		dbInfo,
		tblInfo,
		nullCols,
		&model.ColumnInfo{Name: ast.NewCIStr("")},
		false,
	)
	if err == nil {
		return nil, nil
	}
	_, err = convertAddIdxJob2RollbackJob(jobCtx, job, tblInfo, []*model.IndexInfo{indexInfo}, err)
	// TODO: Support non-strict mode.
	// warnings = append(warnings, ErrWarnDataTruncated.GenWithStackByArgs(oldCol.Name.L, 0).Error())
	return nil, err
}

// moveAndUpdateHiddenColumnsToPublic updates the hidden columns to public, and
// moves the hidden columns to proper offsets, so that Table.Columns' states meet the assumption of
// [public, public, ..., public, non-public, non-public, ..., non-public].
func moveAndUpdateHiddenColumnsToPublic(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) {
	hiddenColOffset := make(map[int]struct{}, 0)
	for _, col := range idxInfo.Columns {
		if tblInfo.Columns[col.Offset].Hidden {
			hiddenColOffset[col.Offset] = struct{}{}
		}
	}
	if len(hiddenColOffset) == 0 {
		return
	}
	// Find the first non-public column.
	firstNonPublicPos := len(tblInfo.Columns) - 1
	for i, c := range tblInfo.Columns {
		if c.State != model.StatePublic {
			firstNonPublicPos = i
			break
		}
	}
	for _, col := range idxInfo.Columns {
		tblInfo.Columns[col.Offset].State = model.StatePublic
		if _, needMove := hiddenColOffset[col.Offset]; needMove {
			tblInfo.MoveColumnInfo(col.Offset, firstNonPublicPos)
		}
	}
}

func checkAndBuildIndexInfo(
	job *model.Job, tblInfo *model.TableInfo,
	columnarIndexType model.ColumnarIndexType, isPK bool, args *model.IndexArg,
) (*model.IndexInfo, error) {
	var err error
	indexInfo := tblInfo.FindIndexByName(args.IndexName.L)
	if indexInfo != nil {
		if indexInfo.State == model.StatePublic {
			err = dbterror.ErrDupKeyName.GenWithStack("index already exist %s", args.IndexName)
			if isPK {
				err = infoschema.ErrMultiplePriKey
			}
			return nil, err
		}
		return indexInfo, nil
	}

	for _, hiddenCol := range args.HiddenCols {
		columnInfo := model.FindColumnInfo(tblInfo.Columns, hiddenCol.Name.L)
		if columnInfo != nil && columnInfo.State == model.StatePublic {
			// We already have a column with the same column name.
			// TODO: refine the error message
			return nil, infoschema.ErrColumnExists.GenWithStackByArgs(hiddenCol.Name)
		}
	}

	if len(args.HiddenCols) > 0 {
		for _, hiddenCol := range args.HiddenCols {
			InitAndAddColumnToTable(tblInfo, hiddenCol)
		}
	}
	if err = checkAddColumnTooManyColumns(len(tblInfo.Columns)); err != nil {
		return nil, errors.Trace(err)
	}
	indexInfo, err = BuildIndexInfo(
		nil,
		tblInfo,
		args.IndexName,
		isPK,
		args.Unique,
		columnarIndexType,
		args.IndexPartSpecifications,
		args.IndexOption,
		model.StateNone,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if isPK {
		if _, err = CheckPKOnGeneratedColumn(tblInfo, args.IndexPartSpecifications); err != nil {
			return nil, err
		}
	}
	indexInfo.ID = AllocateIndexID(tblInfo)
	tblInfo.Indices = append(tblInfo.Indices, indexInfo)
	if err = checkTooManyIndexes(tblInfo.Indices); err != nil {
		return nil, errors.Trace(err)
	}
	// Here we need do this check before set state to `DeleteOnly`,
	// because if hidden columns has been set to `DeleteOnly`,
	// the `DeleteOnly` columns are missing when we do this check.
	if err := checkInvisibleIndexOnPK(tblInfo); err != nil {
		return nil, err
	}
	logutil.DDLLogger().Info("[ddl] run add index job", zap.String("job", job.String()), zap.Reflect("indexInfo", indexInfo))
	return indexInfo, nil
}

func (w *worker) onCreateColumnarIndex(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropIndex(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// Handle normal job.
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if err := checkTableTypeForColumnarIndex(tblInfo); err != nil {
		return ver, errors.Trace(err)
	}

	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	a := args.IndexArgs[0]
	columnarIndexType := a.GetColumnarIndexType()
	if columnarIndexType == model.ColumnarIndexTypeVector {
		a.IndexPartSpecifications[0].Expr, err = generatedexpr.ParseExpression(a.FuncExpr)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		defer func() {
			a.IndexPartSpecifications[0].Expr = nil
		}()
	}

	indexInfo, err := checkAndBuildIndexInfo(job, tblInfo, columnarIndexType, false, a)
	if err != nil {
		return ver, errors.Trace(err)
	}
	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StateNone:
		// none -> delete only
		indexInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		indexInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		indexInfo.State = model.StateWriteReorganization
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		// reorganization -> public
		tbl, err := getTable(jobCtx.getAutoIDRequirement(), schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		if job.IsCancelling() {
			return convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), []*model.IndexInfo{indexInfo}, dbterror.ErrCancelledDDLJob)
		}

		// Send sync schema notification to TiFlash.
		if job.SnapshotVer == 0 {
			currVer, err := getValidCurrentVersion(jobCtx.store)
			if err != nil {
				return ver, errors.Trace(err)
			}
			err = infosync.SyncTiFlashTableSchema(jobCtx.stepCtx, tbl.Meta().ID)
			if err != nil {
				return ver, errors.Trace(err)
			}
			job.SnapshotVer = currVer.Ver
			return ver, nil
		}

		// Check the progress of the TiFlash backfill index.
		var done bool
		done, ver, err = w.checkColumnarIndexProcessOnTiFlash(jobCtx, job, tbl, indexInfo)
		if err != nil || !done {
			return ver, err
		}

		indexInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		finishedArgs := &model.ModifyIndexArgs{
			IndexArgs:    []*model.IndexArg{{IndexID: indexInfo.ID}},
			PartitionIDs: getPartitionIDs(tblInfo),
			OpType:       model.OpAddIndex,
		}
		job.FillFinishedArgs(finishedArgs)

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		logutil.DDLLogger().Info("[ddl] run add columnar index job done",
			zap.Int64("ver", ver),
			zap.String("charset", job.Charset),
			zap.String("collation", job.Collate))
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", indexInfo.State)
	}

	return ver, errors.Trace(err)
}

func (w *worker) checkColumnarIndexProcessOnTiFlash(jobCtx *jobContext, job *model.Job, tbl table.Table, indexInfo *model.IndexInfo,
) (done bool, ver int64, err error) {
	err = w.checkColumnarIndexProcess(jobCtx, tbl, job, indexInfo)
	if err != nil {
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			return false, ver, nil
		}
		if !isRetryableJobError(err, job.ErrorCount) {
			logutil.DDLLogger().Warn("run add columnar index job failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), []*model.IndexInfo{indexInfo}, err)
		}
		return false, ver, errors.Trace(err)
	}

	return true, ver, nil
}

func (w *worker) checkColumnarIndexProcess(jobCtx *jobContext, tbl table.Table, job *model.Job, index *model.IndexInfo) error {
	waitTimeout := 500 * time.Millisecond
	ticker := time.NewTicker(waitTimeout)
	defer ticker.Stop()
	notAddedRowCnt := int64(-1)
	for {
		select {
		case <-w.ddlCtx.ctx.Done():
			return dbterror.ErrInvalidWorker.GenWithStack("worker is closed")
		case <-ticker.C:
			logutil.DDLLogger().Info(
				"index backfill state running, check columnar index process",
				zap.Stringer("job", job),
				zap.Stringer("index name", index.Name),
				zap.Int64("index ID", index.ID),
				zap.Duration("wait time", waitTimeout),
				zap.Int64("total added row count", job.RowCount),
				zap.Int64("not added row count", notAddedRowCnt))
			return dbterror.ErrWaitReorgTimeout
		default:
		}

		if !w.ddlCtx.isOwner() {
			// If it's not the owner, we will try later, so here just returns an error.
			logutil.DDLLogger().Info("DDL is not the DDL owner", zap.String("ID", w.ddlCtx.uuid))
			return errors.Trace(dbterror.ErrNotOwner)
		}

		isDone, notAddedIndexCnt, addedIndexCnt, err := w.checkColumnarIndexProcessOnce(jobCtx, tbl, index.ID)
		if err != nil {
			return errors.Trace(err)
		}
		notAddedRowCnt = notAddedIndexCnt
		job.RowCount = addedIndexCnt

		if isDone {
			break
		}
	}
	return nil
}

// checkColumnarIndexProcessOnce checks the backfill process of a columnar index from TiFlash once.
func (w *worker) checkColumnarIndexProcessOnce(jobCtx *jobContext, tbl table.Table, indexID int64) (
	isDone bool, notAddedIndexCnt, addedIndexCnt int64, err error) {
	failpoint.Inject("MockCheckColumnarIndexProcess", func(val failpoint.Value) {
		if valInt, ok := val.(int); ok {
			logutil.DDLLogger().Info("MockCheckColumnarIndexProcess", zap.Int("val", valInt))
			if valInt < 0 {
				failpoint.Return(false, 0, 0, dbterror.ErrTiFlashBackfillIndex.FastGenByArgs("mock a check error"))
			} else if valInt == 0 {
				failpoint.Return(false, 0, 0, nil)
			} else {
				failpoint.Return(true, 0, int64(valInt), nil)
			}
		}
	})

	sql := fmt.Sprintf("select rows_stable_not_indexed, rows_stable_indexed, error_message from information_schema.tiflash_indexes where table_id = %d and index_id = %d;",
		tbl.Meta().ID, indexID)
	rows, err := w.sess.Execute(jobCtx.stepCtx, sql, "add_vector_index_check_result")
	if err != nil || len(rows) == 0 {
		return false, 0, 0, errors.Trace(err)
	}

	// Get and process info from multiple TiFlash nodes.
	errMsg := ""
	for _, row := range rows {
		notAddedIndexCnt += row.GetInt64(0)
		addedIndexCnt += row.GetInt64(1)
		errMsg = row.GetString(2)
		if len(errMsg) != 0 {
			err = dbterror.ErrTiFlashBackfillIndex.FastGenByArgs(errMsg)
			break
		}
	}
	if err != nil {
		return false, 0, 0, errors.Trace(err)
	}
	if notAddedIndexCnt != 0 {
		return false, 0, 0, nil
	}

	return true, notAddedIndexCnt, addedIndexCnt, nil
}

func (w *worker) onCreateIndex(jobCtx *jobContext, job *model.Job, isPK bool) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropIndex(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// Handle normal job.
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Create Index"))
	}

	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	allIndexInfos := make([]*model.IndexInfo, 0, len(args.IndexArgs))
	for _, arg := range args.IndexArgs {
		indexInfo, err := checkAndBuildIndexInfo(job, tblInfo, model.ColumnarIndexTypeNA, job.Type == model.ActionAddPrimaryKey, arg)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// The condition in the index option is not marshaled, so we need to set it here.
		if len(arg.ConditionString) > 0 {
			indexInfo.ConditionExprString = arg.ConditionString
			// As we've updated the `ConditionExprString`, we need to rebuild the AffectColumn.
			indexInfo.AffectColumn, err = buildAffectColumn(indexInfo, tblInfo)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		}
		allIndexInfos = append(allIndexInfos, indexInfo)
	}

	originalState := allIndexInfos[0].State

SwitchIndexState:
	switch allIndexInfos[0].State {
	case model.StateNone:
		// none -> delete only
		err = initForReorgIndexes(w, job, allIndexInfos)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		err = preSplitIndexRegions(jobCtx.stepCtx, w.sess.Context, jobCtx.store, tblInfo, allIndexInfos, job.ReorgMeta, args)
		if err != nil {
			if !isRetryableJobError(err, job.ErrorCount) {
				job.State = model.JobStateCancelled
			}
			return ver, err
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteOnly
			moveAndUpdateHiddenColumnsToPublic(tblInfo, indexInfo)
		}
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != model.StateDeleteOnly)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteOnly
			_, err = checkPrimaryKeyNotNull(jobCtx, w, job, tblInfo, indexInfo)
			if err != nil {
				break SwitchIndexState
			}
		}

		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateWriteOnly)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteReorganization
			_, err = checkPrimaryKeyNotNull(jobCtx, w, job, tblInfo, indexInfo)
			if err != nil {
				break SwitchIndexState
			}
		}

		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateWriteReorganization)
		if err != nil {
			return ver, err
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		// reorganization -> public
		tbl, err := getTable(jobCtx.getAutoIDRequirement(), schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		switch job.ReorgMeta.AnalyzeState {
		case model.AnalyzeStateNone:
			// reorg the index data.
			var done bool
			done, ver, err = doReorgWorkForCreateIndex(w, jobCtx, job, tbl, allIndexInfos)
			if !done {
				return ver, err
			}
			// For multi-schema change, analyze is done by parent job.
			if job.MultiSchemaInfo == nil && checkNeedAnalyze(job, tblInfo) {
				job.ReorgMeta.AnalyzeState = model.AnalyzeStateRunning
			} else {
				job.ReorgMeta.AnalyzeState = model.AnalyzeStateSkipped
				checkAndMarkNonRevertible(job)
			}
		case model.AnalyzeStateRunning:
			intest.Assert(job.MultiSchemaInfo == nil, "multi schema change shouldn't reach here")
			w.startAnalyzeAndWait(job, tblInfo)
		case model.AnalyzeStateDone, model.AnalyzeStateSkipped, model.AnalyzeStateTimeout, model.AnalyzeStateFailed:
			// Set column index flag.
			for _, indexInfo := range allIndexInfos {
				AddIndexColumnFlag(tblInfo, indexInfo)
				if isPK {
					if err = UpdateColsNull2NotNull(tblInfo, indexInfo); err != nil {
						return ver, errors.Trace(err)
					}
				}
				indexInfo.State = model.StatePublic
			}

			// Inject the failpoint to prevent the progress of index creation.
			failpoint.Inject("create-index-stuck-before-public", func(v failpoint.Value) {
				if sigFile, ok := v.(string); ok {
					for {
						time.Sleep(1 * time.Second)
						if _, err := os.Stat(sigFile); err != nil {
							if os.IsNotExist(err) {
								continue
							}
							failpoint.Return(ver, errors.Trace(err))
						}
						break
					}
				}
			})

			ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StatePublic)
			if err != nil {
				return ver, errors.Trace(err)
			}

			a := &model.ModifyIndexArgs{
				PartitionIDs: getPartitionIDs(tbl.Meta()),
				OpType:       model.OpAddIndex,
			}
			for _, indexInfo := range allIndexInfos {
				a.IndexArgs = append(a.IndexArgs, &model.IndexArg{
					IndexID:  indexInfo.ID,
					IfExist:  false,
					IsGlobal: indexInfo.Global,
				})
			}
			job.FillFinishedArgs(a)

			analyzed := job.ReorgMeta.AnalyzeState == model.AnalyzeStateDone
			addIndexEvent := notifier.NewAddIndexEvent(tblInfo, allIndexInfos, analyzed)
			err2 := asyncNotifyEvent(jobCtx, addIndexEvent, job, noSubJob, w.sess)
			if err2 != nil {
				return ver, errors.Trace(err2)
			}

			// Finish this job.
			job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
			logutil.DDLLogger().Info("run add index job done",
				zap.String("charset", job.Charset),
				zap.String("collation", job.Collate))
		}
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", allIndexInfos[0].State)
	}

	return ver, errors.Trace(err)
}

func initForReorgIndexes(w *worker, job *model.Job, idxInfos []*model.IndexInfo) error {
	if len(idxInfos) == 0 {
		return nil
	}
	reorgTp, err := pickBackfillType(job)
	if err != nil {
		return err
	}
	// Partial Index is not supported without fast reorg.
	for _, indexInfo := range idxInfos {
		if (reorgTp == model.ReorgTypeTxn || reorgTp == model.ReorgTypeTxnMerge) && indexInfo.HasCondition() {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs("add partial index without fast reorg is not supported")
		}
	}
	loadCloudStorageURI(w, job)
	if reorgTp.NeedMergeProcess() {
		// Increase telemetryAddIndexIngestUsage
		telemetryAddIndexIngestUsage.Inc()
		for _, indexInfo := range idxInfos {
			indexInfo.BackfillState = model.BackfillStateRunning
		}
	}
	return nil
}

type analyzeStatus int

const (
	analyzeUnknown analyzeStatus = iota
	analyzeRunning
	analyzeFinished
	analyzeFailed
)

// queryAnalyzeStatusSince queries `analyze_jobs` for the specified table
// and start_time >= startTS. It returns one of analyzeStatus values. If the
// sessPool cannot provide a session or the query fails, it returns
// analyzeUnknown and a nil error so the caller may decide to proceed with
// starting ANALYZE.
func (w *worker) queryAnalyzeStatusSince(startTS uint64, dbName, tblName string) (analyzeStatus, error) {
	sessCtx, err := w.sessPool.Get()
	if err != nil {
		return analyzeUnknown, err
	}
	defer w.sessPool.Put(sessCtx)

	startTimeStr := time.Now().UTC().Format(time.DateTime)
	if startTS > 0 {
		startTimeStr = model.TSConvert2Time(startTS).UTC().Format(time.DateTime)
	}
	kctx := kv.WithInternalSourceType(w.ctx, kv.InternalTxnStats)

	// set session time zone to UTC to match the time format in `startTimeStr`
	originalTimeZone := sessCtx.GetSessionVars().TimeZone
	sessCtx.GetSessionVars().TimeZone = time.UTC
	defer func() {
		sessCtx.GetSessionVars().TimeZone = originalTimeZone
	}()
	exec := sessCtx.GetRestrictedSQLExecutor()
	rows, _, chkErr := exec.ExecRestrictedSQL(kctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		"SELECT state FROM mysql.analyze_jobs WHERE table_schema = %? AND table_name = %? AND start_time >= %?", dbName, tblName, startTimeStr)
	if chkErr != nil {
		return analyzeUnknown, chkErr
	}
	if len(rows) == 0 {
		return analyzeUnknown, nil
	}

	for _, r := range rows {
		var state string
		if !r.IsNull(0) {
			state = r.GetString(0)
		}
		switch {
		case strings.EqualFold(state, "running"):
			return analyzeRunning, nil
		case strings.EqualFold(state, "failed"):
			return analyzeFailed, nil
		case strings.EqualFold(state, "finished"):
			return analyzeFinished, nil
		default:
			// unknown state: continue checking other rows
		}
	}
	// If no recognizable state found, treat as unknown so caller may attempt to start ANALYZE.
	return analyzeUnknown, nil
}

// analyzeStatusDecision encapsulates the decision logic after observing
// the analyze status for a table. It returns three booleans:
//
//	done: whether the caller should consider analyze finished (true) and continue DDL;
//	timedOut: whether the analyze has exceeded cumulative timeout and caller should proceed with timeout handling;
//	failed: whether the analyze has failed;
//	proceed: whether the caller should proceed to start ANALYZE locally (true for unknown status).
func (w *worker) analyzeStatusDecision(job *model.Job, dbName, tblName string, status analyzeStatus, cumulativeTimeout time.Duration) (done, timedOut, failed, proceed bool) {
	switch status {
	case analyzeFinished:
		logutil.DDLLogger().Info("analyze already finished by other owner", zap.Int64("jobID", job.ID), zap.String("db", dbName), zap.String("table", tblName))
		w.ddlCtx.clearAnalyzeStartTime(job.ID)
		w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
		return true, false, false, false
	case analyzeFailed:
		logutil.DDLLogger().Warn("analyze previously failed on another owner, continue finishing DDL", zap.Int64("jobID", job.ID), zap.String("db", dbName), zap.String("table", tblName))
		w.ddlCtx.clearAnalyzeStartTime(job.ID)
		w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
		return true, false, true, false
	case analyzeRunning:
		// If analyze is running, respect cumulative timeout. If expired, return timeout to let caller proceed.
		if start, ok := w.ddlCtx.getAnalyzeStartTime(job.ID); ok {
			if time.Since(start) > cumulativeTimeout {
				logutil.DDLLogger().Warn("analyze table is running but exceeded cumulative timeout, proceeding to finish DDL", zap.Int64("jobID", job.ID), zap.Duration("elapsed", time.Since(start)))
				w.ddlCtx.clearAnalyzeStartTime(job.ID)
				w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
				return false, true, false, false
			}
		} else {
			w.ddlCtx.setAnalyzeStartTime(job.ID, time.Now())
		}
		select {
		case <-w.ctx.Done():
			logutil.DDLLogger().Info("analyze table after create index context done", zap.Int64("jobID", job.ID), zap.Error(w.ctx.Err()))
			w.ddlCtx.clearAnalyzeStartTime(job.ID)
			w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
			return true, false, false, false
		case <-time.After(DefaultAnalyzeCheckInterval):
			return false, false, false, false
		}
	default:
		// analyzeUnknown: caller should proceed to start ANALYZE locally.
		return false, false, false, true
	}
}

// doAnalyzeWithoutReorg performs analyze for the table if needed without the logic of
// reorg and and returns whether the table info is updated.
func (w *worker) doAnalyzeWithoutReorg(job *model.Job, tblInfo *model.TableInfo) (finished bool) {
	switch job.ReorgMeta.AnalyzeState {
	case model.AnalyzeStateNone:
		if checkNeedAnalyze(job, tblInfo) {
			// Start analyze for the next time.
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateRunning
		} else {
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateSkipped
		}
		return false
	case model.AnalyzeStateRunning:
		w.startAnalyzeAndWait(job, tblInfo)
		return false
	default:
		logutil.DDLLogger().Info("analyze skipped or finished for multi-schema change",
			zap.Int64("job", job.ID), zap.Int8("state", job.ReorgMeta.AnalyzeState))
		return true
	}
}

func (w *worker) startAnalyzeAndWait(job *model.Job, tblInfo *model.TableInfo) {
	done, timedOut, failed := w.analyzeTableInner(job, tblInfo, job.SchemaName)
	failpoint.InjectCall("analyzeTableDone", job)
	if done || timedOut || failed {
		if done {
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateDone
		}
		if timedOut {
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateTimeout
		}
		if failed {
			job.ReorgMeta.AnalyzeState = model.AnalyzeStateFailed
		}
	}
}

// analyzeTableInner analyzes the table after creating index/modify column.
func (w *worker) analyzeTableInner(job *model.Job, tblInfo *model.TableInfo, dbName string) (done, timedOut, failed bool) {
	doneCh := w.ddlCtx.getAnalyzeDoneCh(job.ID)
	tblName := tblInfo.Name.L
	cumulativeTimeout, found := w.ddlCtx.getAnalyzeCumulativeTimeout(job.ID)
	if !found {
		cumulativeTimeout = DefaultCumulativeTimeout
		if job.RealStartTS != 0 {
			addStart := model.TSConvert2Time(job.RealStartTS)
			elapsed := time.Since(addStart)
			if elapsed*2 > cumulativeTimeout {
				cumulativeTimeout = elapsed * 2
			}
		}
		w.ddlCtx.setAnalyzeCumulativeTimeout(job.ID, cumulativeTimeout)
	}

	if doneCh == nil {
		status, err := w.queryAnalyzeStatusSince(job.StartTS, dbName, tblName)
		if err != nil {
			logutil.DDLLogger().Warn("query analyze status failed", zap.Int64("jobID", job.ID), zap.Error(err))
			status = analyzeUnknown
		}

		done, timedOut, failed, proceed := w.analyzeStatusDecision(job, dbName, tblName, status, cumulativeTimeout)
		if done || timedOut || failed {
			return done, timedOut, failed
		}
		if !proceed {
			// We decided not to proceed to start ANALYZE locally (i.e. it's running and we waited),
			// so simply return and retry later.
			return false, false, false
		}

		if _, ok := w.ddlCtx.getAnalyzeStartTime(job.ID); !ok {
			w.ddlCtx.setAnalyzeStartTime(job.ID, time.Now())
		}

		// Use a buffered channel so analyze goroutine can always report an error
		// even after caller has timed out and moved on.
		doneCh = make(chan error, 1)
		eg := util.NewErrorGroupWithRecover()
		eg.Go(func() error {
			sessCtx, err := w.sessPool.Get()
			if err != nil {
				return err
			}
			defer func() {
				w.sessPool.Put(sessCtx)
				close(doneCh)
			}()
			dbTable := fmt.Sprintf("`%s`.`%s`", dbName, tblName)

			exec, ok := sessCtx.(sqlexec.RestrictedSQLExecutor)
			if !ok {
				return errors.Errorf("not restricted SQL executor: %T", sessCtx)
			}
			// internal sql may not init the analysis related variable correctly.
			err = statsutil.UpdateSCtxVarsForStats(sessCtx)
			if err != nil {
				return err
			}
			failpoint.InjectCall("beforeAnalyzeTable")
			_, _, err = exec.ExecRestrictedSQL(w.ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession, sqlexec.ExecOptionEnableDDLAnalyze}, "ANALYZE TABLE "+dbTable+";", "ddl analyze table")
			failpoint.InjectCall("afterAnalyzeTable", &err)
			if err != nil {
				logutil.DDLLogger().Warn("analyze table failed",
					zap.Int64("jobID", job.ID),
					zap.String("db", dbName),
					zap.String("table", tblName),
					zap.Error(err),
					zap.Stack("stack"))
				// We can continue to finish the job even if analyze table failed.
				doneCh <- err
			}
			return nil
		})
		w.ddlCtx.setAnalyzeDoneCh(job.ID, doneCh)
	}
	select {
	case err := <-doneCh:
		logutil.DDLLogger().Info("analyze table after create index done", zap.Int64("jobID", job.ID))
		w.ddlCtx.clearAnalyzeStartTime(job.ID)
		w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
		return true, false, err != nil
	case <-w.ctx.Done():
		logutil.DDLLogger().Info("analyze table after create index context done",
			zap.Int64("jobID", job.ID), zap.Error(w.ctx.Err()))
		w.ddlCtx.clearAnalyzeStartTime(job.ID)
		w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
		return true, false, false
	case <-time.After(DefaultAnalyzeCheckInterval):
		failpoint.Inject("mockAnalyzeTimeout", func(val failpoint.Value) {
			if v, ok := val.(int); ok {
				cumulativeTimeout = time.Duration(v) * time.Millisecond
			}
		})
		if start, ok := w.ddlCtx.getAnalyzeStartTime(job.ID); ok {
			if time.Since(start) > cumulativeTimeout {
				logutil.DDLLogger().Warn("analyze table after create index exceed cumulative timeout, proceeding to finish DDL",
					zap.Int64("jobID", job.ID), zap.Duration("elapsed", time.Since(start)))
				// Do not persist job here. Let the caller mark AnalyzeStateTimeout and persist.
				w.ddlCtx.clearAnalyzeStartTime(job.ID)
				w.ddlCtx.clearAnalyzeCumulativeTimeout(job.ID)
				return false, true, false
			}
		} else {
			w.ddlCtx.setAnalyzeStartTime(job.ID, time.Now())
		}
		return false, false, false
	}
}

func checkIfTableReorgWorkCanSkip(
	store kv.Storage,
	sessCtx sessionctx.Context,
	tbl table.Table,
	job *model.Job,
) bool {
	if job.SnapshotVer != 0 {
		// Reorg work has begun.
		return false
	}
	txn, err := sessCtx.Txn(false)
	validTxn := err == nil && txn != nil && txn.Valid()
	intest.Assert(validTxn)
	if !validTxn {
		logutil.DDLLogger().Warn("check if table is empty failed", zap.Error(err))
		return false
	}
	startTS := txn.StartTS()
	ctx := NewReorgContext()
	ctx.resourceGroupName = job.ReorgMeta.ResourceGroupName
	ctx.attachTopProfilingInfo(job.Query)
	if isEmpty, err := checkIfTableIsEmpty(ctx, store, tbl, startTS); err != nil || !isEmpty {
		return false
	}
	return true
}

// CheckImportIntoTableIsEmpty check import into table is empty or not.
func CheckImportIntoTableIsEmpty(
	store kv.Storage,
	sessCtx sessionctx.Context,
	tblInfo *model.TableInfo,
) (bool, error) {
	failpoint.Inject("checkImportIntoTableIsEmpty", func(_val failpoint.Value) {
		if val, ok := _val.(string); ok {
			switch val {
			case "error":
				failpoint.Return(false, errors.New("check is empty get error"))
			case "notEmpty":
				failpoint.Return(false, nil)
			}
		}
	})
	tbl, err := tables.TableFromMeta(autoid.Allocators{}, tblInfo)
	if err != nil {
		return false, err
	}
	txn, err := sessCtx.Txn(true)
	if err != nil {
		return false, err
	}
	validTxn := txn != nil && txn.Valid()
	if !validTxn {
		return false, errors.New("check if table is empty failed")
	}
	startTS := txn.StartTS()
	return checkIfTableIsEmpty(NewReorgContext(), store, tbl, startTS)
}

func checkIfTableIsEmpty(
	ctx *ReorgContext,
	store kv.Storage,
	tbl table.Table,
	startTS uint64,
) (bool, error) {
	if pTbl, ok := tbl.(table.PartitionedTable); ok {
		for _, pid := range pTbl.GetAllPartitionIDs() {
			pTbl := pTbl.GetPartition(pid)
			if isEmpty, err := checkIfPhysicalTableIsEmpty(ctx, store, pTbl, startTS); err != nil || !isEmpty {
				return false, err
			}
		}
		return true, nil
	}
	//nolint:forcetypeassert
	plainTbl := tbl.(table.PhysicalTable)
	return checkIfPhysicalTableIsEmpty(ctx, store, plainTbl, startTS)
}

func checkIfPhysicalTableIsEmpty(
	ctx *ReorgContext,
	store kv.Storage,
	tbl table.PhysicalTable,
	startTS uint64,
) (bool, error) {
	hasRecord, err := existsTableRow(ctx, store, tbl, startTS)
	intest.Assert(err == nil)
	if err != nil {
		logutil.DDLLogger().Warn("check if table is empty failed", zap.Error(err))
		return false, err
	}
	return !hasRecord, nil
}

func checkIfTempIndexReorgWorkCanSkip(
	store kv.Storage,
	sessCtx sessionctx.Context,
	tbl table.Table,
	allIndexInfos []*model.IndexInfo,
	job *model.Job,
) bool {
	failpoint.Inject("skipReorgWorkForTempIndex", func(val failpoint.Value) {
		if v, ok := val.(bool); ok {
			failpoint.Return(v)
		}
	})
	if job.SnapshotVer != 0 {
		// Reorg work has begun.
		return false
	}
	txn, err := sessCtx.Txn(false)
	validTxn := err == nil && txn != nil && txn.Valid()
	intest.Assert(validTxn)
	if !validTxn {
		logutil.DDLLogger().Warn("check if temp index is empty failed", zap.Error(err))
		return false
	}
	startTS := txn.StartTS()
	ctx := NewReorgContext()
	ctx.resourceGroupName = job.ReorgMeta.ResourceGroupName
	ctx.attachTopProfilingInfo(job.Query)
	firstIdxID := allIndexInfos[0].ID
	lastIdxID := allIndexInfos[len(allIndexInfos)-1].ID
	var globalIdxIDs []int64
	for _, idxInfo := range allIndexInfos {
		if idxInfo.Global {
			globalIdxIDs = append(globalIdxIDs, idxInfo.ID)
		}
	}
	return checkIfTempIndexIsEmpty(ctx, store, tbl, firstIdxID, lastIdxID, globalIdxIDs, startTS)
}

func checkIfTempIndexIsEmpty(
	ctx *ReorgContext,
	store kv.Storage,
	tbl table.Table,
	firstIdxID, lastIdxID int64,
	globalIdxIDs []int64,
	startTS uint64,
) bool {
	tblMetaID := tbl.Meta().ID
	if pTbl, ok := tbl.(table.PartitionedTable); ok {
		for _, pid := range pTbl.GetAllPartitionIDs() {
			if !checkIfTempIndexIsEmptyForPhysicalTable(ctx, store, pid, firstIdxID, lastIdxID, startTS) {
				return false
			}
		}
		for _, globalIdxID := range globalIdxIDs {
			if !checkIfTempIndexIsEmptyForPhysicalTable(ctx, store, tblMetaID, globalIdxID, globalIdxID, startTS) {
				return false
			}
		}
		return true
	}
	return checkIfTempIndexIsEmptyForPhysicalTable(ctx, store, tblMetaID, firstIdxID, lastIdxID, startTS)
}

func checkIfTempIndexIsEmptyForPhysicalTable(
	ctx *ReorgContext,
	store kv.Storage,
	pid int64,
	firstIdxID, lastIdxID int64,
	startTS uint64,
) bool {
	start, end := encodeTempIndexRange(pid, firstIdxID, lastIdxID)
	foundKey := false
	idxPrefix := tablecodec.GenTableIndexPrefix(pid)
	err := iterateSnapshotKeys(ctx, store, kv.PriorityLow, idxPrefix, startTS, start, end,
		func(_ kv.Handle, _ kv.Key, _ []byte) (more bool, err error) {
			foundKey = true
			return false, nil
		})
	intest.Assert(err == nil)
	if err != nil {
		logutil.DDLLogger().Info("check if temp index is empty failed", zap.Error(err))
		return false
	}
	return !foundKey
}

// pickBackfillType determines which backfill process will be used. The result is
// both stored in job.ReorgMeta.ReorgTp and returned.
func pickBackfillType(job *model.Job) (model.ReorgType, error) {
	if job.ReorgMeta.ReorgTp != model.ReorgTypeNone {
		// The backfill task has been started.
		// Don't change the backfill type.
		return job.ReorgMeta.ReorgTp, nil
	}
	if !job.ReorgMeta.IsFastReorg {
		job.ReorgMeta.ReorgTp = model.ReorgTypeTxn
		return model.ReorgTypeTxn, nil
	}
	if ingest.LitInitialized {
		if job.ReorgMeta.UseCloudStorage {
			job.ReorgMeta.ReorgTp = model.ReorgTypeIngest
			return model.ReorgTypeIngest, nil
		}
		if err := ingest.LitDiskRoot.PreCheckUsage(); err != nil {
			logutil.DDLIngestLogger().Info("ingest backfill is not available", zap.Error(err))
			return model.ReorgTypeNone, err
		}
		job.ReorgMeta.ReorgTp = model.ReorgTypeIngest
		return model.ReorgTypeIngest, nil
	}
	// The lightning environment is unavailable, but we can still use the txn-merge backfill.
	logutil.DDLLogger().Info("fallback to txn-merge backfill process",
		zap.Bool("lightning env initialized", ingest.LitInitialized))
	job.ReorgMeta.ReorgTp = model.ReorgTypeTxnMerge
	return model.ReorgTypeTxnMerge, nil
}

func loadCloudStorageURI(w *worker, job *model.Job) {
	jc := w.jobContext(job.ID, job.ReorgMeta)
	jc.cloudStorageURI = handle.GetCloudStorageURI(w.workCtx, w.store)
	job.ReorgMeta.UseCloudStorage = len(jc.cloudStorageURI) > 0 && job.ReorgMeta.IsDistReorg
	failpoint.InjectCall("afterLoadCloudStorageURI", job)
}

func doReorgWorkForCreateIndex(
	w *worker,
	jobCtx *jobContext,
	job *model.Job,
	tbl table.Table,
	allIndexInfos []*model.IndexInfo,
) (done bool, ver int64, err error) {
	var reorgTp model.ReorgType
	reorgTp, err = pickBackfillType(job)
	if err != nil {
		return false, ver, err
	}
	if !reorgTp.NeedMergeProcess() {
		skipReorg := checkIfTableReorgWorkCanSkip(w.store, w.sess.Session(), tbl, job)
		if skipReorg {
			logutil.DDLLogger().Info("table is empty, skipping reorg work",
				zap.Int64("jobID", job.ID),
				zap.String("table", tbl.Meta().Name.O))
			return true, ver, nil
		}
		return runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
	}
	switch allIndexInfos[0].BackfillState {
	case model.BackfillStateRunning:
		skipReorg := checkIfTableReorgWorkCanSkip(w.store, w.sess.Session(), tbl, job)
		if !skipReorg {
			logutil.DDLLogger().Info("index backfill state running",
				zap.Int64("job ID", job.ID), zap.String("table", tbl.Meta().Name.O),
				zap.Bool("ingest mode", reorgTp == model.ReorgTypeIngest),
				zap.String("index", allIndexInfos[0].Name.O))
			switch reorgTp {
			case model.ReorgTypeIngest:
				if job.ReorgMeta.IsDistReorg {
					done, ver, err = runIngestReorgJobDist(w, jobCtx, job, tbl, allIndexInfos)
				} else {
					done, ver, err = runIngestReorgJob(w, jobCtx, job, tbl, allIndexInfos)
				}
			case model.ReorgTypeTxnMerge:
				done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
			}
			if err != nil || !done {
				return false, ver, errors.Trace(err)
			}
		} else {
			failpoint.InjectCall("afterCheckTableReorgCanSkip")
			logutil.DDLLogger().Info("table is empty, skipping reorg work",
				zap.Int64("jobID", job.ID),
				zap.String("table", tbl.Meta().Name.O))
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateReadyToMerge
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tbl.Meta(), true)
		failpoint.InjectCall("afterBackfillStateRunningDone", job)
		return false, ver, errors.Trace(err)
	case model.BackfillStateReadyToMerge:
		failpoint.InjectCall("beforeBackfillMerge")
		logutil.DDLLogger().Info("index backfill state ready to merge",
			zap.Int64("job ID", job.ID),
			zap.String("table", tbl.Meta().Name.O),
			zap.String("index", allIndexInfos[0].Name.O))
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateMerging
		}
		job.SnapshotVer = 0 // Reset the snapshot version for merge index reorg.
		ver, err = updateVersionAndTableInfo(jobCtx, job, tbl.Meta(), true)
		return false, ver, errors.Trace(err)
	case model.BackfillStateMerging:
		skipReorg := checkIfTempIndexReorgWorkCanSkip(w.store, w.sess.Session(), tbl, allIndexInfos, job)
		if !skipReorg {
			done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, true)
			if !done {
				return false, ver, err
			}
		} else {
			failpoint.InjectCall("afterCheckTempIndexReorgCanSkip")
			logutil.DDLLogger().Info("temp index is empty, skipping reorg work",
				zap.Int64("jobID", job.ID),
				zap.String("table", tbl.Meta().Name.O))
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateInapplicable // Prevent double-write on this index.
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tbl.Meta(), true)
		return true, ver, errors.Trace(err)
	default:
		return false, 0, dbterror.ErrInvalidDDLState.GenWithStackByArgs("backfill", allIndexInfos[0].BackfillState)
	}
}

func runIngestReorgJobDist(w *worker, jobCtx *jobContext, job *model.Job,
	tbl table.Table, allIndexInfos []*model.IndexInfo) (done bool, ver int64, err error) {
	done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
	if err != nil {
		return false, ver, errors.Trace(err)
	}

	if !done {
		return false, ver, nil
	}

	return true, ver, nil
}

func runIngestReorgJob(w *worker, jobCtx *jobContext, job *model.Job,
	tbl table.Table, allIndexInfos []*model.IndexInfo) (done bool, ver int64, err error) {
	done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
	if err != nil {
		if kv.ErrKeyExists.Equal(err) {
			logutil.DDLLogger().Warn("import index duplicate key, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), allIndexInfos, err)
		} else if !isRetryableJobError(err, job.ErrorCount) {
			logutil.DDLLogger().Warn("run reorg job failed, convert job to rollback",
				zap.String("job", job.String()), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), allIndexInfos, err)
		} else {
			logutil.DDLLogger().Warn("run add index ingest job error", zap.Error(err))
		}
		return false, ver, errors.Trace(err)
	}
	failpoint.InjectCall("afterRunIngestReorgJob", job, done)
	return done, ver, nil
}

func isRetryableJobError(err error, jobErrCnt int64) bool {
	if jobErrCnt+1 >= vardef.GetDDLErrorCountLimit() {
		return false
	}
	return isRetryableError(err)
}

func isRetryableError(err error) bool {
	errMsg := err.Error()
	for _, m := range dbterror.ReorgRetryableErrMsgs {
		if strings.Contains(errMsg, m) {
			return true
		}
	}
	originErr := errors.Cause(err)
	if tErr, ok := originErr.(*terror.Error); ok {
		sqlErr := terror.ToSQLError(tErr)
		_, ok := dbterror.ReorgRetryableErrCodes[sqlErr.Code]
		return ok
	}
	// For the unknown errors, we should retry.
	return true
}

func runReorgJobAndHandleErr(
	w *worker,
	jobCtx *jobContext,
	job *model.Job,
	tbl table.Table,
	allIndexInfos []*model.IndexInfo,
	mergingTmpIdx bool,
) (done bool, ver int64, err error) {
	elements := make([]*meta.Element, 0, len(allIndexInfos))
	for _, indexInfo := range allIndexInfos {
		elements = append(elements, &meta.Element{ID: indexInfo.ID, TypeKey: meta.IndexElementKey})
	}

	failpoint.InjectCall("beforeRunReorgJobAndHandleErr", allIndexInfos)

	sctx, err1 := w.sessPool.Get()
	if err1 != nil {
		err = err1
		return
	}
	defer w.sessPool.Put(sctx)
	rh := newReorgHandler(sess.NewSession(sctx))
	dbInfo, err := jobCtx.metaMut.GetDatabase(job.SchemaID)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	reorgInfo, err := getReorgInfo(jobCtx.oldDDLCtx.jobContext(job.ID, job.ReorgMeta), jobCtx, rh, job, dbInfo, tbl, elements, mergingTmpIdx)
	if err != nil || reorgInfo == nil || reorgInfo.first {
		// If we run reorg firstly, we should update the job snapshot version
		// and then run the reorg next time.
		return false, ver, errors.Trace(err)
	}
	err = overwriteReorgInfoFromGlobalCheckpoint(w, rh.s, job, reorgInfo)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	err = w.runReorgJob(jobCtx, reorgInfo, tbl.Meta(), func() (addIndexErr error) {
		defer util.Recover(metrics.LabelDDL, "onCreateIndex",
			func() {
				addIndexErr = dbterror.ErrCancelledDDLJob.GenWithStack("add table `%v` index `%v` panic", tbl.Meta().Name, allIndexInfos[0].Name)
			}, false)
		return w.addTableIndex(jobCtx, tbl, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			return false, ver, nil
		}
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		// TODO(tangenta): get duplicate column and match index.
		err = ingest.TryConvertToKeyExistsErr(err, allIndexInfos[0], tbl.Meta())
		if !isRetryableJobError(err, job.ErrorCount) {
			logutil.DDLLogger().Warn("run add index job failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), allIndexInfos, err)
			if err1 := rh.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
				logutil.DDLLogger().Warn("run add index job failed, convert job to rollback, RemoveDDLReorgHandle failed", zap.Stringer("job", job), zap.Error(err1))
			}
		}
		return false, ver, errors.Trace(err)
	}

	failpoint.InjectCall("afterRunReorgJobAndHandleErr")
	return true, ver, nil
}

func onDropIndex(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	tblInfo, allIndexInfos, ifExists, err := checkDropIndex(jobCtx.infoCache, jobCtx.metaMut, job)
	if err != nil {
		if ifExists && dbterror.ErrCantDropFieldOrKey.Equal(err) {
			job.Warning = toTError(err)
			job.State = model.JobStateDone
			return ver, nil
		}
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Drop Index"))
	}

	if job.MultiSchemaInfo != nil && !job.IsRollingback() && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		job.SchemaState = allIndexInfos[0].State
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, false)
	}

	originalState := allIndexInfos[0].State
	switch allIndexInfos[0].State {
	case model.StatePublic:
		// public -> write only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteOnly
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateWriteOnly)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteOnly
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateDeleteOnly)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteOnly:
		// delete only -> reorganization
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteReorganization
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateDeleteReorganization)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteReorganization:
		// reorganization -> absent
		isColumnarIndex := false
		indexIDs := make([]int64, 0, len(allIndexInfos))
		for _, indexInfo := range allIndexInfos {
			if indexInfo.IsColumnarIndex() {
				isColumnarIndex = true
			}
			indexInfo.State = model.StateNone
			// Set column index flag.
			DropIndexColumnFlag(tblInfo, indexInfo)
			RemoveDependentHiddenColumns(tblInfo, indexInfo)
			removeIndexInfo(tblInfo, indexInfo)
			indexIDs = append(indexIDs, indexInfo.ID)
		}

		failpoint.Inject("mockExceedErrorLimit", func(val failpoint.Value) {
			//nolint:forcetypeassert
			if val.(bool) {
				panic("panic test in cancelling add index")
			}
		})

		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != model.StateNone)
		if err != nil {
			return ver, errors.Trace(err)
		}

		if isColumnarIndex {
			// Send sync schema notification to TiFlash.
			if err := infosync.SyncTiFlashTableSchema(jobCtx.stepCtx, tblInfo.ID); err != nil {
				logutil.DDLLogger().Warn("run drop column index but syncing TiFlash schema failed", zap.Error(err))
			}
		}

		// Finish this job.
		if job.IsRollingback() {
			dropArgs, err := model.GetFinishedModifyIndexArgs(job)
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}

			// Convert drop index args to finished add index args again to finish add index job.
			// Only rolled back add index jobs will get here, since drop index jobs can only be cancelled, not rolled back.
			addIndexArgs := &model.ModifyIndexArgs{
				PartitionIDs: dropArgs.PartitionIDs,
				OpType:       model.OpAddIndex,
			}
			for i, indexID := range indexIDs {
				addIndexArgs.IndexArgs = append(addIndexArgs.IndexArgs,
					&model.IndexArg{
						IndexID: indexID,
						IfExist: dropArgs.IndexArgs[i].IfExist,
					})
			}
			job.FillFinishedArgs(addIndexArgs)
		} else {
			// the partition ids were append by convertAddIdxJob2RollbackJob, it is weird, but for the compatibility,
			// we should keep appending the partitions in the convertAddIdxJob2RollbackJob.
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			// Global index key has t{tableID}_ prefix.
			// Assign partitionIDs empty to guarantee correct prefix in insertJobIntoDeleteRangeTable.
			dropArgs, err := model.GetDropIndexArgs(job)
			dropArgs.OpType = model.OpDropIndex
			if err != nil {
				return ver, errors.Trace(err)
			}
			dropArgs.IndexArgs[0].IndexID = indexIDs[0]
			dropArgs.IndexArgs[0].IsColumnar = allIndexInfos[0].IsColumnarIndex()
			dropArgs.IndexArgs[0].ColumnarIndexType = allIndexInfos[0].GetColumnarIndexType()
			if !allIndexInfos[0].Global {
				dropArgs.PartitionIDs = getPartitionIDs(tblInfo)
			}
			job.FillFinishedArgs(dropArgs)
		}
	default:
		return ver, errors.Trace(dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", allIndexInfos[0].State))
	}
	job.SchemaState = allIndexInfos[0].State
	return ver, errors.Trace(err)
}

// RemoveDependentHiddenColumns removes hidden columns by the indexInfo.
func RemoveDependentHiddenColumns(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) {
	hiddenColOffs := make([]int, 0)
	for _, indexColumn := range idxInfo.Columns {
		col := tblInfo.Columns[indexColumn.Offset]
		if col.Hidden {
			hiddenColOffs = append(hiddenColOffs, col.Offset)
		}
	}
	// Sort the offset in descending order.
	slices.SortFunc(hiddenColOffs, func(a, b int) int { return cmp.Compare(b, a) })
	// Move all the dependent hidden columns to the end.
	endOffset := len(tblInfo.Columns) - 1
	for _, offset := range hiddenColOffs {
		tblInfo.MoveColumnInfo(offset, endOffset)
	}
	tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-len(hiddenColOffs)]
}

func removeIndexInfo(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) {
	indices := tblInfo.Indices
	offset := -1
	for i, idx := range indices {
		if idxInfo.ID == idx.ID {
			offset = i
			break
		}
	}
	if offset == -1 {
		// The target index has been removed.
		return
	}
	// Remove the target index.
	tblInfo.Indices = slices.Delete(tblInfo.Indices, offset, offset+1)
}

func checkDropIndex(infoCache *infoschema.InfoCache, t *meta.Mutator, job *model.Job) (*model.TableInfo, []*model.IndexInfo, bool /* ifExists */, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}

	args, err := model.GetDropIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, false, errors.Trace(err)
	}

	indexInfos := make([]*model.IndexInfo, 0, len(args.IndexArgs))
	for _, idxArg := range args.IndexArgs {
		indexInfo := tblInfo.FindIndexByName(idxArg.IndexName.L)
		if indexInfo == nil {
			job.State = model.JobStateCancelled
			return nil, nil, idxArg.IfExist, dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", idxArg.IndexName)
		}

		// Check that drop primary index will not cause invisible implicit primary index.
		if err := checkInvisibleIndexesOnPK(tblInfo, []*model.IndexInfo{indexInfo}, job); err != nil {
			job.State = model.JobStateCancelled
			return nil, nil, false, errors.Trace(err)
		}

		// Double check for drop index needed in foreign key.
		if err := checkIndexNeededInForeignKeyInOwner(infoCache, job, job.SchemaName, tblInfo, indexInfo); err != nil {
			return nil, nil, false, errors.Trace(err)
		}
		indexInfos = append(indexInfos, indexInfo)
	}
	return tblInfo, indexInfos, false, nil
}

func checkInvisibleIndexesOnPK(tblInfo *model.TableInfo, indexInfos []*model.IndexInfo, job *model.Job) error {
	newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, oidx := range tblInfo.Indices {
		needAppend := true
		for _, idx := range indexInfos {
			if idx.Name.L == oidx.Name.L {
				needAppend = false
				break
			}
		}
		if needAppend {
			newIndices = append(newIndices, oidx)
		}
	}
	newTbl := tblInfo.Clone()
	newTbl.Indices = newIndices
	if err := checkInvisibleIndexOnPK(newTbl); err != nil {
		job.State = model.JobStateCancelled
		return err
	}

	return nil
}

func checkRenameIndex(t *meta.Mutator, job *model.Job) (tblInfo *model.TableInfo, from, to ast.CIStr, err error) {
	schemaID := job.SchemaID
	tblInfo, err = GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, from, to, errors.Trace(err)
	}

	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}
	from, to = args.GetRenameIndexes()

	// Double check. See function `RenameIndex` in executor.go
	duplicate, err := ValidateRenameIndex(from, to, tblInfo)
	if duplicate {
		return nil, from, to, nil
	}
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}
	return tblInfo, from, to, errors.Trace(err)
}

func checkAlterIndexVisibility(t *meta.Mutator, job *model.Job) (*model.TableInfo, ast.CIStr, bool, error) {
	var (
		indexName ast.CIStr
		invisible bool
	)

	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, indexName, invisible, errors.Trace(err)
	}

	args, err := model.GetAlterIndexVisibilityArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, indexName, invisible, errors.Trace(err)
	}
	indexName, invisible = args.IndexName, args.Invisible

	skip, err := validateAlterIndexVisibility(nil, indexName, invisible, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, indexName, invisible, errors.Trace(err)
	}
	if skip {
		job.State = model.JobStateDone
		return nil, indexName, invisible, nil
	}
	return tblInfo, indexName, invisible, nil
}

// indexRecord is the record information of an index.
type indexRecord struct {
	handle kv.Handle
	key    []byte        // It's used to lock a record. Record it to reduce the encoding time.
	vals   []types.Datum // It's the index values.
	rsData []types.Datum // It's the restored data for handle.
	skip   bool          // skip indicates that the index key is already exists, we should not add it.
}

type baseIndexWorker struct {
	*backfillCtx
	indexes []table.Index

	tp backfillerType
	// The following attributes are used to reduce memory allocation.
	defaultVals []types.Datum
	idxRecords  []*indexRecord
	rowMap      map[int64]types.Datum
	rowDecoder  *decoder.RowDecoder
}

type addIndexTxnWorker struct {
	baseIndexWorker

	// The following attributes are used to reduce memory allocation.
	idxKeyBufs         [][]byte
	batchCheckKeys     []kv.Key
	batchCheckValues   [][]byte
	distinctCheckFlags []bool
	recordIdx          []int
}

func newAddIndexTxnWorker(
	decodeColMap map[int64]decoder.Column,
	t table.PhysicalTable,
	bfCtx *backfillCtx,
	job *model.Job,
	elements []*meta.Element,
	currElement *meta.Element,
) (*addIndexTxnWorker, error) {
	if !bytes.Equal(currElement.TypeKey, meta.IndexElementKey) {
		logutil.DDLLogger().Error("Element type for addIndexTxnWorker incorrect",
			zap.Int64("job ID", job.ID), zap.ByteString("element type", currElement.TypeKey), zap.Int64("element ID", elements[0].ID))
		return nil, errors.Errorf("element type is not index, typeKey: %v", currElement.TypeKey)
	}

	allIndexes := make([]table.Index, 0, len(elements))
	for _, elem := range elements {
		if !bytes.Equal(elem.TypeKey, meta.IndexElementKey) {
			continue
		}
		indexInfo := model.FindIndexInfoByID(t.Meta().Indices, elem.ID)
		index, err := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
		if err != nil {
			return nil, err
		}
		allIndexes = append(allIndexes, index)
	}
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)

	return &addIndexTxnWorker{
		baseIndexWorker: baseIndexWorker{
			backfillCtx: bfCtx,
			indexes:     allIndexes,
			rowDecoder:  rowDecoder,
			defaultVals: make([]types.Datum, len(t.WritableCols())),
			rowMap:      make(map[int64]types.Datum, len(decodeColMap)),
		},
	}, nil
}

func (w *baseIndexWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

func (w *baseIndexWorker) String() string {
	return w.tp.String()
}

func (w *baseIndexWorker) GetCtx() *backfillCtx {
	return w.backfillCtx
}

// mockNotOwnerErrOnce uses to make sure `notOwnerErr` only mock error once.
var mockNotOwnerErrOnce uint32

// getIndexRecord gets index columns values use w.rowDecoder, and generate indexRecord.
func (w *baseIndexWorker) getIndexRecord(idxInfo *model.IndexInfo, handle kv.Handle, recordKey []byte) (*indexRecord, error) {
	cols := w.table.WritableCols()
	failpoint.Inject("MockGetIndexRecordErr", func(val failpoint.Value) {
		if valStr, ok := val.(string); ok {
			switch valStr {
			case "cantDecodeRecordErr":
				failpoint.Return(nil, errors.Trace(dbterror.ErrCantDecodeRecord.GenWithStackByArgs("index",
					errors.New("mock can't decode record error"))))
			case "modifyColumnNotOwnerErr":
				if idxInfo.Name.O == "_Idx$_idx_0" && handle.IntValue() == 7168 && atomic.CompareAndSwapUint32(&mockNotOwnerErrOnce, 0, 1) {
					failpoint.Return(nil, errors.Trace(dbterror.ErrNotOwner))
				}
			case "addIdxNotOwnerErr":
				// For the case of the old TiDB version(do not exist the element information) is upgraded to the new TiDB version.
				// First step, we need to exit "addPhysicalTableIndex".
				if idxInfo.Name.O == "idx2" && handle.IntValue() == 6144 && atomic.CompareAndSwapUint32(&mockNotOwnerErrOnce, 1, 2) {
					failpoint.Return(nil, errors.Trace(dbterror.ErrNotOwner))
				}
			}
		}
	})
	idxVal := make([]types.Datum, len(idxInfo.Columns))
	var err error
	for j, v := range idxInfo.Columns {
		col := cols[v.Offset]
		idxColumnVal, ok := w.rowMap[col.ID]
		if ok {
			idxVal[j] = idxColumnVal
			continue
		}
		idxColumnVal, err = tables.GetColDefaultValue(w.exprCtx, col, w.defaultVals)
		if err != nil {
			return nil, errors.Trace(err)
		}

		idxVal[j] = idxColumnVal
	}

	rsData := tables.TryGetHandleRestoredDataWrapper(w.table.Meta(), nil, w.rowMap, idxInfo)
	idxRecord := &indexRecord{handle: handle, key: recordKey, vals: idxVal, rsData: rsData}
	return idxRecord, nil
}

func (w *baseIndexWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
}

// getNextKey gets next key of entry that we are going to process.
func (w *baseIndexWorker) getNextKey(taskRange reorgBackfillTask, taskDone bool) (nextKey kv.Key) {
	if !taskDone {
		// The task is not done. So we need to pick the last processed entry's handle and add one.
		lastHandle := w.idxRecords[len(w.idxRecords)-1].handle
		recordKey := tablecodec.EncodeRecordKey(taskRange.physicalTable.RecordPrefix(), lastHandle)
		return recordKey.Next()
	}
	return taskRange.endKey
}

func (w *baseIndexWorker) updateRowDecoder(handle kv.Handle, rawRecord []byte) error {
	sysZone := w.loc
	_, err := w.rowDecoder.DecodeAndEvalRowWithMap(w.exprCtx, handle, rawRecord, sysZone, w.rowMap)
	return errors.Trace(err)
}

// fetchRowColVals fetch w.batchCnt count records that need to reorganize indices, and build the corresponding indexRecord slice.
// fetchRowColVals returns:
// 1. The corresponding indexRecord slice.
// 2. Next handle of entry that we need to process.
// 3. Boolean indicates whether the task is done.
// 4. error occurs in fetchRowColVals. nil if no error occurs.
func (w *baseIndexWorker) fetchRowColVals(txn kv.Transaction, taskRange reorgBackfillTask) ([]*indexRecord, kv.Key, bool, error) {
	// TODO: use tableScan to prune columns.
	w.idxRecords = w.idxRecords[:0]
	startTime := time.Now()

	// taskDone means that the reorged handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	err := iterateSnapshotKeys(w.jobContext, w.ddlCtx.store, taskRange.priority, taskRange.physicalTable.RecordPrefix(), txn.StartTS(),
		taskRange.startKey, taskRange.endKey, func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotKeys in baseIndexWorker fetchRowColVals", 0)
			oprStartTime = oprEndTime

			taskDone = recordKey.Cmp(taskRange.endKey) >= 0

			if taskDone || len(w.idxRecords) >= w.batchCnt {
				return false, nil
			}

			// Decode one row, generate records of this row.
			err := w.updateRowDecoder(handle, rawRow)
			if err != nil {
				return false, err
			}

			for _, index := range w.indexes {
				if index.Meta().HasCondition() {
					return false, dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs("add partial index without fast reorg")
				}
				actualHandle := handle
				// For global indexes V1+ on partitioned tables, we need to wrap the handle
				// with the partition ID to create a PartitionHandle.
				// This is critical for non-clustered tables after EXCHANGE PARTITION,
				// where duplicate _tidb_rowid values exist across partitions.
				// Legacy indexes (version 0) don't use PartitionHandle in the key.
				if index.Meta().Global && index.Meta().GlobalIndexVersion >= model.GlobalIndexVersionV1 {
					actualHandle = kv.NewPartitionHandle(taskRange.physicalTable.GetPhysicalID(), handle)
				}
				idxRecord, err1 := w.getIndexRecord(index.Meta(), actualHandle, recordKey)
				if err1 != nil {
					return false, errors.Trace(err1)
				}
				w.idxRecords = append(w.idxRecords, idxRecord)
			}
			// If there are generated column, rowDecoder will use column value that not in idxInfo.Columns to calculate
			// the generated value, so we need to clear up the reusing map.
			w.cleanRowMap()

			if recordKey.Cmp(taskRange.endKey) == 0 {
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if len(w.idxRecords) == 0 {
		taskDone = true
	}

	logutil.DDLLogger().Debug("txn fetches handle info", zap.Stringer("worker", w), zap.Uint64("txnStartTS", txn.StartTS()),
		zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.idxRecords, w.getNextKey(taskRange, taskDone), taskDone, errors.Trace(err)
}

func (w *addIndexTxnWorker) initBatchCheckBufs(batchCount int) {
	if len(w.idxKeyBufs) < batchCount {
		w.idxKeyBufs = make([][]byte, batchCount)
	}

	w.batchCheckKeys = w.batchCheckKeys[:0]
	w.batchCheckValues = w.batchCheckValues[:0]
	w.distinctCheckFlags = w.distinctCheckFlags[:0]
	w.recordIdx = w.recordIdx[:0]
}

func (w *addIndexTxnWorker) checkHandleExists(idxInfo *model.IndexInfo, key kv.Key, value []byte, handle kv.Handle) error {
	tblInfo := w.table.Meta()
	idxColLen := len(idxInfo.Columns)
	h, err := tablecodec.DecodeIndexHandle(key, value, idxColLen)
	if err != nil {
		return errors.Trace(err)
	}
	hasBeenBackFilled := h.Equal(handle)
	if hasBeenBackFilled {
		return nil
	}
	return ddlutil.GenKeyExistsErr(key, value, idxInfo, tblInfo)
}

// batchCheckUniqueKey checks the unique keys in the batch.
// Note that `idxRecords` may belong to multiple indexes.
func (w *addIndexTxnWorker) batchCheckUniqueKey(txn kv.Transaction, idxRecords []*indexRecord) error {
	w.initBatchCheckBufs(len(idxRecords))
	evalCtx := w.exprCtx.GetEvalCtx()
	ec := evalCtx.ErrCtx()
	uniqueBatchKeys := make([]kv.Key, 0, len(idxRecords))
	cnt := 0
	for i, record := range idxRecords {
		idx := w.indexes[i%len(w.indexes)]
		if !idx.Meta().Unique {
			// non-unique key need not to check, use `nil` as a placeholder to keep
			// `idxRecords[i]` belonging to `indexes[i%len(indexes)]`.
			w.batchCheckKeys = append(w.batchCheckKeys, nil)
			w.batchCheckValues = append(w.batchCheckValues, nil)
			w.distinctCheckFlags = append(w.distinctCheckFlags, false)
			w.recordIdx = append(w.recordIdx, 0)
			continue
		}
		// skip by default.
		idxRecords[i].skip = true
		iter := idx.GenIndexKVIter(ec, w.loc, record.vals, record.handle, idxRecords[i].rsData)
		for iter.Valid() {
			var buf []byte
			if cnt < len(w.idxKeyBufs) {
				buf = w.idxKeyBufs[cnt]
			}
			key, val, distinct, err := iter.Next(buf, nil)
			if err != nil {
				return errors.Trace(err)
			}
			if cnt < len(w.idxKeyBufs) {
				w.idxKeyBufs[cnt] = key
			} else {
				w.idxKeyBufs = append(w.idxKeyBufs, key)
			}
			cnt++
			w.batchCheckKeys = append(w.batchCheckKeys, key)
			w.batchCheckValues = append(w.batchCheckValues, val)
			w.distinctCheckFlags = append(w.distinctCheckFlags, distinct)
			w.recordIdx = append(w.recordIdx, i)
			uniqueBatchKeys = append(uniqueBatchKeys, key)
		}
	}

	if len(uniqueBatchKeys) == 0 {
		return nil
	}

	batchVals, err := kv.BatchGetValue(context.Background(), txn, uniqueBatchKeys)
	if err != nil {
		return errors.Trace(err)
	}

	// 1. unique-key/primary-key is duplicate and the handle is equal, skip it.
	// 2. unique-key/primary-key is duplicate and the handle is not equal, return duplicate error.
	// 3. non-unique-key is duplicate, skip it.
	for i, key := range w.batchCheckKeys {
		if len(key) == 0 {
			continue
		}
		idx := w.indexes[i%len(w.indexes)]
		val, found := batchVals[string(key)]
		if found {
			if w.distinctCheckFlags[i] {
				if err := w.checkHandleExists(idx.Meta(), key, val, idxRecords[w.recordIdx[i]].handle); err != nil {
					return errors.Trace(err)
				}
			}
		} else if w.distinctCheckFlags[i] {
			// The keys in w.batchCheckKeys also maybe duplicate,
			// so we need to backfill the not found key into `batchVals` map.
			batchVals[string(key)] = w.batchCheckValues[i]
		}
		idxRecords[w.recordIdx[i]].skip = found && idxRecords[w.recordIdx[i]].skip
	}
	return nil
}

func getLocalWriterConfig(indexCnt, writerCnt int) *backend.LocalWriterConfig {
	writerCfg := &backend.LocalWriterConfig{}
	// avoid unit test panic
	memRoot := ingest.LitMemRoot
	if memRoot == nil {
		return writerCfg
	}

	// leave some room for objects overhead
	availMem := memRoot.MaxMemoryQuota() - memRoot.CurrentUsage() - int64(10*size.MB)
	memLimitPerWriter := availMem / int64(indexCnt) / int64(writerCnt)
	memLimitPerWriter = min(memLimitPerWriter, litconfig.DefaultLocalWriterMemCacheSize)
	writerCfg.Local.MemCacheSize = memLimitPerWriter
	return writerCfg
}

func writeChunk(
	ctx context.Context,
	writers []ingest.Writer,
	indexes []table.Index,
	indexConditionCheckers []func(row chunk.Row) (bool, error),
	copCtx copr.CopContext,
	loc *time.Location,
	errCtx errctx.Context,
	writeStmtBufs *variable.WriteStmtBufs,
	copChunk *chunk.Chunk,
	tblInfo *model.TableInfo,
) (rowCnt int, bytes int, err error) {
	iter := chunk.NewIterator4Chunk(copChunk)
	c := copCtx.GetBase()
	ectx := c.ExprCtx.GetEvalCtx()

	maxIdxColCnt := maxIndexColumnCount(indexes)
	idxDataBuf := make([]types.Datum, maxIdxColCnt)
	handleDataBuf := make([]types.Datum, len(c.HandleOutputOffsets))
	var restoreDataBuf []types.Datum
	count := 0
	totalBytes := 0

	unlockFns := make([]func(), 0, len(writers))
	for _, w := range writers {
		unlock := w.LockForWrite()
		unlockFns = append(unlockFns, unlock)
	}
	defer func() {
		for _, unlock := range unlockFns {
			unlock()
		}
	}()
	needRestoreForIndexes := make([]bool, len(indexes))
	restore, pkNeedRestore := false, false
	if c.PrimaryKeyInfo != nil && c.TableInfo.IsCommonHandle && c.TableInfo.CommonHandleVersion != 0 {
		pkNeedRestore = tables.NeedRestoredData(c.PrimaryKeyInfo.Columns, c.TableInfo.Columns)
	}
	for i, index := range indexes {
		needRestore := pkNeedRestore || tables.NeedRestoredData(index.Meta().Columns, c.TableInfo.Columns)
		needRestoreForIndexes[i] = needRestore
		restore = restore || needRestore
	}
	if restore {
		restoreDataBuf = make([]types.Datum, len(c.HandleOutputOffsets))
	}

	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		handleDataBuf := ExtractDatumByOffsets(ectx, row, c.HandleOutputOffsets, c.ExprColumnInfos, handleDataBuf)
		if restore {
			// restoreDataBuf should not truncate index values.
			for i, datum := range handleDataBuf {
				restoreDataBuf[i] = *datum.Clone()
			}
		}
		h, err := BuildHandle(handleDataBuf, c.TableInfo, c.PrimaryKeyInfo, loc, errCtx)
		if err != nil {
			return 0, totalBytes, errors.Trace(err)
		}
		for i, index := range indexes {
			// If the `IndexRecordChunk.conditionPushed` is true and we have only 1 index, the `indexConditionCheckers`
			// will not be initialized.
			if index.Meta().HasCondition() && indexConditionCheckers != nil {
				ok, err := indexConditionCheckers[i](row)
				if err != nil {
					return 0, 0, errors.Trace(err)
				}
				if !ok {
					continue
				}
			}

			idxID := index.Meta().ID
			idxDataBuf = ExtractDatumByOffsets(ectx,
				row, copCtx.IndexColumnOutputOffsets(idxID), c.ExprColumnInfos, idxDataBuf)
			idxData := idxDataBuf[:len(index.Meta().Columns)]
			var rsData []types.Datum
			if needRestoreForIndexes[i] {
				rsData = getRestoreData(c.TableInfo, copCtx.IndexInfo(idxID), c.PrimaryKeyInfo, restoreDataBuf)
			}
			kvBytes, err := writeOneKV(ctx, writers[i], index, loc, errCtx, writeStmtBufs, idxData, rsData, h)
			if err != nil {
				err = ingest.TryConvertToKeyExistsErr(err, index.Meta(), tblInfo)
				return 0, totalBytes, errors.Trace(err)
			}
			totalBytes += int(kvBytes)
		}
		count++
	}
	return count, totalBytes, nil
}

func maxIndexColumnCount(indexes []table.Index) int {
	maxCnt := 0
	for _, idx := range indexes {
		colCnt := len(idx.Meta().Columns)
		if colCnt > maxCnt {
			maxCnt = colCnt
		}
	}
	return maxCnt
}

func writeOneKV(
	ctx context.Context,
	writer ingest.Writer,
	index table.Index,
	loc *time.Location,
	errCtx errctx.Context,
	writeBufs *variable.WriteStmtBufs,
	idxDt, rsData []types.Datum,
	handle kv.Handle,
) (int64, error) {
	var kvBytes int64
	iter := index.GenIndexKVIter(errCtx, loc, idxDt, handle, rsData)
	for iter.Valid() {
		key, idxVal, _, err := iter.Next(writeBufs.IndexKeyBuf, writeBufs.RowValBuf)
		if err != nil {
			return kvBytes, errors.Trace(err)
		}
		failpoint.Inject("mockLocalWriterPanic", func() {
			panic("mock panic")
		})
		err = writer.WriteRow(ctx, key, idxVal, handle)
		if err != nil {
			return kvBytes, errors.Trace(err)
		}
		// TODO this size doesn't consider keyspace prefix.
		kvBytes += int64(len(key) + len(idxVal))
		failpoint.Inject("mockLocalWriterError", func() {
			failpoint.Return(0, errors.New("mock engine error"))
		})
		writeBufs.IndexKeyBuf = key
		writeBufs.RowValBuf = idxVal
	}
	return kvBytes, nil
}

// BackfillData will backfill table index in a transaction. A lock corresponds to a rowKey if the value of rowKey is changed,
// Note that index columns values may change, and an index is not allowed to be added, so the txn will rollback and retry.
// BackfillData will add w.batchCnt indices once, default value of w.batchCnt is 128.
func (w *addIndexTxnWorker) BackfillData(_ context.Context, handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			panic("panic test")
		}
	})

	oprStartTime := time.Now()
	jobID := handleRange.getJobID()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) (err error) {
		taskCtx.finishTS = txn.StartTS()
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		updateTxnEntrySizeLimitIfNeeded(txn)
		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(jobID); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}
		txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

		idxRecords, nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		err = w.batchCheckUniqueKey(txn, idxRecords)
		if err != nil {
			return errors.Trace(err)
		}
		failpoint.InjectCall("addIndexTxnWorkerBackfillData", len(idxRecords))

		for i, idxRecord := range idxRecords {
			taskCtx.scanCount++
			// The index is already exists, we skip it, no needs to backfill it.
			// The following update, delete, insert on these rows, TiDB can handle it correctly.
			if idxRecord.skip {
				continue
			}

			// We need to add this lock to make sure pessimistic transaction can realize this operation.
			// For the normal pessimistic transaction, it's ok. But if async commit is used, it may lead to inconsistent data and index.
			// TODO: For global index, lock the correct key?! Currently it locks the partition (phyTblID) and the handle or actual key?
			// but should really lock the table's ID + key col(s)
			err := txn.LockKeys(context.Background(), new(kv.LockCtx), idxRecord.key)
			if err != nil {
				return errors.Trace(err)
			}

			handle, err := w.indexes[i%len(w.indexes)].Create(
				w.tblCtx, txn, idxRecord.vals, idxRecord.handle, idxRecord.rsData,
				table.WithIgnoreAssertion,
				table.FromBackfill,
				// Constrains is already checked in batchCheckUniqueKey
				table.DupKeyCheckSkip,
			)
			if err != nil {
				if kv.ErrKeyExists.Equal(err) && idxRecord.handle.Equal(handle) {
					// Index already exists, skip it.
					continue
				}
				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}

		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "AddIndexBackfillData", 3000)
	failpoint.Inject("mockDMLExecution", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && MockDMLExecution != nil {
			MockDMLExecution()
		}
	})
	failpoint.InjectCall("mockAddIndexTxnWorkerStuck")
	return
}

// MockDMLExecution is only used for test.
var MockDMLExecution func()

// MockDMLExecutionMerging is only used for test.
var MockDMLExecutionMerging func()

func (w *worker) addPhysicalTableIndex(
	ctx context.Context,
	t table.PhysicalTable,
	reorgInfo *reorgInfo,
) error {
	if reorgInfo.mergingTmpIdx {
		logutil.DDLLogger().Info("start to merge temp index", zap.Stringer("job", reorgInfo.Job), zap.Stringer("reorgInfo", reorgInfo))
		return w.writePhysicalTableRecord(ctx, w.sessPool, t, typeAddIndexMergeTmpWorker, reorgInfo)
	}
	logutil.DDLLogger().Info("start to add table index", zap.Stringer("job", reorgInfo.Job), zap.Stringer("reorgInfo", reorgInfo))
	m := metrics.RegisterLightningCommonMetricsForDDL(reorgInfo.ID)
	ctx = lightningmetric.WithCommonMetric(ctx, m)
	defer func() {
		metrics.UnregisterLightningCommonMetricsForDDL(reorgInfo.ID, m)
	}()
	return w.writePhysicalTableRecord(ctx, w.sessPool, t, typeAddIndexWorker, reorgInfo)
}

// addTableIndex handles the add index reorganization state for a table.
func (w *worker) addTableIndex(
	jobCtx *jobContext,
	t table.Table,
	reorgInfo *reorgInfo,
) error {
	ctx := jobCtx.stepCtx
	if reorgInfo.ReorgMeta.IsDistReorg && reorgInfo.ReorgMeta.ReorgTp == model.ReorgTypeIngest {
		err := w.executeDistTask(jobCtx, t, reorgInfo)
		if err != nil {
			return err
		}
		if reorgInfo.ReorgMeta.UseCloudStorage {
			// When adding unique index by global sort, it detects duplicate keys in each step.
			// A duplicate key must be detected before, so we can skip the check bellow.
			return nil
		}
		if reorgInfo.mergingTmpIdx {
			// Merging temp index checks the duplicate keys in subtask executors.
			return nil
		}
		return checkDuplicateForUniqueIndex(ctx, t, reorgInfo, w.store)
	}

	var err error
	if tbl, ok := t.(table.PartitionedTable); ok {
		var finish, ok bool
		for !finish {
			var p table.PhysicalTable
			if tbl.Meta().ID == reorgInfo.PhysicalTableID {
				p, ok = t.(table.PhysicalTable) // global index
				if !ok {
					return fmt.Errorf("unexpected error, can't cast %T to table.PhysicalTable", t)
				}
			} else {
				p = tbl.GetPartition(reorgInfo.PhysicalTableID)
				if p == nil {
					return dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
				}
			}
			err = w.addPhysicalTableIndex(ctx, p, reorgInfo)
			if err != nil {
				break
			}

			finish, err = updateReorgInfo(w.sessPool, tbl, reorgInfo)
			if err != nil {
				return errors.Trace(err)
			}
			failpoint.InjectCall("afterUpdatePartitionReorgInfo", reorgInfo.Job)
			// Every time we finish a partition, we update the progress of the job.
			if rc := w.getReorgCtx(reorgInfo.Job.ID); rc != nil {
				reorgInfo.Job.SetRowCount(rc.getRowCount())
			}
		}
	} else {
		//nolint:forcetypeassert
		phyTbl := t.(table.PhysicalTable)
		err = w.addPhysicalTableIndex(ctx, phyTbl, reorgInfo)
	}
	return errors.Trace(err)
}

func checkDuplicateForUniqueIndex(ctx context.Context, t table.Table, reorgInfo *reorgInfo, store kv.Storage) (err error) {
	var (
		backendCtx ingest.BackendCtx
		cfg        *local.BackendConfig
		backend    *local.Backend
	)
	defer func() {
		if backendCtx != nil {
			backendCtx.Close()
		}
		if backend != nil {
			backend.Close()
		}
	}()
	for _, elem := range reorgInfo.elements {
		indexInfo := model.FindIndexInfoByID(t.Meta().Indices, elem.ID)
		if indexInfo == nil {
			return errors.New("unexpected error, can't find index info")
		}
		if indexInfo.Unique {
			ctx := tidblogutil.WithCategory(ctx, "ddl-ingest")
			if backendCtx == nil {
				if config.GetGlobalConfig().Store == config.StoreTypeTiKV {
					cfg, backend, err = ingest.CreateLocalBackend(ctx, store, reorgInfo.Job, true, true, 0)
					if err != nil {
						return errors.Trace(err)
					}
				}
				backendCtx, err = ingest.NewBackendCtxBuilder(ctx, store, reorgInfo.Job).
					ForDuplicateCheck().
					Build(cfg, backend)
				if err != nil {
					return err
				}
			}
			err = backendCtx.CollectRemoteDuplicateRows(indexInfo.ID, t)
			failpoint.Inject("mockCheckDuplicateForUniqueIndexError", func(_ failpoint.Value) {
				err = context.DeadlineExceeded
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// TaskKeyBuilder is used to build task key for the backfill job.
type TaskKeyBuilder struct {
	multiSchemaSeq int32
	mergeTempIdx   bool
}

// NewTaskKeyBuilder creates a new TaskKeyBuilder.
func NewTaskKeyBuilder() *TaskKeyBuilder {
	return &TaskKeyBuilder{multiSchemaSeq: -1}
}

// SetMergeTempIndex sets whether to merge the temporary index.
func (b *TaskKeyBuilder) SetMergeTempIndex(flag bool) *TaskKeyBuilder {
	b.mergeTempIdx = flag
	return b
}

// SetMultiSchema sets the multi-schema change information.
func (b *TaskKeyBuilder) SetMultiSchema(info *model.MultiSchemaInfo) *TaskKeyBuilder {
	if info != nil {
		b.multiSchemaSeq = info.Seq
	}
	return b
}

// Build builds the task key for the backfill job.
func (b *TaskKeyBuilder) Build(jobID int64) string {
	labels := make([]string, 0, 8)
	if kerneltype.IsNextGen() {
		labels = append(labels, keyspace.GetKeyspaceNameBySettings())
	}
	labels = append(labels, "ddl", proto.Backfill.String(), strconv.FormatInt(jobID, 10))
	if b.multiSchemaSeq >= 0 {
		labels = append(labels, strconv.Itoa(int(b.multiSchemaSeq)))
	}
	if b.mergeTempIdx {
		labels = append(labels, "merge")
	}
	return strings.Join(labels, "/")
}

// TaskKey generates a task key for the backfill job.
func TaskKey(jobID int64, mergeTempIdx bool) string {
	labels := make([]string, 0, 8)
	if kerneltype.IsNextGen() {
		ks := keyspace.GetKeyspaceNameBySettings()
		labels = append(labels, ks)
	}
	labels = append(labels, "ddl", proto.Backfill.String(), strconv.FormatInt(jobID, 10))
	if mergeTempIdx {
		labels = append(labels, "merge")
	}
	return strings.Join(labels, "/")
}

func (w *worker) executeDistTask(jobCtx *jobContext, t table.Table, reorgInfo *reorgInfo) error {
	stepCtx := jobCtx.stepCtx
	taskType := proto.Backfill
	tkBuilder := NewTaskKeyBuilder().
		SetMultiSchema(reorgInfo.Job.MultiSchemaInfo).
		SetMergeTempIndex(reorgInfo.mergingTmpIdx)
	taskKey := tkBuilder.Build(reorgInfo.Job.ID)
	g, ctx := errgroup.WithContext(w.workCtx)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalDistTask)

	done := make(chan struct{})
	// For resuming add index task.
	// Need to fetch task by taskKey in tidb_global_task and tidb_global_task_history tables.
	// When pausing the related ddl job, it is possible that the task with taskKey is succeed and in tidb_global_task_history.
	// As a result, when resuming the related ddl job,
	// it is necessary to check task exits in tidb_global_task and tidb_global_task_history tables.
	taskManager, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		return err
	}
	task, err := taskManager.GetTaskByKeyWithHistory(w.workCtx, taskKey)
	if err != nil && !goerrors.Is(err, storage.ErrTaskNotFound) {
		return err
	}

	var (
		taskID                                              int64
		lastRequiredSlots, lastBatchSize, lastMaxWriteSpeed int
	)
	if task != nil {
		// It's possible that the task state is succeed but the ddl job is paused.
		// When task in succeed state, we can skip the dist task execution/scheduling process.
		if task.State == proto.TaskStateSucceed {
			w.updateDistTaskRowCount(taskKey, reorgInfo.Job.ID)
			w.logDistTaskObservedTiKVUsage(taskManager, taskKey, reorgInfo.Job.ID)
			logutil.DDLLogger().Info(
				"task succeed, start to resume the ddl job",
				zap.String("task-key", taskKey))
			return nil
		}
		taskMeta := &BackfillTaskMeta{}
		if err := json.Unmarshal(task.Meta, taskMeta); err != nil {
			return errors.Trace(err)
		}
		taskID = task.ID
		lastRequiredSlots = task.RequiredSlots
		lastBatchSize = taskMeta.Job.ReorgMeta.GetBatchSize()
		lastMaxWriteSpeed = taskMeta.Job.ReorgMeta.GetMaxWriteSpeed()
		g.Go(func() error {
			defer close(done)
			backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
			err := handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logutil.DDLLogger(),
				func(context.Context) (bool, error) {
					return true, handle.ResumeTask(w.workCtx, taskKey)
				},
			)
			if err != nil {
				return err
			}
			err = handle.WaitTaskDoneOrPaused(ctx, task.ID)
			if err := w.isReorgRunnable(stepCtx, true); err != nil {
				if dbterror.ErrPausedDDLJob.Equal(err) {
					logutil.DDLLogger().Warn("job paused by user", zap.Error(err))
					return dbterror.ErrPausedDDLJob.GenWithStackByArgs(reorgInfo.Job.ID)
				}
			}
			return err
		})
	} else {
		job := reorgInfo.Job
		workerCntLimit := job.ReorgMeta.GetConcurrency()
		requiredSlots, err := adjustConcurrency(ctx, taskManager, workerCntLimit)
		if err != nil {
			return err
		}
		logutil.DDLLogger().Info("adjusted add-index task required slots",
			zap.Int("worker-cnt", workerCntLimit), zap.Int("required-slots", requiredSlots),
			zap.String("task-key", taskKey))
		rowSize := estimateTableRowSize(w.workCtx, w.store, w.sess.GetRestrictedSQLExecutor(), t)
		var (
			initialCapacity                   *TiKVClusterCapacity
			blockSamplePrediction             sampleTiKVIndexPredictionResult
			blockSamplePeakPredictedTiKVBytes uint64
			tikvReplicaCount                  uint64
			tikvReplicaCountSource            string
			tikvReplicaCountPhysicalID        int64
		)
		if !reorgInfo.mergingTmpIdx {
			runTiKVSpacePrecheck := false
			predictionOK := true
			clearPrediction := func() {
				blockSamplePrediction = sampleTiKVIndexPredictionResult{}
				blockSamplePeakPredictedTiKVBytes = 0
				tikvReplicaCount = 0
				tikvReplicaCountSource = ""
				tikvReplicaCountPhysicalID = 0
			}
			skipPrediction := func(phase string, err error) {
				logutil.DDLLogger().Warn("skip TiKV index size prediction and space precheck for add-index task because prediction failed",
					zap.Int64("jobID", job.ID),
					zap.String("task-key", taskKey),
					zap.String("prediction-phase", phase),
					zap.Error(err))
				clearPrediction()
				predictionOK = false
				runTiKVSpacePrecheck = false
			}
			blockSamplePrediction, err = w.predictTiKVIndexBytesBlockSample(ctx, w.sess.Session(), t, reorgInfo)
			if err != nil {
				skipPrediction("block-sample", err)
			}
			if predictionOK {
				tikvReplicaCount, tikvReplicaCountSource, tikvReplicaCountPhysicalID, err = w.resolveAddIndexTiKVReplicaCount(ctx, w.sess.Session(), t, reorgInfo)
				if err != nil {
					skipPrediction("tikv-replica-count", err)
				} else {
					blockSamplePrediction.PredictedBytes = scalePredictedBytesByReplicaCount(blockSamplePrediction.PredictedBytes, tikvReplicaCount)
					blockSamplePrediction.MVCCOverheadBytes = scalePredictedBytesByReplicaCount(blockSamplePrediction.MVCCOverheadBytes, tikvReplicaCount)
					blockSamplePeakPredictedTiKVBytes = estimateBlockSamplePeakPredictedTiKVIndexBytes(blockSamplePrediction.PredictedBytes)
				}
			}
			if predictionOK {
				runTiKVSpacePrecheck, err = canRunTiKVSpacePrecheck(w.store)
				if err != nil {
					logutil.DDLLogger().Warn("skip TiKV space precheck for add-index task because PD HTTP client capability check failed",
						zap.Int64("jobID", job.ID),
						zap.String("task-key", taskKey),
						zap.Error(err))
					runTiKVSpacePrecheck = false
				}
				if !runTiKVSpacePrecheck {
					logutil.DDLLogger().Info("skip TiKV space precheck for add-index task because PD HTTP client is unavailable",
						zap.Int64("jobID", job.ID),
						zap.String("task-key", taskKey))
				}
			}
			if predictionOK && runTiKVSpacePrecheck {
				initialCapacity, err = collectTiKVStoreCapacity(ctx, w.store)
				if err != nil {
					logutil.DDLLogger().Warn("skip TiKV space precheck for add-index task because TiKV capacity snapshot failed",
						zap.Int64("jobID", job.ID),
						zap.String("task-key", taskKey),
						zap.Error(err))
					initialCapacity = nil
					runTiKVSpacePrecheck = false
				} else if initialCapacity == nil || initialCapacity.StoreCount == 0 {
					logutil.DDLLogger().Warn("skip TiKV space precheck for add-index task because TiKV capacity snapshot is empty",
						zap.Int64("jobID", job.ID),
						zap.String("task-key", taskKey))
					initialCapacity = nil
					runTiKVSpacePrecheck = false
				}
			}
			if predictionOK && runTiKVSpacePrecheck {
				enforceTiKVSpacePrecheck := vardef.EnforceDiskSpacePrecheckBeforeAddIndex.Load()
				precheckLogFields := addIndexTiKVSpacePrecheckLogFields(
					job.ID, taskKey, blockSamplePrediction, blockSamplePeakPredictedTiKVBytes,
					tikvReplicaCount, tikvReplicaCountSource, tikvReplicaCountPhysicalID,
					initialCapacity, enforceTiKVSpacePrecheck)
				precheckErr, rejectErr := evaluateAddIndexTiKVSpacePrecheck(initialCapacity, blockSamplePeakPredictedTiKVBytes, enforceTiKVSpacePrecheck)
				if precheckErr != nil {
					logutil.DDLLogger().Warn("insufficient TiKV space predicted for add-index task",
						append(precheckLogFields, zap.Error(precheckErr))...)
					if rejectErr != nil {
						return rejectErr
					}
				} else {
					logutil.DDLLogger().Info("passed TiKV space precheck for add-index task",
						precheckLogFields...)
				}
			}
		} else {
			initialCapacity, err = collectTiKVStoreCapacity(ctx, w.store)
			if err != nil {
				logutil.DDLLogger().Warn("failed to collect initial TiKV capacity for merge-temp-index task",
					zap.Int64("jobID", job.ID), zap.String("task-key", taskKey), zap.Error(err))
			}
		}
		taskMeta := &BackfillTaskMeta{
			Job:                                      *job.Clone(),
			EleIDs:                                   extractElemIDs(reorgInfo),
			EleTypeKey:                               reorgInfo.currElement.TypeKey,
			CloudStorageURI:                          w.jobContext(job.ID, job.ReorgMeta).cloudStorageURI,
			MergeTempIndex:                           reorgInfo.mergingTmpIdx,
			EstimateRowSize:                          rowSize,
			InitialTiKVCapacity:                      initialCapacity,
			BlockSampleSteadyPredictedTiKVIndexBytes: blockSamplePrediction.PredictedBytes,
			BlockSamplePeakPredictedTiKVIndexBytes:   blockSamplePeakPredictedTiKVBytes,
			BlockSampleMVCCOverheadTotalBytes:        blockSamplePrediction.MVCCOverheadBytes,
			BlockSampleUseStats:                      blockSamplePrediction.UseStats,
			TiKVReplicaCount:                         tikvReplicaCount,
			TiKVReplicaCountSource:                   tikvReplicaCountSource,
			TiKVReplicaCountPhysicalID:               tikvReplicaCountPhysicalID,
			BlockSamplePredictionRegionCount:         blockSamplePrediction.SampledRegionCount,
			BlockSamplePredictionRowCount:            blockSamplePrediction.SampledRowCount,
			BlockSamplePredictionReadErrorCount:      blockSamplePrediction.ReadErrorCount,
			BlockSampleEncodedKeySharedPrefixAvg:     blockSamplePrediction.EncodedKeySharedPrefixAvgBytes,
			BlockSampleRawKeySharedPrefixAvg:         blockSamplePrediction.RawKeySharedPrefixAvgBytes,
			BlockSampleRawKeyLengthAvg:               blockSamplePrediction.RawKeyLengthAvgBytes,
			Version:                                  BackfillTaskMetaVersion1,
		}
		if initialCapacity != nil {
			taskMeta.InitialTiKVStoreUsage = &TiKVStoreUsageSnapshot{
				UsedBytes:  initialCapacity.UsedBytes,
				StoreCount: initialCapacity.StoreCount,
			}
		}

		metaData, err := json.Marshal(taskMeta)
		if err != nil {
			return err
		}

		targetScope := reorgInfo.ReorgMeta.TargetScope
		maxNodeCnt := reorgInfo.ReorgMeta.MaxNodeCount
		task, err := handle.SubmitTask(ctx, taskKey, taskType, w.store.GetKeyspace(), requiredSlots, targetScope, maxNodeCnt, metaData)
		if err != nil {
			return err
		}

		taskID = task.ID
		lastRequiredSlots = requiredSlots
		lastBatchSize = taskMeta.Job.ReorgMeta.GetBatchSize()
		lastMaxWriteSpeed = taskMeta.Job.ReorgMeta.GetMaxWriteSpeed()

		g.Go(func() error {
			defer close(done)
			err := handle.WaitTaskDoneOrPaused(ctx, task.ID)
			failpoint.InjectCall("pauseAfterDistTaskFinished")
			if err := w.isReorgRunnable(stepCtx, true); err != nil {
				if dbterror.ErrPausedDDLJob.Equal(err) {
					logutil.DDLLogger().Warn("job paused by user", zap.Error(err))
					return dbterror.ErrPausedDDLJob.GenWithStackByArgs(reorgInfo.Job.ID)
				}
			}
			return err
		})
	}

	g.Go(func() error {
		checkFinishTk := time.NewTicker(CheckBackfillJobFinishInterval)
		defer checkFinishTk.Stop()
		updateRowCntTk := time.NewTicker(UpdateBackfillJobRowCountInterval)
		defer updateRowCntTk.Stop()
		for {
			select {
			case <-done:
				w.updateDistTaskRowCount(taskKey, reorgInfo.Job.ID)
				err := w.checkRunnableOrHandlePauseOrCanceled(stepCtx, taskKey)
				return errors.Trace(err)
			case <-checkFinishTk.C:
				err := w.checkRunnableOrHandlePauseOrCanceled(stepCtx, taskKey)
				if err != nil {
					return errors.Trace(err)
				}
			case <-updateRowCntTk.C:
				w.updateDistTaskRowCount(taskKey, reorgInfo.Job.ID)
			}
		}
	})

	g.Go(func() error {
		modifyTaskParamLoop(ctx, jobCtx.sysTblMgr, taskManager, done,
			reorgInfo.Job.ID, taskID, lastRequiredSlots, lastBatchSize, lastMaxWriteSpeed)
		return nil
	})

	err = g.Wait()
	if err == nil {
		w.logDistTaskObservedTiKVUsage(taskManager, taskKey, reorgInfo.Job.ID)
		w.scheduleDelayedDistTaskObservedTiKVUsage(taskManager, taskKey, reorgInfo.Job.ID)
	}
	return err
}

func (w *worker) checkRunnableOrHandlePauseOrCanceled(stepCtx context.Context, taskKey string) (err error) {
	if err = w.isReorgRunnable(stepCtx, true); err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			if err = handle.PauseTask(w.workCtx, taskKey); err != nil {
				logutil.DDLLogger().Warn("pause task error", zap.String("task_key", taskKey), zap.Error(err))
				return nil
			}
			failpoint.InjectCall("syncDDLTaskPause")
		}
		if !dbterror.ErrCancelledDDLJob.Equal(err) {
			return errors.Trace(err)
		}
		if err = handle.CancelTask(w.workCtx, taskKey); err != nil {
			logutil.DDLLogger().Warn("cancel task error", zap.String("task_key", taskKey), zap.Error(err))
			return nil
		}
	}
	return nil
}

// Note: we can achieve the same effect by calling ModifyTaskByID directly inside
// the process of 'ADMIN ALTER DDL JOB xxx', so we can eliminate the goroutine,
// but if the task hasn't been created we need to make sure the task is created
// with config after ALTER DDL JOB is executed. A possible solution is to make
// the DXF task submission and 'ADMIN ALTER DDL JOB xxx' txn conflict with each
// other when they overlap in time, by modify the job at the same time when submit
// task, as we are using optimistic txn. But this will cause WRITE CONFLICT with
// outer txn in transitOneJobStep.
func modifyTaskParamLoop(
	ctx context.Context,
	sysTblMgr systable.Manager,
	taskManager storage.Manager,
	done chan struct{},
	jobID, taskID int64,
	lastRequiredSlots, lastBatchSize, lastMaxWriteSpeed int,
) {
	logger := logutil.DDLLogger().With(zap.Int64("jobID", jobID), zap.Int64("taskID", taskID))
	ticker := time.NewTicker(UpdateDDLJobReorgCfgInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
		}

		latestJob, err := sysTblMgr.GetJobByID(ctx, jobID)
		if err != nil {
			if goerrors.Is(err, systable.ErrNotFound) {
				logger.Info("job not found, might already finished")
				return
			}
			logger.Error("get job failed, will retry later", zap.Error(err))
			continue
		}

		modifies := make([]proto.Modification, 0, 3)
		workerCntLimit := latestJob.ReorgMeta.GetConcurrency()
		requiredSlots, err := adjustConcurrency(ctx, taskManager, workerCntLimit)
		if err != nil {
			logger.Error("adjust required slots failed", zap.Error(err))
			continue
		}
		if requiredSlots != lastRequiredSlots {
			modifies = append(modifies, proto.Modification{
				Type: proto.ModifyRequiredSlots,
				To:   int64(requiredSlots),
			})
		}
		batchSize := latestJob.ReorgMeta.GetBatchSize()
		if batchSize != lastBatchSize {
			modifies = append(modifies, proto.Modification{
				Type: proto.ModifyBatchSize,
				To:   int64(batchSize),
			})
		}
		maxWriteSpeed := latestJob.ReorgMeta.GetMaxWriteSpeed()
		if maxWriteSpeed != lastMaxWriteSpeed {
			modifies = append(modifies, proto.Modification{
				Type: proto.ModifyMaxWriteSpeed,
				To:   int64(maxWriteSpeed),
			})
		}
		if len(modifies) == 0 {
			continue
		}
		currTask, err := taskManager.GetTaskByID(ctx, taskID)
		if err != nil {
			if goerrors.Is(err, storage.ErrTaskNotFound) {
				logger.Info("task not found, might already finished")
				return
			}
			logger.Error("get task failed, will retry later", zap.Error(err))
			continue
		}
		if !currTask.State.CanMoveToModifying() {
			// user might modify param again while another modify is ongoing.
			logger.Info("task state is not suitable for modifying, will retry later",
				zap.String("state", currTask.State.String()))
			continue
		}
		if err = taskManager.ModifyTaskByID(ctx, taskID, &proto.ModifyParam{
			PrevState:     currTask.State,
			Modifications: modifies,
		}); err != nil {
			logger.Error("modify task failed", zap.Error(err))
			continue
		}
		logger.Info("modify task success",
			zap.Int("oldRequiredSlots", lastRequiredSlots), zap.Int("newRequiredSlots", requiredSlots),
			zap.Int("oldBatchSize", lastBatchSize), zap.Int("newBatchSize", batchSize),
			zap.String("oldMaxWriteSpeed", units.HumanSize(float64(lastMaxWriteSpeed))),
			zap.String("newMaxWriteSpeed", units.HumanSize(float64(maxWriteSpeed))),
		)
		lastRequiredSlots = requiredSlots
		lastBatchSize = batchSize
		lastMaxWriteSpeed = maxWriteSpeed
	}
}

func adjustConcurrency(ctx context.Context, taskMgr storage.Manager, workerCnt int) (int, error) {
	cpuCount, err := taskMgr.GetCPUCountOfNode(ctx)
	if err != nil {
		return 0, err
	}
	return min(workerCnt, cpuCount), nil
}

// EstimateTableRowSizeForTest is used for test.
var EstimateTableRowSizeForTest = estimateTableRowSize

// estimateTableRowSize estimates the row size in bytes of a table.
// This function tries to retrieve row size in following orders:
//  1. AVG_ROW_LENGTH column from information_schema.tables.
//  2. region info's approximate key size / key number.
func estimateTableRowSize(
	ctx context.Context,
	store kv.Storage,
	exec sqlexec.RestrictedSQLExecutor,
	tbl table.Table,
) (sizeInBytes int) {
	defer util.Recover(metrics.LabelDDL, "estimateTableRowSize", nil, false)
	var gErr error
	defer func() {
		tidblogutil.Logger(ctx).Info("estimate row size",
			zap.Int64("tableID", tbl.Meta().ID), zap.Int("size", sizeInBytes), zap.Error(gErr))
	}()
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil,
		"select AVG_ROW_LENGTH from information_schema.tables where TIDB_TABLE_ID = %?", tbl.Meta().ID)
	if err != nil {
		gErr = err
		return 0
	}
	if len(rows) == 0 {
		gErr = errors.New("no average row data")
		return 0
	}
	avgRowSize := rows[0].GetInt64(0)
	if avgRowSize != 0 {
		return int(avgRowSize)
	}
	regionRowSize, err := estimateRowSizeFromRegion(ctx, store, tbl)
	if err != nil {
		gErr = err
		return 0
	}
	return regionRowSize
}

func estimateRowSizeFromRegion(ctx context.Context, store kv.Storage, tbl table.Table) (int, error) {
	// We use the second region to prevent the influence of the front and back tables.
	const regionLimit = 3
	regions, err := listTableRegions(ctx, store, tbl.Meta().ID, regionLimit)
	if err != nil {
		return 0, err
	}
	if len(regions) != regionLimit {
		return 0, fmt.Errorf("less than 3 regions")
	}
	sample := regions[1]
	// ApproximateSize is SST/blob file size (can reflect compression), while
	// ApproximateKvSize is KV data size and usually closer to logical table size.
	sizeInMiB := max(sample.ApproximateSize, sample.ApproximateKvSize)
	if sample.ApproximateKeys == 0 || sizeInMiB == 0 {
		return 0, fmt.Errorf("zero approximate size")
	}
	return int(uint64(sizeInMiB)*size.MB) / int(sample.ApproximateKeys), nil
}

func canRunTiKVSpacePrecheck(store kv.Storage) (bool, error) {
	if pdStore, ok := store.(kv.StorageWithPD); ok && pdStore.GetPDClient() != nil {
		return true, nil
	}
	hStore, ok := store.(helper.Storage)
	if !ok {
		return false, fmt.Errorf("store %T does not implement helper.Storage", store)
	}
	return hStore.GetPDHTTPClient() != nil, nil
}

func collectTiKVStoreCapacity(ctx context.Context, store kv.Storage) (*TiKVClusterCapacity, error) {
	capacity, err := collectTiKVStoreCapacityFromPDGRPC(ctx, store)
	if err == nil {
		return capacity, nil
	}
	logutil.DDLLogger().Warn("failed to collect TiKV store capacity from PD gRPC, fallback to PD HTTP",
		zap.Error(err))
	return collectTiKVStoreCapacityFromPDHTTP(ctx, store)
}

func collectTiKVStoreCapacityFromPDHTTP(ctx context.Context, store kv.Storage) (*TiKVClusterCapacity, error) {
	hStore, ok := store.(helper.Storage)
	if !ok {
		return nil, fmt.Errorf("store %T does not implement helper.Storage", store)
	}
	h := &helper.Helper{
		Store:       hStore,
		RegionCache: hStore.GetRegionCache(),
	}
	pdCli, err := h.TryGetPDHTTPClient()
	if err != nil {
		return nil, err
	}
	storesInfo, err := pdCli.GetStores(ctx)
	if err != nil {
		return nil, err
	}
	capacity, err := sumTiKVStoreCapacity(storesInfo.Stores)
	if err != nil {
		return nil, err
	}
	capacity.Source = tikvCapacitySourcePDHTTPCapacityMinusAvail
	return capacity, nil
}

func collectTiKVStoreCapacityFromPDGRPC(ctx context.Context, store kv.Storage) (*TiKVClusterCapacity, error) {
	pdStore, ok := store.(kv.StorageWithPD)
	if !ok {
		return nil, fmt.Errorf("store %T does not implement kv.StorageWithPD", store)
	}
	pdCli := pdStore.GetPDClient()
	if pdCli == nil {
		return nil, errors.New("pd client unavailable")
	}
	stores, err := pdCli.GetAllStores(ctx, opt.WithExcludeTombstone())
	if err != nil {
		return nil, err
	}
	capacity := &TiKVClusterCapacity{Source: tikvCapacitySourcePDGRPCStoreStats}
	for _, store := range stores {
		if isNonTiKVMetaStore(store) {
			continue
		}
		storeCapacity, err := collectTiKVStoreCapacityFromStoreStats(ctx, pdCli, store)
		if err != nil {
			return nil, err
		}
		appendTiKVStoreCapacity(capacity, storeCapacity)
	}
	return capacity, nil
}

func collectTiKVStoreCapacityFromStoreStats(ctx context.Context, pdCli pdStoreStatsClient, store *metapb.Store) (*TiKVStoreCapacity, error) {
	if store == nil {
		return nil, errors.New("store is nil")
	}
	stats, err := getPDStoreStats(ctx, pdCli, store.GetId())
	if err != nil {
		return nil, errors.Annotatef(err, "get store %d stats", store.GetId())
	}
	return &TiKVStoreCapacity{
		StoreID:        int64(store.GetId()),
		TotalBytes:     stats.GetCapacity(),
		AvailableBytes: stats.GetAvailable(),
		UsedBytes:      stats.GetUsedSize(),
	}, nil
}

func getPDStoreStats(ctx context.Context, pdCli pdStoreStatsClient, storeID uint64) (*pdpb.StoreStats, error) {
	serviceDiscovery := pdCli.GetServiceDiscovery()
	if serviceDiscovery == nil {
		return nil, errors.New("pd service discovery unavailable")
	}
	serviceClient := serviceDiscovery.GetServiceClient()
	if serviceClient == nil || serviceClient.GetClientConn() == nil {
		return nil, errors.New("pd grpc client unavailable")
	}
	req := &pdpb.GetStoreRequest{
		Header: &pdpb.RequestHeader{
			ClusterId:       pdCli.GetClusterID(ctx),
			CallerId:        string(caller.GetCallerID()),
			CallerComponent: string(caller.Ddl),
		},
		StoreId: storeID,
	}
	reqCtx, cancel := context.WithTimeout(ctx, pdStoreStatsRequestTimeout)
	defer cancel()
	reqCtx = serviceClient.BuildGRPCTargetContext(reqCtx, true)
	resp, err := pdpb.NewPDClient(serviceClient.GetClientConn()).GetStore(reqCtx, req)
	if needRetryPDStoreStats(serviceClient, resp, err) {
		serviceClient = serviceDiscovery.GetServiceClient()
		if serviceClient != nil && serviceClient.GetClientConn() != nil {
			reqCtx = serviceClient.BuildGRPCTargetContext(reqCtx, true)
			resp, err = pdpb.NewPDClient(serviceClient.GetClientConn()).GetStore(reqCtx, req)
		}
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp == nil {
		return nil, errors.New("pd get store response is nil")
	}
	if resp.GetHeader().GetError() != nil {
		return nil, errors.New(resp.GetHeader().GetError().String())
	}
	if resp.GetStore() == nil {
		return nil, errors.New("store field in rpc response not set")
	}
	if resp.GetStore().GetState() == metapb.StoreState_Tombstone || resp.GetStore().GetNodeState() == metapb.NodeState_Removed {
		return nil, errors.Errorf("store %d is removed", storeID)
	}
	stats := resp.GetStats()
	if stats == nil {
		return nil, errors.Errorf("store %d stats is nil", storeID)
	}
	return stats, nil
}

func needRetryPDStoreStats(serviceClient sd.ServiceClient, resp *pdpb.GetStoreResponse, err error) bool {
	if serviceClient == nil {
		return false
	}
	if resp == nil {
		return serviceClient.NeedRetry(nil, err)
	}
	return serviceClient.NeedRetry(resp.GetHeader().GetError(), err)
}

func sumTiKVStoreUsage(stores []pdhttp.StoreInfo) (*TiKVStoreUsageSnapshot, error) {
	capacity, err := sumTiKVStoreCapacity(stores)
	if err != nil {
		return nil, err
	}
	return &TiKVStoreUsageSnapshot{
		UsedBytes:  capacity.UsedBytes,
		StoreCount: capacity.StoreCount,
	}, nil
}

func sumTiKVStoreCapacity(stores []pdhttp.StoreInfo) (*TiKVClusterCapacity, error) {
	capacity := &TiKVClusterCapacity{Source: tikvCapacitySourcePDHTTPCapacityMinusAvail}
	for _, store := range stores {
		if isNonTiKVStoreInfo(store) {
			continue
		}
		storeCapacity, err := buildTiKVStoreCapacity(store)
		if err != nil {
			return nil, errors.Annotatef(err, "parse store %d capacity", store.Store.ID)
		}
		appendTiKVStoreCapacity(capacity, storeCapacity)
	}
	return capacity, nil
}

func appendTiKVStoreCapacity(capacity *TiKVClusterCapacity, storeCapacity *TiKVStoreCapacity) {
	capacity.StoreCount++
	capacity.TotalBytes += storeCapacity.TotalBytes
	capacity.AvailableBytes += storeCapacity.AvailableBytes
	capacity.UsedBytes += storeCapacity.UsedBytes
	capacity.Stores = append(capacity.Stores, *storeCapacity)
}

func buildTiKVStoreCapacity(store pdhttp.StoreInfo) (*TiKVStoreCapacity, error) {
	capacityBytes, err := units.RAMInBytes(store.Status.Capacity)
	if err != nil {
		return nil, err
	}
	availableBytes, err := units.RAMInBytes(store.Status.Available)
	if err != nil {
		return nil, err
	}
	usedBytes := int64(0)
	if capacityBytes <= availableBytes {
		availableBytes = capacityBytes
	} else {
		usedBytes = capacityBytes - availableBytes
	}
	return &TiKVStoreCapacity{
		StoreID:        store.Store.ID,
		TotalBytes:     uint64(capacityBytes),
		AvailableBytes: uint64(availableBytes),
		UsedBytes:      uint64(usedBytes),
	}, nil
}

func isNonTiKVStoreInfo(store pdhttp.StoreInfo) bool {
	return engine.IsTiFlashHTTPResp(&store.Store)
}

func isNonTiKVMetaStore(store *metapb.Store) bool {
	if store == nil {
		return true
	}
	if store.GetState() == metapb.StoreState_Tombstone || store.GetNodeState() == metapb.NodeState_Removed {
		return true
	}
	return engine.IsTiFlash(store)
}

func calcObservedTiKVCapacityIncrease(initial, final *TiKVClusterCapacity) observedTiKVCapacityIncrease {
	if initial == nil {
		return observedTiKVCapacityIncrease{reason: "initial_capacity_unavailable"}
	}
	if final == nil {
		return observedTiKVCapacityIncrease{reason: "final_capacity_unavailable"}
	}
	if len(initial.Stores) == 0 {
		return observedTiKVCapacityIncrease{reason: "initial_store_details_unavailable"}
	}
	if len(final.Stores) == 0 {
		return observedTiKVCapacityIncrease{reason: "final_store_details_unavailable"}
	}
	initialStores := make(map[int64]uint64, len(initial.Stores))
	var totalInitialUsedBytes uint64
	for _, store := range initial.Stores {
		if _, ok := initialStores[store.StoreID]; ok {
			return observedTiKVCapacityIncrease{reason: "duplicate_store_id"}
		}
		initialStores[store.StoreID] = store.UsedBytes
		totalInitialUsedBytes += store.UsedBytes
	}
	finalStores := make(map[int64]struct{}, len(final.Stores))
	var totalFinalUsedBytes uint64
	for _, store := range final.Stores {
		if _, ok := finalStores[store.StoreID]; ok {
			return observedTiKVCapacityIncrease{reason: "duplicate_store_id"}
		}
		finalStores[store.StoreID] = struct{}{}
		_, ok := initialStores[store.StoreID]
		if !ok {
			return observedTiKVCapacityIncrease{reason: "store_set_changed"}
		}
		totalFinalUsedBytes += store.UsedBytes
		delete(initialStores, store.StoreID)
	}
	if len(initialStores) > 0 {
		return observedTiKVCapacityIncrease{reason: "store_set_changed"}
	}
	var increase uint64
	if totalFinalUsedBytes > totalInitialUsedBytes {
		increase = totalFinalUsedBytes - totalInitialUsedBytes
	}
	return observedTiKVCapacityIncrease{increase: int64(increase), reliable: true, reason: "ok"}
}

func (w *worker) logDistTaskObservedTiKVUsage(taskMgr *storage.TaskManager, taskKey string, jobID int64) {
	w.logDistTaskObservedTiKVUsageWithOptions(taskMgr, taskKey, jobID, observedTiKVUsageLogOptions{
		phase:      observedTiKVUsagePhaseTaskEnd,
		observedAt: time.Now(),
	})
}

func buildAddIndexPostTaskObservationDelays(taskExecutionDuration time.Duration) []time.Duration {
	if taskExecutionDuration <= 0 {
		return nil
	}
	delays := make([]time.Duration, 0, len(addIndexPostTaskObservationDelays))
	for _, observationDelay := range addIndexPostTaskObservationDelays {
		if observationDelay.durationMultiplierDenominator <= 0 {
			continue
		}
		delay := taskExecutionDuration *
			time.Duration(observationDelay.durationMultiplierNumerator) /
			time.Duration(observationDelay.durationMultiplierDenominator)
		if delay < observationDelay.minDelay {
			delay = observationDelay.minDelay
		}
		if delay <= 0 {
			continue
		}
		delays = append(delays, delay)
	}
	return delays
}

func observedTiKVUsageTaskTiming(task *proto.Task, fallbackObservedAt time.Time) (time.Time, time.Duration) {
	if task == nil {
		return time.Time{}, 0
	}
	taskEndTime := task.StateUpdateTime
	if taskEndTime.IsZero() {
		taskEndTime = fallbackObservedAt
	}
	if task.StartTime.IsZero() || taskEndTime.IsZero() || taskEndTime.Before(task.StartTime) {
		return taskEndTime, 0
	}
	return taskEndTime, taskEndTime.Sub(task.StartTime)
}

func (w *worker) scheduleDelayedDistTaskObservedTiKVUsage(taskMgr *storage.TaskManager, taskKey string, jobID int64) {
	task, err := taskMgr.GetTaskByKeyWithHistory(w.workCtx, taskKey)
	if err != nil {
		logutil.DDLLogger().Warn("cannot get add-index task for delayed TiKV capacity logging",
			zap.String("task_key", taskKey), zap.Int64("jobID", jobID), zap.Error(err))
		return
	}
	if task == nil || task.State != proto.TaskStateSucceed {
		return
	}

	now := time.Now()
	taskEndTime, taskExecutionDuration := observedTiKVUsageTaskTiming(task, now)
	delays := buildAddIndexPostTaskObservationDelays(taskExecutionDuration)
	if len(delays) == 0 {
		logutil.DDLLogger().Info("skip delayed TiKV capacity logging because task execution duration is unavailable for add-index task",
			zap.String("task_key", taskKey),
			zap.Int64("jobID", jobID),
			zap.Int64("taskID", task.ID))
		return
	}

	delayMillis := make([]int64, 0, len(delays))
	for i, delay := range delays {
		delayMillis = append(delayMillis, delay.Milliseconds())
		sequence := i + 1
		remainingDelay := delay
		if !taskEndTime.IsZero() && !now.Before(taskEndTime) {
			elapsedSinceTaskEnd := now.Sub(taskEndTime)
			if elapsedSinceTaskEnd >= delay {
				remainingDelay = 0
			} else {
				remainingDelay = delay - elapsedSinceTaskEnd
			}
		}
		w.wg.Run(func() {
			timer := time.NewTimer(remainingDelay)
			defer timer.Stop()
			select {
			case <-w.workCtx.Done():
				return
			case <-timer.C:
			}
			w.logDistTaskObservedTiKVUsageWithOptions(taskMgr, taskKey, jobID, observedTiKVUsageLogOptions{
				phase:          observedTiKVUsagePhasePostTask,
				sequence:       sequence,
				scheduledDelay: delay,
				observedAt:     time.Now(),
			})
		})
	}

	logutil.DDLLogger().Info("scheduled delayed TiKV capacity logging for add-index task",
		zap.Int64("jobID", jobID),
		zap.Int64("taskID", task.ID),
		zap.String("task_key", taskKey),
		zap.Int64("task_execution_duration_ms", taskExecutionDuration.Milliseconds()),
		zap.Int64s("scheduled_delay_ms", delayMillis))
}

func (w *worker) logDistTaskObservedTiKVUsageWithOptions(taskMgr *storage.TaskManager, taskKey string, jobID int64, opts observedTiKVUsageLogOptions) {
	task, err := taskMgr.GetTaskByKeyWithHistory(w.workCtx, taskKey)
	if err != nil {
		logutil.DDLLogger().Warn("cannot get add-index task for observed TiKV capacity logging",
			zap.String("task_key", taskKey), zap.Int64("jobID", jobID), zap.Error(err))
		return
	}
	if task == nil || task.State != proto.TaskStateSucceed {
		return
	}

	taskMeta := &BackfillTaskMeta{}
	if err := json.Unmarshal(task.Meta, taskMeta); err != nil {
		logutil.DDLLogger().Warn("cannot decode add-index task meta for observed TiKV capacity logging",
			zap.String("task_key", taskKey), zap.Int64("jobID", jobID), zap.Error(err))
		return
	}
	if taskMeta.InitialTiKVStoreUsage == nil {
		logutil.DDLLogger().Info("skip observed TiKV capacity logging because initial snapshot is unavailable for add-index task",
			zap.String("task_key", taskKey), zap.Int64("jobID", jobID), zap.Int64("taskID", task.ID))
		return
	}

	finalCapacity, err := collectTiKVStoreCapacity(w.workCtx, w.store)
	if err != nil {
		logutil.DDLLogger().Warn("failed to collect final TiKV store usage for add-index task",
			zap.String("task_key", taskKey), zap.Int64("jobID", jobID), zap.Int64("taskID", task.ID), zap.Error(err))
		return
	}
	finalUsage := &TiKVStoreUsageSnapshot{
		UsedBytes:  finalCapacity.UsedBytes,
		StoreCount: finalCapacity.StoreCount,
	}
	initialCapacity := taskMeta.InitialTiKVCapacity
	initialStoreCount := taskMeta.InitialTiKVStoreUsage.StoreCount
	if taskMeta.InitialTiKVCapacity != nil {
		initialStoreCount = taskMeta.InitialTiKVCapacity.StoreCount
	} else if taskMeta.InitialTiKVStoreUsage != nil {
		initialCapacity = &TiKVClusterCapacity{
			UsedBytes:  taskMeta.InitialTiKVStoreUsage.UsedBytes,
			StoreCount: taskMeta.InitialTiKVStoreUsage.StoreCount,
		}
	}

	subtasks, err := taskMgr.GetSubtasksWithHistory(w.workCtx, task.ID, proto.BackfillStepReadIndex)
	if err != nil {
		logutil.DDLLogger().Warn("failed to collect read-index subtasks for add-index task",
			zap.String("task_key", taskKey), zap.Int64("jobID", jobID), zap.Int64("taskID", task.ID), zap.Error(err))
		return
	}
	_, logicalIndexKVBytes, err := sumReadIndexSubtaskSummary(subtasks)
	if err != nil {
		logutil.DDLLogger().Warn("failed to sum read-index processed bytes for add-index task",
			zap.String("task_key", taskKey), zap.Int64("jobID", jobID), zap.Int64("taskID", task.ID), zap.Error(err))
		return
	}

	observedIncrease := calcObservedTiKVCapacityIncrease(initialCapacity, finalCapacity)
	phase := opts.phase
	if phase == "" {
		phase = observedTiKVUsagePhaseTaskEnd
	}
	observedAt := opts.observedAt
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	taskEndTime, taskExecutionDuration := observedTiKVUsageTaskTiming(task, observedAt)
	actualDelay := time.Duration(0)
	if !taskEndTime.IsZero() && !observedAt.Before(taskEndTime) {
		actualDelay = observedAt.Sub(taskEndTime)
	}
	logFields := []zap.Field{
		zap.Int64("jobID", jobID),
		zap.Int64("taskID", task.ID),
		zap.String("task_key", taskKey),
		zap.String("observation_phase", phase),
		zap.Int("observation_sequence", opts.sequence),
		zap.Int64("task_execution_duration_ms", taskExecutionDuration.Milliseconds()),
		zap.Int64("scheduled_delay_ms", opts.scheduledDelay.Milliseconds()),
		zap.Int64("actual_delay_ms", actualDelay.Milliseconds()),
	}
	logFields = appendBlockSamplePredictionLogFields(logFields, sampleTiKVIndexPredictionResult{
		PredictedBytes:                 taskMeta.BlockSampleSteadyPredictedTiKVIndexBytes,
		MVCCOverheadBytes:              taskMeta.BlockSampleMVCCOverheadTotalBytes,
		UseStats:                       taskMeta.BlockSampleUseStats,
		SampledRegionCount:             taskMeta.BlockSamplePredictionRegionCount,
		SampledRowCount:                taskMeta.BlockSamplePredictionRowCount,
		ReadErrorCount:                 taskMeta.BlockSamplePredictionReadErrorCount,
		EncodedKeySharedPrefixAvgBytes: taskMeta.BlockSampleEncodedKeySharedPrefixAvg,
		RawKeySharedPrefixAvgBytes:     taskMeta.BlockSampleRawKeySharedPrefixAvg,
		RawKeyLengthAvgBytes:           taskMeta.BlockSampleRawKeyLengthAvg,
	},
		taskMeta.BlockSamplePeakPredictedTiKVIndexBytes,
		taskMeta.TiKVReplicaCount,
		taskMeta.TiKVReplicaCountSource,
		taskMeta.TiKVReplicaCountPhysicalID)
	logFields = append(logFields,
		zap.Int64("logical_index_kv_bytes", logicalIndexKVBytes),
		zap.Int64("observed_tikv_capacity_increase_bytes", observedIncrease.increase),
		zap.Bool("observed_tikv_capacity_reliable", observedIncrease.reliable),
		zap.String("observed_tikv_capacity_reliable_reason", observedIncrease.reason),
		zap.String("initial_tikv_capacity_source", initialCapacity.Source),
		zap.String("final_tikv_capacity_source", finalCapacity.Source),
		zap.Uint64("initial_tikv_used_bytes", taskMeta.InitialTiKVStoreUsage.UsedBytes),
		zap.Uint64("final_tikv_used_bytes", finalUsage.UsedBytes),
		zap.Int("initial_tikv_store_count", initialStoreCount),
		zap.Int("final_tikv_store_count", finalUsage.StoreCount),
	)
	logutil.DDLLogger().Info("observed TiKV capacity increase for add-index task", logFields...)
	failpoint.InjectCall("afterLogObservedTiKVCapacityIncrease",
		task.ID, logicalIndexKVBytes, observedIncrease.increase,
		int64(taskMeta.InitialTiKVStoreUsage.UsedBytes), int64(finalUsage.UsedBytes))
}

func checkTiKVSpaceForAddIndex(capacity *TiKVClusterCapacity, predictedTiKVIndexBytes uint64) error {
	if capacity == nil {
		return errors.New("initial TiKV capacity snapshot is nil")
	}
	if capacity.StoreCount == 0 {
		return dbterror.ErrIngestCheckEnvFailed.FastGenByArgs("no TiKV stores found for add index capacity check")
	}
	clusterRemaining := remainingTiKVBytes(capacity.AvailableBytes, predictedTiKVIndexBytes)
	clusterThreshold := uint64(float64(capacity.TotalBytes) * 0.20)
	if clusterRemaining < clusterThreshold {
		return dbterror.ErrIngestCheckEnvFailed.FastGenByArgs(
			fmt.Sprintf("insufficient TiKV cluster capacity predicted for add index: cluster_available_bytes=%d predict_tikv_index_bytes=%d cluster_total_bytes=%d cluster_remaining_bytes=%d cluster_threshold_bytes=%d",
				capacity.AvailableBytes, predictedTiKVIndexBytes, capacity.TotalBytes, clusterRemaining, clusterThreshold))
	}

	perStorePredictBytes := predictedTiKVIndexBytes / uint64(capacity.StoreCount)
	for _, store := range capacity.Stores {
		storeRemaining := remainingTiKVBytes(store.AvailableBytes, perStorePredictBytes)
		storeThreshold := uint64(float64(store.TotalBytes) * 0.15)
		if storeRemaining < storeThreshold {
			return dbterror.ErrIngestCheckEnvFailed.FastGenByArgs(
				fmt.Sprintf("insufficient TiKV store capacity predicted for add index: store_id=%d store_available_bytes=%d predict_tikv_index_bytes=%d tikv_store_count=%d per_store_predict_bytes=%d store_total_bytes=%d store_remaining_bytes=%d store_threshold_bytes=%d",
					store.StoreID, store.AvailableBytes, predictedTiKVIndexBytes, capacity.StoreCount, perStorePredictBytes, store.TotalBytes, storeRemaining, storeThreshold))
		}
	}
	return nil
}

func evaluateAddIndexTiKVSpacePrecheck(capacity *TiKVClusterCapacity, predictedTiKVIndexBytes uint64, enforce bool) (checkErr, rejectErr error) {
	checkErr = checkTiKVSpaceForAddIndex(capacity, predictedTiKVIndexBytes)
	if checkErr != nil && enforce {
		rejectErr = checkErr
	}
	return checkErr, rejectErr
}

func remainingTiKVBytes(availableBytes, predictBytes uint64) uint64 {
	if availableBytes <= predictBytes {
		return 0
	}
	return availableBytes - predictBytes
}

func (w *worker) resolveAddIndexTiKVReplicaCount(
	ctx context.Context,
	sctx sessionctx.Context,
	tbl table.Table,
	reorgInfo *reorgInfo,
) (uint64, string, int64, error) {
	if reorgInfo == nil {
		return 0, "", 0, errors.New("reorg info is nil")
	}
	tblInfo := tbl.Meta()
	targetIndexes, err := collectBackfillIndexes(tblInfo, reorgInfo)
	if err != nil {
		return 0, "", 0, err
	}
	if len(targetIndexes) == 0 {
		return 0, "", 0, errors.New("no backfill indexes found")
	}

	physicalID := representativeAddIndexTiKVReplicaPhysicalID(tblInfo, targetIndexes)
	if is := sctx.GetLatestInfoSchema(); is != nil {
		if bundle, ok := is.PlacementBundleByPhysicalTableID(physicalID); ok {
			if replicaCount := tiKVReplicaCountFromBundle(bundle, physicalID); replicaCount > 0 {
				return replicaCount, tikvReplicaCountSourceInfoSchemaPlacementBundle, physicalID, nil
			}
		}
		if physicalID != tblInfo.ID {
			if bundle, ok := is.PlacementBundleByPhysicalTableID(tblInfo.ID); ok {
				if replicaCount := tiKVReplicaCountFromBundle(bundle, physicalID); replicaCount > 0 {
					return replicaCount, tikvReplicaCountSourceInfoSchemaPlacementBundle, physicalID, nil
				}
			}
		}
	}

	if replicaCount, err := collectPDMaxReplicas(ctx, w.store); err == nil && replicaCount > 0 {
		return replicaCount, tikvReplicaCountSourcePDMaxReplicas, physicalID, nil
	}
	return defaultTiKVReplicaCount, tikvReplicaCountSourceFallbackDefault, physicalID, nil
}

func representativeAddIndexTiKVReplicaPhysicalID(tblInfo *model.TableInfo, targetIndexes []*model.IndexInfo) int64 {
	for _, idxInfo := range targetIndexes {
		if idxInfo != nil && idxInfo.Global {
			return tblInfo.ID
		}
	}
	if pi := tblInfo.GetPartitionInfo(); pi != nil && len(pi.Definitions) > 0 {
		return pi.Definitions[0].ID
	}
	return tblInfo.ID
}

func tiKVReplicaCountFromBundle(bundle *placement.Bundle, physicalID int64) uint64 {
	if bundle == nil {
		return 0
	}
	startKeyHex, endKeyHex, filterByRange := bundleRuleRangeForPhysicalID(bundle, physicalID)
	var replicaCount uint64
	for _, rule := range bundle.Rules {
		if rule == nil || rule.Count <= 0 || ruleTargetsNonTiKV(rule) {
			continue
		}
		if filterByRange && (rule.StartKeyHex != startKeyHex || rule.EndKeyHex != endKeyHex) {
			continue
		}
		switch rule.Role {
		case pdhttp.Leader, pdhttp.Voter, pdhttp.Follower, pdhttp.Learner:
			replicaCount += uint64(rule.Count)
		}
	}
	return replicaCount
}

func bundleRuleRangeForPhysicalID(
	bundle *placement.Bundle,
	physicalID int64,
) (startKeyHex, endKeyHex string, filterByRange bool) {
	if physicalID > 0 {
		startKeyHex, endKeyHex := placementBundleRuleKeyRangeHex(physicalID)
		if bundleHasRuleRange(bundle, startKeyHex, endKeyHex) {
			return startKeyHex, endKeyHex, true
		}
	}
	return firstBundleRuleRange(bundle)
}

func placementBundleRuleKeyRangeHex(physicalID int64) (startKeyHex, endKeyHex string) {
	startKey := utilcodec.EncodeBytes(nil, tablecodec.GenTablePrefix(physicalID))
	endKey := utilcodec.EncodeBytes(nil, tablecodec.GenTablePrefix(physicalID+1))
	return hex.EncodeToString(startKey), hex.EncodeToString(endKey)
}

func bundleHasRuleRange(bundle *placement.Bundle, startKeyHex, endKeyHex string) bool {
	for _, rule := range bundle.Rules {
		if rule != nil && rule.StartKeyHex == startKeyHex && rule.EndKeyHex == endKeyHex {
			return true
		}
	}
	return false
}

func firstBundleRuleRange(bundle *placement.Bundle) (startKeyHex, endKeyHex string, filterByRange bool) {
	for _, rule := range bundle.Rules {
		if rule == nil {
			continue
		}
		if rule.StartKeyHex != "" || rule.EndKeyHex != "" {
			return rule.StartKeyHex, rule.EndKeyHex, true
		}
	}
	return "", "", false
}

func ruleTargetsNonTiKV(rule *pdhttp.Rule) bool {
	if rule.GroupID == placement.TiFlashRuleGroupID {
		return true
	}
	for _, constraint := range rule.LabelConstraints {
		if constraint.Key != placement.EngineLabelKey || constraint.Op != pdhttp.In || len(constraint.Values) == 0 {
			continue
		}
		allNonTiKV := true
		for _, value := range constraint.Values {
			if value != placement.EngineLabelTiFlash && value != placement.EngineLabelTiFlashCompute {
				allNonTiKV = false
				break
			}
		}
		if allNonTiKV {
			return true
		}
	}
	return false
}

func collectPDMaxReplicas(ctx context.Context, store kv.Storage) (uint64, error) {
	hStore, ok := store.(helper.Storage)
	if !ok {
		return 0, fmt.Errorf("store %T does not implement helper.Storage", store)
	}
	pdCli, err := helper.NewHelper(hStore).TryGetPDHTTPClient()
	if err != nil {
		return 0, err
	}
	replicateConfig, err := pdCli.GetReplicateConfig(ctx)
	if err != nil {
		return 0, err
	}
	if replicaCount, ok := pdReplicateConfigMaxReplicas(replicateConfig); ok {
		return replicaCount, nil
	}
	return 0, errors.New("PD replicate config max-replicas is unavailable")
}

func pdReplicateConfigMaxReplicas(replicateConfig map[string]any) (uint64, bool) {
	value, ok := replicateConfig["max-replicas"]
	if !ok {
		return 0, false
	}
	switch v := value.(type) {
	case uint64:
		return v, v > 0
	case uint:
		return uint64(v), v > 0
	case int:
		if v <= 0 {
			return 0, false
		}
		return uint64(v), true
	case int64:
		if v <= 0 {
			return 0, false
		}
		return uint64(v), true
	case float64:
		if v <= 0 || v > float64(math.MaxUint64) || math.Trunc(v) != v {
			return 0, false
		}
		return uint64(v), true
	case json.Number:
		replicaCount, err := strconv.ParseUint(v.String(), 10, 64)
		return replicaCount, err == nil && replicaCount > 0
	case string:
		replicaCount, err := strconv.ParseUint(v, 10, 64)
		return replicaCount, err == nil && replicaCount > 0
	default:
		return 0, false
	}
}

func scalePredictedBytesByReplicaCount(predictedBytes, replicaCount uint64) uint64 {
	if replicaCount <= 1 {
		return predictedBytes
	}
	return saturatingMulUint64(predictedBytes, replicaCount)
}

func estimateBlockSamplePeakPredictedTiKVIndexBytes(steadyPredictedBytes uint64) uint64 {
	return saturatingMulUint64(steadyPredictedBytes, blockSamplePeakPredictionFactor)
}

func saturatingMulUint64(value, factor uint64) uint64 {
	if value == 0 || factor == 0 {
		return 0
	}
	if value > math.MaxUint64/factor {
		return math.MaxUint64
	}
	return value * factor
}

func appendBlockSamplePredictionLogFields(
	fields []zap.Field,
	prediction sampleTiKVIndexPredictionResult,
	peakPredictedBytes, replicaCount uint64,
	replicaCountSource string,
	replicaCountPhysicalID int64,
) []zap.Field {
	return append(fields,
		zap.Uint64("block_sample_steady_predicted_tikv_index_bytes", prediction.PredictedBytes),
		zap.Uint64("block_sample_peak_predicted_tikv_index_bytes", peakPredictedBytes),
		zap.Uint64("block_sample_mvcc_overhead_total_bytes", prediction.MVCCOverheadBytes),
		zap.Bool("use_stats", prediction.UseStats),
		zap.Uint64("tikv_replica_count", replicaCount),
		zap.String("tikv_replica_count_source", replicaCountSource),
		zap.Int64("tikv_replica_count_physical_id", replicaCountPhysicalID),
		zap.Int("block_sample_prediction_region_count", prediction.SampledRegionCount),
		zap.Int("block_sample_prediction_row_count", prediction.SampledRowCount),
		zap.Int("block_sample_prediction_read_error_count", prediction.ReadErrorCount),
		zap.Float64("block_sample_encoded_key_shared_prefix_avg", prediction.EncodedKeySharedPrefixAvgBytes),
		zap.Float64("block_sample_raw_key_shared_prefix_avg", prediction.RawKeySharedPrefixAvgBytes),
		zap.Float64("block_sample_raw_key_length_avg", prediction.RawKeyLengthAvgBytes),
	)
}

func addIndexTiKVSpacePrecheckLogFields(
	jobID int64,
	taskKey string,
	prediction sampleTiKVIndexPredictionResult,
	peakPredictedBytes uint64,
	replicaCount uint64,
	replicaCountSource string,
	replicaCountPhysicalID int64,
	capacity *TiKVClusterCapacity,
	enforce bool,
) []zap.Field {
	fields := []zap.Field{
		zap.Int64("jobID", jobID),
		zap.String("task-key", taskKey),
	}
	fields = appendBlockSamplePredictionLogFields(fields, prediction, peakPredictedBytes, replicaCount, replicaCountSource, replicaCountPhysicalID)
	fields = append(fields,
		zap.Uint64("block_sample_peak_prediction_factor", blockSamplePeakPredictionFactor),
		zap.Bool("enforce_disk_space_precheck_before_add_index", enforce))
	if capacity != nil {
		fields = append(fields,
			zap.Uint64("cluster_available_bytes", capacity.AvailableBytes),
			zap.Uint64("cluster_total_bytes", capacity.TotalBytes),
			zap.Int("tikv_store_count", capacity.StoreCount))
	}
	return fields
}

const (
	samplePredictionMaxRegionCount       = 5
	blockSamplePredictionProbeRows       = 10
	blockSamplePredictionMaxRows         = 2048
	blockSamplePredictionMaxLogicalBytes = 4 * units.MiB
	samplePredictionMaxReadErrors        = 3
	samplePredictionMaxSkipRows          = 256
)

const (
	tikvMVCCTimestampBytes       = 8
	tikvMVCCShortValueMaxBytes   = 64
	tikvMVCCPredictionFallbackTS = uint64(1) << 56
)

type sampleTiKVIndexPredictionResult struct {
	PredictedBytes                 uint64
	MVCCOverheadBytes              uint64
	UseStats                       bool
	SampledRegionCount             int
	SampledRowCount                int
	ReadErrorCount                 int
	EncodedKeySharedPrefixAvgBytes float64
	RawKeySharedPrefixAvgBytes     float64
	RawKeyLengthAvgBytes           float64
}

type samplePredictionPhysicalTable struct {
	physicalTbl table.PhysicalTable
	indexes     []table.Index
	rowCount    int64
}

type samplePredictionPhysicalTableSelection struct {
	samplePredictionPhysicalTable
	regionCount int
}

type samplePredictionRegionTask struct {
	samplePredictionPhysicalTable
	region samplePredictionRegion
}

type samplePredictionRegion struct {
	StartKey        kv.Key
	EndKey          kv.Key
	ApproximateKeys int64
}

func (w *worker) predictTiKVIndexBytesBlockSample(
	ctx context.Context,
	sctx sessionctx.Context,
	tbl table.Table,
	reorgInfo *reorgInfo,
) (sampleTiKVIndexPredictionResult, error) {
	result := sampleTiKVIndexPredictionResult{}
	if reorgInfo == nil {
		return result, errors.New("reorg info is nil")
	}

	targetIndexInfos, err := collectBackfillIndexes(tbl.Meta(), reorgInfo)
	if err != nil {
		return result, err
	}
	physicalTables, totalRowCount, useStats, err := w.buildSamplePredictionPhysicalTables(tbl, targetIndexInfos)
	if err != nil {
		return result, err
	}
	result.UseStats = useStats
	if totalRowCount <= 0 {
		return result, nil
	}
	jobCtx := w.jobContext(reorgInfo.Job.ID, reorgInfo.Job.ReorgMeta)
	currentVer, err := getValidCurrentVersion(w.store)
	if err != nil {
		return result, err
	}

	baseSeed := samplePredictionSeed(reorgInfo.Job.ID, tbl.Meta().ID)
	regionTasks, readErrors, err := w.buildSamplePredictionRegionTasks(ctx, reorgInfo.Job.ID, "block sample prediction", physicalTables, baseSeed)
	if readErrors > 0 {
		result.ReadErrorCount += readErrors
		if result.ReadErrorCount > samplePredictionMaxReadErrors {
			return result, dbterror.ErrIngestCheckEnvFailed.FastGenByArgs(
				fmt.Sprintf("add index block sample prediction failed after %d read errors", result.ReadErrorCount))
		}
	}
	if err != nil {
		return result, err
	}
	if len(regionTasks) == 0 {
		return result, nil
	}

	predictedAvgBytesPerRow, mvccAvgBytesPerRow, encodedKeySharedPrefixAvg, rawKeySharedPrefixAvg, rawKeyLengthAvg, sampledRegions, sampledRows, readErrors, err := w.estimateBlockSampleBytesPerRow(
		jobCtx,
		sctx,
		regionTasks,
		currentVer.Ver,
		baseSeed^0xd1b54a32d192ed03,
	)
	result.SampledRegionCount += sampledRegions
	result.SampledRowCount += sampledRows
	if readErrors > 0 {
		result.ReadErrorCount += readErrors
		if result.ReadErrorCount > samplePredictionMaxReadErrors {
			return result, dbterror.ErrIngestCheckEnvFailed.FastGenByArgs(
				fmt.Sprintf("add index block sample prediction failed after %d read errors", result.ReadErrorCount))
		}
	}
	if err != nil {
		return result, err
	}
	if predictedAvgBytesPerRow > 0 {
		result.PredictedBytes = uint64(predictedAvgBytesPerRow * float64(totalRowCount))
		result.MVCCOverheadBytes = uint64(mvccAvgBytesPerRow * float64(totalRowCount))
		result.EncodedKeySharedPrefixAvgBytes = encodedKeySharedPrefixAvg
		result.RawKeySharedPrefixAvgBytes = rawKeySharedPrefixAvg
		result.RawKeyLengthAvgBytes = rawKeyLengthAvg
	}
	return result, nil
}

func (w *worker) estimateBlockSampleBytesPerRow(
	jobCtx *ReorgContext,
	sctx sessionctx.Context,
	regionTasks []samplePredictionRegionTask,
	snapshotTS uint64,
	seed uint64,
) (
	predictedAvgBytesPerRow float64,
	mvccAvgBytesPerRow float64,
	encodedKeySharedPrefixAvg float64,
	rawKeySharedPrefixAvg float64,
	rawKeyLengthAvg float64,
	sampledRegions int,
	totalRows int,
	readErrorCount int,
	err error,
) {
	if len(regionTasks) == 0 {
		return 0, 0, 0, 0, 0, 0, 0, 0, nil
	}
	rnd := rand.New(rand.NewSource(int64(seed)))
	sampledKVCapacity := 0
	for _, task := range regionTasks {
		sampledKVCapacity += blockSamplePredictionMaxRows * max(len(task.indexes), 1)
	}
	var (
		totalLogicalBytes int64
		sampledKVs        = make([]sampledIndexKV, 0, sampledKVCapacity)
		scopeSampleCounts = make(map[sampledIndexKVScope]int)
	)
	for _, task := range regionTasks {
		skipRows := blockSamplePredictionSkipRows(task.region, rnd)
		rowCnt, logicalBytes, kvs, err := w.sampleBlockIndexKVsFromRegion(jobCtx, sctx, task.physicalTbl, task.indexes, task.region, snapshotTS, skipRows)
		if err != nil {
			readErrorCount++
			logutil.DDLLogger().Warn("failed to block-sample add-index prediction rows from region",
				zap.Int64("physicalID", task.physicalTbl.GetPhysicalID()),
				zap.Int("skipRows", skipRows),
				zap.Error(err))
			if readErrorCount > samplePredictionMaxReadErrors {
				return 0, 0, 0, 0, 0, sampledRegions, totalRows, readErrorCount, dbterror.ErrIngestCheckEnvFailed.FastGenByArgs(
					fmt.Sprintf("add index block sample prediction failed after %d read errors: %v", readErrorCount, err))
			}
			continue
		}
		sampledRegions++
		totalRows += rowCnt
		if rowCnt == 0 {
			continue
		}
		totalLogicalBytes += logicalBytes
		sampledKVs = append(sampledKVs, kvs...)
		for scope := range collectSampledIndexKVScopes(kvs) {
			scopeSampleCounts[scope]++
		}
	}
	if totalRows == 0 {
		return 0, 0, 0, 0, 0, sampledRegions, totalRows, readErrorCount, nil
	}
	sortedKVs := sortSampledIndexKVs(sampledKVs)
	encodedKeySharedPrefixAvg = estimateSortedSampledIndexKVEncodedKeySharedPrefixAvg(sortedKVs)
	rawKeySharedPrefixAvg = estimateSortedSampledIndexKVRawKeySharedPrefixAvg(sortedKVs)
	rawKeyLengthAvg = estimateSampledIndexKVRawKeyLengthAvg(sortedKVs)
	estimatedBytes := estimateSortedBlockSampledIndexKVPredictionBytes(sortedKVs, scopeSampleCounts, snapshotTS)
	if estimatedBytes.Err != nil {
		logutil.DDLLogger().Warn("failed to estimate physical TiKV bytes from block-sampled add-index KVs; fallback to logical bytes for sampled rows",
			zap.Int("sampled_rows", totalRows),
			zap.Int("sampled_kv_count", len(sampledKVs)),
			zap.Int("sampled_region_count", sampledRegions),
			zap.Int("sample_scope_count", len(scopeSampleCounts)),
			zap.Error(estimatedBytes.Err))
	}
	if estimatedBytes.PredictedBytes <= 0 {
		estimatedBytes.PredictedBytes = totalLogicalBytes
	}
	return float64(estimatedBytes.PredictedBytes) / float64(totalRows), float64(estimatedBytes.MVCCOverheadBytes) / float64(totalRows), encodedKeySharedPrefixAvg, rawKeySharedPrefixAvg, rawKeyLengthAvg, sampledRegions, totalRows, readErrorCount, nil
}

func (w *worker) sampleBlockIndexKVsFromRegion(
	jobCtx *ReorgContext,
	sctx sessionctx.Context,
	physicalTbl table.PhysicalTable,
	indexes []table.Index,
	region samplePredictionRegion,
	snapshotTS uint64,
	skipRows int,
) (int, int64, []sampledIndexKV, error) {
	if len(region.StartKey) == 0 || len(region.EndKey) == 0 || bytes.Compare(region.StartKey, region.EndKey) >= 0 {
		return 0, 0, nil, nil
	}
	cols := physicalTbl.Cols()
	virtualColumnFiller, err := newSamplePredictionVirtualColumnFiller(sctx.GetExprCtx(), physicalTbl.Meta(), cols)
	if err != nil {
		return 0, 0, nil, errors.Trace(err)
	}

	skipped := 0
	rowCount := 0
	totalBytes := int64(0)
	targetRows := blockSamplePredictionProbeRows
	logicalByteLimit := int64(0)
	kvs := make([]sampledIndexKV, 0, blockSamplePredictionProbeRows*len(indexes))
	err = iterateSnapshotKeys(jobCtx, w.store, kv.PriorityLow, physicalTbl.RecordPrefix(), snapshotTS, region.StartKey, region.EndKey,
		func(handle kv.Handle, rowKey kv.Key, rawRecord []byte) (bool, error) {
			if bytes.Compare(rowKey, region.EndKey) >= 0 {
				return false, nil
			}
			if skipped < skipRows {
				skipped++
				return true, nil
			}
			if rowCount >= targetRows {
				return false, nil
			}
			row, _, err := tables.DecodeRawRowData(sctx.GetExprCtx(), physicalTbl.Meta(), handle, cols, rawRecord)
			if err != nil {
				return false, errors.Trace(err)
			}
			if virtualColumnFiller != nil {
				row, err = virtualColumnFiller.fill(row)
				if err != nil {
					return false, errors.Trace(err)
				}
			}
			rowKVs, rowBytes, err := collectIndexKVsForSampledRow(sctx, physicalTbl, indexes, row, handle)
			if err != nil {
				return false, err
			}
			kvs = append(kvs, rowKVs...)
			totalBytes += rowBytes
			rowCount++
			if rowCount == blockSamplePredictionProbeRows {
				targetRows = blockSamplePredictionTargetRows(rowCount, totalBytes)
				logicalByteLimit = blockSamplePredictionMaxLogicalBytes
			}
			return rowCount < targetRows && (logicalByteLimit == 0 || totalBytes < logicalByteLimit), nil
		},
	)
	if err != nil {
		return rowCount, totalBytes, kvs, err
	}
	if rowCount == 0 {
		return 0, 0, kvs, nil
	}
	return rowCount, totalBytes, kvs, nil
}

type samplePredictionVirtualColumnFiller struct {
	exprCtx              expression.BuildContext
	exprCols             []*expression.Column
	colInfos             []*model.ColumnInfo
	fieldTypes           []*types.FieldType
	virtualColumnOffsets []int
	virtualColumnTypes   []*types.FieldType
}

func newSamplePredictionVirtualColumnFiller(
	exprCtx expression.BuildContext,
	tblInfo *model.TableInfo,
	cols []*table.Column,
) (*samplePredictionVirtualColumnFiller, error) {
	colInfos := make([]*model.ColumnInfo, 0, len(cols))
	fieldTypes := make([]*types.FieldType, 0, len(cols))
	hasVirtualColumn := false
	for _, col := range cols {
		colInfos = append(colInfos, col.ColumnInfo)
		fieldTypes = append(fieldTypes, &col.FieldType)
		if col.IsVirtualGenerated() {
			hasVirtualColumn = true
		}
	}
	if !hasVirtualColumn {
		return nil, nil
	}

	exprCols, _, err := expression.ColumnInfos2ColumnsAndNames(exprCtx, ast.CIStr{}, tblInfo.Name, colInfos, tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	filler := &samplePredictionVirtualColumnFiller{
		exprCtx:    exprCtx,
		exprCols:   exprCols,
		colInfos:   colInfos,
		fieldTypes: fieldTypes,
	}
	filler.virtualColumnOffsets, filler.virtualColumnTypes = copr.CollectVirtualColumnOffsetsAndTypes(exprCtx.GetEvalCtx(), exprCols)
	if len(filler.virtualColumnOffsets) == 0 {
		return nil, nil
	}
	return filler, nil
}

func (f *samplePredictionVirtualColumnFiller) fill(row []types.Datum) ([]types.Datum, error) {
	chk := chunk.NewChunkWithCapacity(f.fieldTypes, 1)
	for i := range f.fieldTypes {
		chk.AppendDatum(i, &row[i])
	}
	if err := table.FillVirtualColumnValue(
		f.virtualColumnTypes,
		f.virtualColumnOffsets,
		f.exprCols,
		f.colInfos,
		f.exprCtx,
		chk,
	); err != nil {
		return nil, errors.Trace(err)
	}
	return chk.GetRow(0).GetDatumRow(f.fieldTypes), nil
}

type sampledIndexKV struct {
	key           []byte
	value         []byte
	rawKey        []byte
	physicalID    int64
	indexID       int64
	isGlobalIndex bool
}

type sampledIndexKVScope struct {
	indexID       int64
	physicalID    int64
	isGlobalIndex bool
}

func (kvPair sampledIndexKV) scope() sampledIndexKVScope {
	scope := sampledIndexKVScope{
		indexID:       kvPair.indexID,
		isGlobalIndex: kvPair.isGlobalIndex,
	}
	if !kvPair.isGlobalIndex {
		scope.physicalID = kvPair.physicalID
	}
	return scope
}

func collectSampledIndexKVScopes(kvs []sampledIndexKV) map[sampledIndexKVScope]struct{} {
	scopes := make(map[sampledIndexKVScope]struct{})
	for _, kvPair := range kvs {
		scopes[kvPair.scope()] = struct{}{}
	}
	return scopes
}

func collectIndexKVsForSampledRow(
	sctx sessionctx.Context,
	physicalTbl table.PhysicalTable,
	indexes []table.Index,
	row []types.Datum,
	handle kv.Handle,
) ([]sampledIndexKV, int64, error) {
	loc := predictionTimeLocation(sctx)
	errCtx := sctx.GetSessionVars().StmtCtx.ErrCtx()
	tblInfo := physicalTbl.Meta()
	pkIdx := tables.FindPrimaryIndex(tblInfo)
	var handleRestoreData []types.Datum
	if tblInfo.IsCommonHandle && tblInfo.CommonHandleVersion != 0 && pkIdx != nil {
		handleRestoreData = extractHandleRestoreDataFromRow(tblInfo, pkIdx, row)
	}

	idxValueBuf := make([]types.Datum, 0, maxIndexColumnCount(indexes))
	kvs := make([]sampledIndexKV, 0, len(indexes))
	var totalBytes int64
	for _, idx := range indexes {
		if idx.Meta().HasCondition() {
			match, err := idx.MeetPartialCondition(row)
			if err != nil {
				return nil, 0, err
			}
			if !match {
				continue
			}
		}
		indexedValues, err := idx.FetchValues(row, idxValueBuf[:0])
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		actualHandle := handle
		if idx.Meta().Global && idx.Meta().GlobalIndexVersion >= model.GlobalIndexVersionV1 {
			actualHandle = kv.NewPartitionHandle(physicalTbl.GetPhysicalID(), handle)
		}
		var rsData []types.Datum
		if len(handleRestoreData) > 0 && (tables.NeedRestoredData(idx.Meta().Columns, tblInfo.Columns) || primaryIndexNeedsRestoredData(tblInfo, pkIdx)) {
			rsData = getRestoreData(tblInfo, idx.Meta(), pkIdx, handleRestoreData)
		}
		iter := idx.GenIndexKVIter(errCtx, loc, indexedValues, actualHandle, rsData)
		rawKeys, err := tables.EncodeRawIndexKeyValues(loc, tblInfo, idx.Meta(), indexedValues)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		idxInfo := idx.Meta()
		physicalID := physicalTbl.GetPhysicalID()
		rawKeyPos := 0
		for iter.Valid() {
			key, value, _, err := iter.Next(nil, nil)
			if err != nil {
				return nil, 0, errors.Trace(err)
			}
			var rawKey []byte
			if rawKeyPos < len(rawKeys) {
				rawKey = rawKeys[rawKeyPos]
			}
			rawKeyPos++
			kvs = append(kvs, sampledIndexKV{
				key:           slices.Clone(key),
				value:         slices.Clone(value),
				rawKey:        slices.Clone(rawKey),
				physicalID:    physicalID,
				indexID:       idxInfo.ID,
				isGlobalIndex: idxInfo.Global,
			})
			totalBytes += int64(len(key) + len(value))
		}
	}
	return kvs, totalBytes, nil
}

type sampleTiKVIndexPredictionBytes struct {
	PredictedBytes    int64
	MVCCOverheadBytes int64
	Err               error
}

func estimateBlockSampledIndexKVPredictionBytes(
	kvs []sampledIndexKV,
	scopeSampleCounts map[sampledIndexKVScope]int,
	commitTS uint64,
) sampleTiKVIndexPredictionBytes {
	return estimateSortedBlockSampledIndexKVPredictionBytes(sortSampledIndexKVs(kvs), scopeSampleCounts, commitTS)
}

func estimateSortedBlockSampledIndexKVPredictionBytes(
	sortedKVs []sampledIndexKV,
	scopeSampleCounts map[sampledIndexKVScope]int,
	commitTS uint64,
) sampleTiKVIndexPredictionBytes {
	result := sampleTiKVIndexPredictionBytes{}
	if len(sortedKVs) == 0 {
		return result
	}
	if commitTS == 0 {
		commitTS = tikvMVCCPredictionFallbackTS
	}
	var totalLogicalBytes int64
	groupedKVs := make(map[sampledIndexKVScope][]sampledIndexKV)
	for _, kvPair := range sortedKVs {
		totalLogicalBytes += int64(len(kvPair.key) + len(kvPair.value))
		scope := kvPair.scope()
		groupedKVs[scope] = append(groupedKVs[scope], kvPair)
	}
	var logicalPhysicalBytes int64
	var mvccPhysicalBytes int64
	for scope, scopeKVs := range groupedKVs {
		splitCount := max(scopeSampleCounts[scope], 1)
		scopeLogicalPhysicalBytes, logicalErr := estimateSortedSampledIndexKVPhysicalBytesWithSplit(scopeKVs, splitCount)
		if logicalErr != nil || scopeLogicalPhysicalBytes <= 0 {
			scopeLogicalPhysicalBytes = sampledIndexKVLogicalBytes(scopeKVs)
		}
		logicalPhysicalBytes += scopeLogicalPhysicalBytes
		scopeMVCCPhysicalBytes, mvccErr := estimateBlockSampledIndexKVMVCCPhysicalBytesWithSplit(scopeKVs, splitCount, commitTS)
		if mvccErr != nil {
			result.PredictedBytes = totalLogicalBytes
			result.Err = mvccErr
			return result
		}
		mvccPhysicalBytes += scopeMVCCPhysicalBytes
	}
	if logicalPhysicalBytes <= 0 {
		logicalPhysicalBytes = totalLogicalBytes
	}
	result.PredictedBytes = mvccPhysicalBytes
	if mvccPhysicalBytes > logicalPhysicalBytes {
		result.MVCCOverheadBytes = mvccPhysicalBytes - logicalPhysicalBytes
	}
	return result
}

func sampledIndexKVLogicalBytes(kvs []sampledIndexKV) int64 {
	var totalBytes int64
	for _, kvPair := range kvs {
		totalBytes += int64(len(kvPair.key) + len(kvPair.value))
	}
	return totalBytes
}

func sortSampledIndexKVs(kvs []sampledIndexKV) []sampledIndexKV {
	sortedKVs := slices.Clone(kvs)
	slices.SortFunc(sortedKVs, func(a, b sampledIndexKV) int {
		if c := bytes.Compare(a.key, b.key); c != 0 {
			return c
		}
		return bytes.Compare(a.value, b.value)
	})
	return sortedKVs
}

func estimateSortedSampledIndexKVEncodedKeySharedPrefixAvg(sortedKVs []sampledIndexKV) float64 {
	if len(sortedKVs) < 2 {
		return 0
	}
	var sharedPrefixBytes int64
	for i := 1; i < len(sortedKVs); i++ {
		sharedPrefixBytes += int64(sharedPrefixLength(sortedKVs[i-1].key, sortedKVs[i].key))
	}
	return float64(sharedPrefixBytes) / float64(len(sortedKVs)-1)
}

func estimateSortedSampledIndexKVRawKeySharedPrefixAvg(sortedKVs []sampledIndexKV) float64 {
	if len(sortedKVs) < 2 {
		return 0
	}
	var sharedPrefixBytes int64
	for i := 1; i < len(sortedKVs); i++ {
		sharedPrefixBytes += int64(sharedPrefixLength(sortedKVs[i-1].rawKey, sortedKVs[i].rawKey))
	}
	return float64(sharedPrefixBytes) / float64(len(sortedKVs)-1)
}

func estimateSampledIndexKVRawKeyLengthAvg(kvs []sampledIndexKV) float64 {
	if len(kvs) == 0 {
		return 0
	}
	var totalRawKeyBytes int64
	for _, kv := range kvs {
		totalRawKeyBytes += int64(len(kv.rawKey))
	}
	return float64(totalRawKeyBytes) / float64(len(kvs))
}

func sharedPrefixLength(a, b []byte) int {
	limit := min(len(a), len(b))
	for i := range limit {
		if a[i] != b[i] {
			return i
		}
	}
	return limit
}

func estimateSortedSampledIndexKVPhysicalBytesWithSplit(sortedKVs []sampledIndexKV, splitCount int) (int64, error) {
	return estimateSampledKVsPhysicalBytesWithSplit(sortedKVs, splitCount, estimateSortedSampledIndexKVPhysicalBytes)
}

func estimateSampledKVsPhysicalBytesWithSplit(
	sortedKVs []sampledIndexKV,
	splitCount int,
	estimateChunk func([]sampledIndexKV) (int64, error),
) (int64, error) {
	if len(sortedKVs) == 0 {
		return 0, nil
	}
	if splitCount <= 1 {
		return estimateChunk(sortedKVs)
	}
	// For a single add-index target, splitting sorted index KVs evenly is equivalent
	// to splitting sampled rows evenly. Each chunk approximates one local SST.
	chunkSize := max(1, int(math.Ceil(float64(len(sortedKVs))/float64(splitCount))))
	var totalPhysicalBytes int64
	for start := 0; start < len(sortedKVs); start += chunkSize {
		end := min(start+chunkSize, len(sortedKVs))
		physicalBytes, err := estimateChunk(sortedKVs[start:end])
		if err != nil {
			return 0, err
		}
		totalPhysicalBytes += physicalBytes
	}
	return totalPhysicalBytes, nil
}

func estimateSortedSampledIndexKVPhysicalBytes(sortedKVs []sampledIndexKV) (int64, error) {
	memFS := vfs.NewMem()
	const sampleSSTName = "ddl-sample-prediction.sst"
	f, err := memFS.Create(sampleSSTName)
	if err != nil {
		return 0, errors.Trace(err)
	}
	writer := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		BlockSize: litconfig.DefaultBlockSize,
	})
	internalKey := sstable.InternalKey{
		Trailer: uint64(sstable.InternalKeyKindSet),
	}
	var lastKey []byte
	for _, kvPair := range sortedKVs {
		if lastKey != nil && bytes.Equal(kvPair.key, lastKey) {
			continue
		}
		internalKey.UserKey = kvPair.key
		if err := writer.Add(internalKey, kvPair.value); err != nil {
			_ = writer.Close()
			return 0, errors.Trace(err)
		}
		lastKey = kvPair.key
	}
	if err := writer.Close(); err != nil {
		return 0, errors.Trace(err)
	}
	meta, err := writer.Metadata()
	if err != nil {
		return 0, errors.Trace(err)
	}
	physicalBytes := int64(meta.Properties.DataSize + meta.Properties.IndexSize + meta.Properties.FilterSize)
	if physicalBytes <= 0 {
		physicalBytes = int64(meta.Size)
	}
	return physicalBytes, nil
}

type sampledTiKVMVCCKVs struct {
	defaultKVs []sampledIndexKV
	writeKVs   []sampledIndexKV
}

func estimateBlockSampledIndexKVMVCCPhysicalBytesWithSplit(sortedKVs []sampledIndexKV, splitCount int, commitTS uint64) (int64, error) {
	return estimateSampledTiKVMVCCKVsPhysicalBytesWithSplit(buildBlockSampledTiKVMVCCKVs(sortedKVs, commitTS), splitCount)
}

func estimateSampledTiKVMVCCKVsPhysicalBytesWithSplit(mvccKVs sampledTiKVMVCCKVs, splitCount int) (int64, error) {
	var physicalBytes int64
	if len(mvccKVs.defaultKVs) > 0 {
		defaultCFBytes, err := estimateSortedSampledIndexKVPhysicalBytesWithSplit(sortSampledIndexKVs(mvccKVs.defaultKVs), splitCount)
		if err != nil {
			return 0, errors.Annotate(err, "estimate default CF MVCC SST")
		}
		physicalBytes += defaultCFBytes
	}
	if len(mvccKVs.writeKVs) > 0 {
		writeCFBytes, err := estimateSortedSampledIndexKVPhysicalBytesWithSplit(sortSampledIndexKVs(mvccKVs.writeKVs), splitCount)
		if err != nil {
			return 0, errors.Annotate(err, "estimate write CF MVCC SST")
		}
		physicalBytes += writeCFBytes
	}
	return physicalBytes, nil
}

func buildBlockSampledTiKVMVCCKVs(kvs []sampledIndexKV, commitTS uint64) sampledTiKVMVCCKVs {
	mvccKVs := sampledTiKVMVCCKVs{
		writeKVs: make([]sampledIndexKV, 0, len(kvs)),
	}
	for _, kvPair := range kvs {
		includeShortValue := len(kvPair.value) <= tikvMVCCShortValueMaxBytes
		writeKV := sampledIndexKV{
			key:   lightningtikv.EncodeTxnSSTWriteCFKey(kvPair.key, commitTS),
			value: lightningtikv.EncodeTxnSSTWriteCFValue(commitTS, kvPair.value, includeShortValue),
		}
		mvccKVs.writeKVs = append(mvccKVs.writeKVs, writeKV)
		if len(kvPair.value) > tikvMVCCShortValueMaxBytes {
			defaultKV := sampledIndexKV{
				// Keep the existing default-CF approximation in this iteration.
				key:   appendTiKVMVCCTimestampForPrediction(kvPair.key),
				value: slices.Clone(kvPair.value),
			}
			mvccKVs.defaultKVs = append(mvccKVs.defaultKVs, defaultKV)
		}
	}
	return mvccKVs
}

func appendTiKVMVCCTimestampForPrediction(key []byte) []byte {
	mvccKey := make([]byte, 0, len(key)+tikvMVCCTimestampBytes)
	mvccKey = append(mvccKey, key...)
	for i := range tikvMVCCTimestampBytes {
		mvccKey = append(mvccKey, byte(0xff-i))
	}
	return mvccKey
}

func extractHandleRestoreDataFromRow(tblInfo *model.TableInfo, pkIdx *model.IndexInfo, row []types.Datum) []types.Datum {
	if !tblInfo.IsCommonHandle || tblInfo.CommonHandleVersion == 0 || pkIdx == nil {
		return nil
	}
	handleRestoreData := make([]types.Datum, 0, len(pkIdx.Columns))
	for _, pkCol := range pkIdx.Columns {
		handleRestoreData = append(handleRestoreData, *row[pkCol.Offset].Clone())
	}
	return handleRestoreData
}

func primaryIndexNeedsRestoredData(tblInfo *model.TableInfo, pkIdx *model.IndexInfo) bool {
	return pkIdx != nil && tables.NeedRestoredData(pkIdx.Columns, tblInfo.Columns)
}

func buildPredictionIndexesForPhysicalTable(physicalTbl table.PhysicalTable, idxInfos []*model.IndexInfo) ([]table.Index, error) {
	indexMap := make(map[int64]table.Index, len(physicalTbl.Indices()))
	for _, idx := range physicalTbl.Indices() {
		indexMap[idx.Meta().ID] = idx
	}
	indexes := make([]table.Index, 0, len(idxInfos))
	for _, idxInfo := range idxInfos {
		idx, ok := indexMap[idxInfo.ID]
		if !ok {
			return nil, errors.Errorf("index not found on physical table: %d", idxInfo.ID)
		}
		indexes = append(indexes, idx)
	}
	return indexes, nil
}

func predictionPhysicalTables(tbl table.Table) ([]table.PhysicalTable, error) {
	if partitionedTbl, ok := tbl.(table.PartitionedTable); ok {
		physicalTables := make([]table.PhysicalTable, 0, len(tbl.Meta().GetPartitionInfo().Definitions))
		for _, def := range tbl.Meta().GetPartitionInfo().Definitions {
			physicalTables = append(physicalTables, partitionedTbl.GetPartition(def.ID))
		}
		return physicalTables, nil
	}
	physicalTbl, ok := tbl.(table.PhysicalTable)
	if !ok {
		return nil, errors.Errorf("table %d is not a physical table", tbl.Meta().ID)
	}
	return []table.PhysicalTable{physicalTbl}, nil
}

func (w *worker) buildSamplePredictionPhysicalTables(
	tbl table.Table,
	targetIndexInfos []*model.IndexInfo,
) ([]samplePredictionPhysicalTable, int64, bool, error) {
	physicalTables, err := predictionPhysicalTables(tbl)
	if err != nil {
		return nil, 0, false, err
	}
	result := make([]samplePredictionPhysicalTable, 0, len(physicalTables))
	var totalRowCount int64
	useStats := true
	for _, physicalTbl := range physicalTables {
		statsTbl := getPhysicalTableStatsForPrediction(physicalTbl.GetPhysicalID(), tbl.Meta(), w.ddlCtx.statsHandle)
		if statsTbl == nil || statsTbl.Pseudo {
			useStats = false
		}
		rowCount := estimatePhysicalTableRowCount(statsTbl)
		if rowCount <= 0 {
			continue
		}
		tableIndexes, err := buildPredictionIndexesForPhysicalTable(physicalTbl, targetIndexInfos)
		if err != nil {
			return nil, 0, false, err
		}
		result = append(result, samplePredictionPhysicalTable{
			physicalTbl: physicalTbl,
			indexes:     tableIndexes,
			rowCount:    rowCount,
		})
		totalRowCount += rowCount
	}
	return result, totalRowCount, useStats, nil
}

func (w *worker) buildSamplePredictionRegionTasks(
	ctx context.Context,
	jobID int64,
	logLabel string,
	physicalTables []samplePredictionPhysicalTable,
	seed uint64,
) ([]samplePredictionRegionTask, int, error) {
	// For partitioned tables, precheck should still stay cheap. We first allocate the
	// fixed region-sample budget across physical tables using row-count weights, then
	// list regions only for the selected physical tables.
	selections := pickSamplePredictionPhysicalTables(physicalTables, seed)
	tasks := make([]samplePredictionRegionTask, 0, samplePredictionMaxRegionCount)
	readErrorCount := 0
	for _, selection := range selections {
		physicalID := selection.physicalTbl.GetPhysicalID()
		regions, err := listSamplePredictionRegions(ctx, w.store, physicalID)
		if err != nil {
			readErrorCount++
			logutil.DDLLogger().Warn("failed to list "+logLabel+" regions for add-index task",
				zap.Int64("jobID", jobID),
				zap.Int64("physicalID", physicalID),
				zap.Error(err))
			if readErrorCount > samplePredictionMaxReadErrors {
				return tasks, readErrorCount, dbterror.ErrIngestCheckEnvFailed.FastGenByArgs(
					fmt.Sprintf("add index %s failed after %d read errors: %v", logLabel, readErrorCount, err))
			}
			continue
		}
		selectedRegions := pickSamplePredictionRegionsWithLimit(regions, seed^uint64(physicalID), selection.regionCount)
		for _, region := range selectedRegions {
			tasks = append(tasks, samplePredictionRegionTask{
				samplePredictionPhysicalTable: selection.samplePredictionPhysicalTable,
				region:                        region,
			})
		}
	}
	return tasks, readErrorCount, nil
}

func listSamplePredictionRegions(ctx context.Context, store kv.Storage, physicalID int64) ([]samplePredictionRegion, error) {
	hStore, ok := store.(helper.Storage)
	if !ok {
		return nil, fmt.Errorf("store %T does not implement helper.Storage", store)
	}
	tableStart, tableEnd := tablecodec.GetTableHandleKeyRange(physicalID)
	regionInfos, err := listTableRegions(ctx, store, physicalID, 0)
	if err != nil {
		return nil, err
	}
	regions := make([]samplePredictionRegion, 0, 16)
	for _, regionInfo := range regionInfos {
		region, ok, err := buildSamplePredictionRegion(hStore.GetCodec(), tableStart, tableEnd, regionInfo)
		if err != nil {
			return nil, err
		}
		if ok {
			regions = append(regions, region)
		}
	}
	return regions, nil
}

func buildSamplePredictionRegion(codec tikv.Codec, tableStart, tableEnd []byte, regionInfo pdhttp.RegionInfo) (samplePredictionRegion, bool, error) {
	var region samplePredictionRegion
	start, err := hex.DecodeString(regionInfo.StartKey)
	if err != nil {
		return region, false, err
	}
	end, err := hex.DecodeString(regionInfo.EndKey)
	if err != nil {
		return region, false, err
	}
	start, end, err = codec.DecodeRegionRange(start, end)
	if err != nil {
		return region, false, err
	}
	start = maxKey(tableStart, start)
	end = minKey(tableEnd, end)
	if len(end) == 0 || bytes.Compare(start, end) >= 0 {
		return region, false, nil
	}
	region = samplePredictionRegion{
		StartKey:        kv.Key(start),
		EndKey:          kv.Key(end),
		ApproximateKeys: regionInfo.ApproximateKeys,
	}
	return region, true, nil
}

func pickSamplePredictionPhysicalTables(physicalTables []samplePredictionPhysicalTable, seed uint64) []samplePredictionPhysicalTableSelection {
	if len(physicalTables) == 0 {
		return nil
	}
	rnd := rand.New(rand.NewSource(int64(seed)))
	totalWeight := int64(0)
	for _, physicalTbl := range physicalTables {
		totalWeight += max(physicalTbl.rowCount, 1)
	}
	if totalWeight <= 0 {
		return nil
	}
	selectionCounts := make([]int, len(physicalTables))
	for range samplePredictionMaxRegionCount {
		target := rnd.Int63n(totalWeight)
		accumulated := int64(0)
		for i, physicalTbl := range physicalTables {
			accumulated += max(physicalTbl.rowCount, 1)
			if accumulated > target {
				selectionCounts[i]++
				break
			}
		}
	}
	result := make([]samplePredictionPhysicalTableSelection, 0, min(len(physicalTables), samplePredictionMaxRegionCount))
	for i, regionCount := range selectionCounts {
		if regionCount == 0 {
			continue
		}
		result = append(result, samplePredictionPhysicalTableSelection{
			samplePredictionPhysicalTable: physicalTables[i],
			regionCount:                   regionCount,
		})
	}
	return result
}

func pickSamplePredictionRegionsWithLimit(regions []samplePredictionRegion, seed uint64, limit int) []samplePredictionRegion {
	if limit <= 0 {
		return nil
	}
	if len(regions) <= limit {
		return regions
	}
	rnd := rand.New(rand.NewSource(int64(seed)))
	perm := rnd.Perm(len(regions))
	selected := make([]samplePredictionRegion, 0, limit)
	for _, idx := range perm[:limit] {
		selected = append(selected, regions[idx])
	}
	return selected
}

func blockSamplePredictionSkipRows(region samplePredictionRegion, rnd *rand.Rand) int {
	if rnd == nil {
		return 0
	}
	maxSkip := maxBlockSamplePredictionSkipRows(region)
	if maxSkip <= 0 {
		return 0
	}
	return rnd.Intn(int(maxSkip) + 1)
}

func maxBlockSamplePredictionSkipRows(region samplePredictionRegion) int64 {
	if region.ApproximateKeys <= int64(blockSamplePredictionMaxRows) {
		return 0
	}
	return min(region.ApproximateKeys-int64(blockSamplePredictionMaxRows), int64(samplePredictionMaxSkipRows))
}

func blockSamplePredictionTargetRows(sampledRows int, logicalBytes int64) int {
	if sampledRows <= 0 || logicalBytes <= 0 {
		return blockSamplePredictionMaxRows
	}
	avgLogicBytesPerRow := float64(logicalBytes) / float64(sampledRows)
	if avgLogicBytesPerRow <= 0 {
		return blockSamplePredictionMaxRows
	}
	blockRows := int(math.Ceil(float64(litconfig.DefaultBlockSize) / avgLogicBytesPerRow))
	return clampSamplePredictionRows(blockRows, blockSamplePredictionProbeRows, blockSamplePredictionMaxRows)
}

func clampSamplePredictionRows(rows, lower, upper int) int {
	if rows < lower {
		return lower
	}
	if rows > upper {
		return upper
	}
	return rows
}

func samplePredictionSeed(jobID, physicalID int64) uint64 {
	return uint64(jobID)*0x9e3779b97f4a7c15 + uint64(physicalID)*0xbf58476d1ce4e5b9
}

func maxKey(base, candidate []byte) []byte {
	if len(candidate) == 0 || bytes.Compare(base, candidate) >= 0 {
		return append([]byte(nil), base...)
	}
	return append([]byte(nil), candidate...)
}

func minKey(base, candidate []byte) []byte {
	if len(candidate) == 0 || bytes.Compare(base, candidate) <= 0 {
		return append([]byte(nil), base...)
	}
	return append([]byte(nil), candidate...)
}

func predictionTimeLocation(sctx sessionctx.Context) *time.Location {
	if sctx == nil {
		return time.UTC
	}
	sessVars := sctx.GetSessionVars() //nolint:forbidigo
	if sessVars == nil {
		return time.UTC
	}
	loc := sessVars.StmtCtx.TimeZone()
	if loc == nil {
		return time.UTC
	}
	return loc
}

func collectBackfillIndexes(tblInfo *model.TableInfo, reorgInfo *reorgInfo) ([]*model.IndexInfo, error) {
	indexes := make([]*model.IndexInfo, 0, len(reorgInfo.elements))
	for _, elem := range reorgInfo.elements {
		if elem == nil || !bytes.Equal(elem.TypeKey, meta.IndexElementKey) {
			continue
		}
		idxInfo := model.FindIndexInfoByID(tblInfo.Indices, elem.ID)
		if idxInfo == nil {
			return nil, errors.Errorf("index info not found: %d", elem.ID)
		}
		indexes = append(indexes, idxInfo)
	}
	return indexes, nil
}

func getPhysicalTableStatsForPrediction(physicalID int64, tblInfo *model.TableInfo, statsHandle *statshandle.Handle) *statistics.Table {
	if statsHandle != nil {
		return statsHandle.GetPhysicalTableStats(physicalID, tblInfo)
	}
	statsTbl := statistics.PseudoTable(tblInfo, false, false)
	statsTbl.PhysicalID = physicalID
	return statsTbl
}

func estimatePhysicalTableRowCount(statsTbl *statistics.Table) int64 {
	if statsTbl == nil {
		return 0
	}
	rowCount := statsTbl.RealtimeCount
	if rowCount <= 0 {
		rowCount = int64(statsTbl.GetAnalyzeRowCount())
	}
	return max(rowCount, 0)
}

func (w *worker) updateDistTaskRowCount(taskKey string, jobID int64) {
	taskMgr, err := storage.GetDXFSvcTaskMgr()
	if err != nil {
		logutil.DDLLogger().Warn("cannot get task manager", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	task, err := taskMgr.GetTaskByKeyWithHistory(w.workCtx, taskKey)
	if err != nil {
		logutil.DDLLogger().Warn("cannot get task", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	rowCount, err := taskMgr.GetSubtaskRowCount(w.workCtx, task.ID, proto.BackfillStepReadIndex)
	if err != nil {
		logutil.DDLLogger().Warn("cannot get subtask row count", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	w.getReorgCtx(jobID).setRowCount(rowCount)
}

func getNextPartitionInfo(reorg *reorgInfo, t table.PartitionedTable, currPhysicalTableID int64) (pid int64, startKey, endKey kv.Key, err error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return 0, nil, nil, nil
	}

	// This will be used in multiple different scenarios/ALTER TABLE:
	// ADD INDEX - no change in partitions, just use pi.Definitions (1)
	// REORGANIZE PARTITION - copy data from partitions to be dropped (2)
	// REORGANIZE PARTITION - (re)create indexes on partitions to be added (3)
	// REORGANIZE PARTITION - Update new Global indexes with data from non-touched partitions (4)
	// (i.e. pi.Definitions - pi.DroppingDefinitions)
	// MODIFY COLUMN - no change in partitions, just use pi.Definitions (5)
	if bytes.Equal(reorg.currElement.TypeKey, meta.IndexElementKey) {
		// case 1, 3 or 4
		if len(pi.AddingDefinitions) == 0 {
			// case 1
			// Simply AddIndex, without any partitions added or dropped!
			if reorg.mergingTmpIdx && currPhysicalTableID == t.Meta().ID {
				// If the current Physical id is the table id,
				// 1. All indexes are global index, the next Physical id should be the first partition id.
				// 2. Not all indexes are global index, return 0.
				allGlobal := true
				for _, element := range reorg.elements {
					if !bytes.Equal(element.TypeKey, meta.IndexElementKey) {
						allGlobal = false
						break
					}
					idxInfo := model.FindIndexInfoByID(t.Meta().Indices, element.ID)
					if !idxInfo.Global {
						allGlobal = false
						break
					}
				}
				if allGlobal {
					pid = 0
				} else {
					pid = pi.Definitions[0].ID
				}
			} else {
				pid, err = findNextPartitionID(currPhysicalTableID, pi.Definitions)
			}
		} else {
			// case 3 (or if not found AddingDefinitions; 4)
			// check if recreating Global Index (during Reorg Partition)
			pid, err = findNextPartitionID(currPhysicalTableID, pi.AddingDefinitions)
			if err != nil {
				// case 4
				// Not a partition in the AddingDefinitions, so it must be an existing
				// non-touched partition, i.e. recreating Global Index for the non-touched partitions
				pid, err = findNextNonTouchedPartitionID(currPhysicalTableID, pi)
			}
		}
	} else if len(pi.DroppingDefinitions) == 0 {
		// case 5
		pid, err = findNextPartitionID(currPhysicalTableID, pi.Definitions)
	} else {
		// case 2
		pid, err = findNextPartitionID(currPhysicalTableID, pi.DroppingDefinitions)
	}
	if err != nil {
		// Fatal error, should not run here.
		logutil.DDLLogger().Error("find next partition ID failed", zap.Reflect("table", t), zap.Error(err))
		return 0, nil, nil, errors.Trace(err)
	}
	if pid == 0 {
		// Next partition does not exist, all the job done.
		return 0, nil, nil, nil
	}

	failpoint.Inject("mockUpdateCachedSafePoint", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			ts := oracle.GoTimeToTS(time.Now())
			//nolint:forcetypeassert
			s := reorg.jobCtx.store.(tikv.Storage)
			s.UpdateTxnSafePointCache(ts, time.Now())
			time.Sleep(time.Second * 3)
		}
	})

	if reorg.mergingTmpIdx {
		elements := reorg.elements
		firstElemTempID := tablecodec.TempIndexPrefix | elements[0].ID
		lastElemTempID := tablecodec.TempIndexPrefix | elements[len(elements)-1].ID
		startKey = tablecodec.EncodeIndexSeekKey(pid, firstElemTempID, nil)
		endKey = tablecodec.EncodeIndexSeekKey(pid, lastElemTempID, []byte{255})
	} else {
		currentVer, err := getValidCurrentVersion(reorg.jobCtx.store)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
		startKey, endKey, err = getTableRange(reorg.NewJobContext(), reorg.jobCtx.store, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
	}
	return pid, startKey, endKey, nil
}

// updateReorgInfo will find the next partition according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func updateReorgInfo(sessPool *sess.Pool, t table.PartitionedTable, reorg *reorgInfo) (bool, error) {
	pid, startKey, endKey, err := getNextPartitionInfo(reorg, t, reorg.PhysicalTableID)
	if err != nil {
		return false, errors.Trace(err)
	}
	if pid == 0 {
		// Next partition does not exist, all the job done.
		return true, nil
	}
	reorg.PhysicalTableID, reorg.StartKey, reorg.EndKey = pid, startKey, endKey

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = reorg.UpdateReorgMeta(reorg.StartKey, sessPool)
	logutil.DDLLogger().Info("job update reorgInfo",
		zap.Int64("jobID", reorg.Job.ID),
		zap.Stringer("element", reorg.currElement),
		zap.Int64("partitionTableID", pid),
		zap.String("startKey", hex.EncodeToString(reorg.StartKey)),
		zap.String("endKey", hex.EncodeToString(reorg.EndKey)), zap.Error(err))
	return false, errors.Trace(err)
}

// findNextPartitionID finds the next partition ID in the PartitionDefinition array.
// Returns 0 if current partition is already the last one.
func findNextPartitionID(currentPartition int64, defs []model.PartitionDefinition) (int64, error) {
	for i, def := range defs {
		if currentPartition == def.ID {
			if i == len(defs)-1 {
				return 0, nil
			}
			return defs[i+1].ID, nil
		}
	}
	return 0, errors.Errorf("partition id not found %d", currentPartition)
}

func findNextNonTouchedPartitionID(currPartitionID int64, pi *model.PartitionInfo) (int64, error) {
	pid, err := findNextPartitionID(currPartitionID, pi.Definitions)
	if err != nil {
		return 0, err
	}
	if pid == 0 {
		return 0, nil
	}
	for _, notFoundErr := findNextPartitionID(pid, pi.DroppingDefinitions); notFoundErr == nil; {
		// This can be optimized, but it is not frequently called, so keeping as-is
		pid, err = findNextPartitionID(pid, pi.Definitions)
		if pid == 0 {
			break
		}
	}
	return pid, err
}

// AllocateIndexID allocates an index ID from TableInfo.
func AllocateIndexID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxIndexID++
	return tblInfo.MaxIndexID
}

func getIndexInfoByNameAndColumn(oldTableInfo *model.TableInfo, newOne *model.IndexInfo) *model.IndexInfo {
	for _, oldOne := range oldTableInfo.Indices {
		if newOne.Name.L == oldOne.Name.L && indexColumnSliceEqual(newOne.Columns, oldOne.Columns) {
			return oldOne
		}
	}
	return nil
}

func indexColumnSliceEqual(a, b []*model.IndexColumn) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		logutil.DDLLogger().Warn("admin repair table : index's columns length equal to 0")
		return true
	}
	// Accelerate the compare by eliminate index bound check.
	b = b[:len(a)]
	for i, v := range a {
		if v.Name.L != b[i].Name.L {
			return false
		}
	}
	return true
}

type cleanUpIndexWorker struct {
	baseIndexWorker
}

func newCleanUpIndexWorker(id int, t table.PhysicalTable, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jc *ReorgContext) (*cleanUpIndexWorker, error) {
	bCtx, err := newBackfillCtx(id, reorgInfo, reorgInfo.SchemaName, t, jc, metrics.LblCleanupIdxRate, false)
	if err != nil {
		return nil, err
	}

	indexes := make([]table.Index, 0, len(t.Indices()))
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	for _, index := range t.Indices() {
		if index.Meta().IsColumnarIndex() {
			continue
		}
		if index.Meta().Global {
			indexes = append(indexes, index)
		}
	}
	return &cleanUpIndexWorker{
		baseIndexWorker: baseIndexWorker{
			backfillCtx: bCtx,
			indexes:     indexes,
			rowDecoder:  rowDecoder,
			defaultVals: make([]types.Datum, len(t.WritableCols())),
			rowMap:      make(map[int64]types.Datum, len(decodeColMap)),
		},
	}, nil
}

func (w *cleanUpIndexWorker) BackfillData(_ context.Context, handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			panic("panic test")
		}
	})

	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		updateTxnEntrySizeLimitIfNeeded(txn)
		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(handleRange.getJobID()); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}
		txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

		idxRecords, nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		txn.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)

		lockCtx := new(kv.LockCtx)
		evalCtx := w.exprCtx.GetEvalCtx()
		loc, ec := evalCtx.Location(), evalCtx.ErrCtx()
		n := len(w.indexes)
		globalIndexKeys := make([]kv.Key, 0, len(idxRecords))
		allKeys := make([]kv.Key, 0, len(idxRecords))
		for i, idxRecord := range idxRecords {
			key, distinct, err := w.indexes[i%n].GenIndexKey(ec, loc, idxRecord.vals, idxRecord.handle, nil)
			if err != nil {
				return errors.Trace(err)
			}
			if distinct {
				globalIndexKeys = append(globalIndexKeys, key)
			}
			allKeys = append(allKeys, key)
		}

		var found map[string][]byte
		found, err = kv.BatchGetValue(ctx, txn, globalIndexKeys)
		if err != nil {
			return errors.Trace(err)
		}

		for i, idxRecord := range idxRecords {
			taskCtx.scanCount++
			if val, ok := found[string(allKeys[i])]; ok {
				// Only delete if it is from the partition it was read from.
				handle, errPart := tablecodec.DecodeHandleInIndexValue(val)
				if errPart != nil {
					return errors.Trace(errPart)
				}
				if partHandle, ok := handle.(kv.PartitionHandle); ok {
					if partHandle.PartitionID != handleRange.physicalTable.GetPhysicalID() {
						continue
					}
				}
				// Lock the global index entry, to prevent deleting something that is concurrently added.
				err = txn.LockKeys(ctx, lockCtx, allKeys[i])
				if err != nil {
					return errors.Trace(err)
				}
			}
			// we fetch records row by row, so records will belong to
			// index[0], index[1] ... index[n-1], index[0], index[1] ...
			// respectively. So indexes[i%n] is the index of idxRecords[i].
			err = w.indexes[i%n].Delete(w.tblCtx, txn, idxRecord.vals, idxRecord.handle)
			if err != nil {
				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "cleanUpIndexBackfillDataInTxn", 3000)
	failpoint.Inject("mockDMLExecution", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && MockDMLExecution != nil {
			MockDMLExecution()
		}
	})

	return
}

// cleanupPhysicalTableIndex handles the drop partition reorganization state for a non-partitioned table or a partition.
func (w *worker) cleanupPhysicalTableIndex(t table.PhysicalTable, reorgInfo *reorgInfo) error {
	logutil.DDLLogger().Info("start to clean up index", zap.Stringer("job", reorgInfo.Job), zap.Stringer("reorgInfo", reorgInfo))
	return w.writePhysicalTableRecord(w.workCtx, w.sessPool, t, typeCleanUpIndexWorker, reorgInfo)
}

// cleanupGlobalIndex handles the drop partition reorganization state to clean up index entries of partitions.
func (w *worker) cleanupGlobalIndexes(tbl table.PartitionedTable, partitionIDs []int64, reorgInfo *reorgInfo) error {
	var err error
	var finish bool
	for !finish {
		p := tbl.GetPartition(reorgInfo.PhysicalTableID)
		if p == nil {
			return dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, tbl.Meta().ID)
		}
		err = w.cleanupPhysicalTableIndex(p, reorgInfo)
		if err != nil {
			break
		}
		finish, err = w.updateReorgInfoForPartitions(tbl, reorgInfo, partitionIDs)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return errors.Trace(err)
}

// updateReorgInfoForPartitions will find the next partition in partitionIDs according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func (w *worker) updateReorgInfoForPartitions(t table.PartitionedTable, reorg *reorgInfo, partitionIDs []int64) (bool, error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return true, nil
	}

	var pid int64
	for i, pi := range partitionIDs {
		if pi == reorg.PhysicalTableID {
			if i == len(partitionIDs)-1 {
				return true, nil
			}
			pid = partitionIDs[i+1]
			break
		}
	}

	currentVer, err := getValidCurrentVersion(reorg.jobCtx.store)
	if err != nil {
		return false, errors.Trace(err)
	}
	start, end, err := getTableRange(reorg.NewJobContext(), reorg.jobCtx.store, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
	if err != nil {
		return false, errors.Trace(err)
	}
	reorg.StartKey, reorg.EndKey, reorg.PhysicalTableID = start, end, pid

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = reorg.UpdateReorgMeta(reorg.StartKey, w.sessPool)
	logutil.DDLLogger().Info("job update reorg info", zap.Int64("jobID", reorg.Job.ID),
		zap.Stringer("element", reorg.currElement),
		zap.Int64("partition table ID", pid), zap.String("start key", hex.EncodeToString(start)),
		zap.String("end key", hex.EncodeToString(end)), zap.Error(err))
	return false, errors.Trace(err)
}

// changingIndex is used to store the index that need to be changed during modifying column.
type changingIndex struct {
	IndexInfo *model.IndexInfo
	// Column offset in idxInfo.Columns.
	Offset int
	// When the modifying column is contained in the index, a temp index is created.
	// isTemp indicates whether the indexInfo is a temp index created by a previous modify column job.
	isTemp bool
}

// FindRelatedIndexesToChange finds the indexes that covering the given column.
// The normal one will be overwritten by the temp one.
func FindRelatedIndexesToChange(tblInfo *model.TableInfo, colName ast.CIStr) []changingIndex {
	// In multi-schema change jobs that contains several "modify column" sub-jobs, there may be temp indexes for another temp index.
	// To prevent reorganizing too many indexes, we should create the temp indexes that are really necessary.
	var normalIdxInfos, tempIdxInfos []changingIndex
	for _, idxInfo := range tblInfo.Indices {
		if pos := findIdxCol(idxInfo, colName); pos != -1 {
			isTemp := isTempIndex(idxInfo, tblInfo)
			r := changingIndex{IndexInfo: idxInfo, Offset: pos, isTemp: isTemp}
			if isTemp {
				tempIdxInfos = append(tempIdxInfos, r)
			} else {
				normalIdxInfos = append(normalIdxInfos, r)
			}
		}
	}
	// Overwrite if the index has the corresponding temp index. For example,
	// we try to find the indexes that contain the column `b` and there are two indexes, `i(a, b)` and `$i($a, b)`.
	// Note that the symbol `$` means temporary. The index `$i($a, b)` is temporarily created by the previous "modify a" statement.
	// In this case, we would create a temporary index like $$i($a, $b), so the latter should be chosen.
	result := normalIdxInfos
	for _, tmpIdx := range tempIdxInfos {
		origName := tmpIdx.IndexInfo.GetChangingOriginName()
		for i, normIdx := range normalIdxInfos {
			if normIdx.IndexInfo.Name.O == origName {
				result[i] = tmpIdx
			}
		}
	}
	return result
}

// isColumnarIndexColumn checks if any index contains the given column is a columnar index.
func isColumnarIndexColumn(tblInfo *model.TableInfo, col *model.ColumnInfo) bool {
	indexesToChange := FindRelatedIndexesToChange(tblInfo, col.Name)
	for _, idx := range indexesToChange {
		if idx.IndexInfo.IsColumnarIndex() {
			return true
		}
	}
	return false
}

// isTempIndex checks whether the index is a temp index created by modify column.
// There are two types of temp index:
// 1. The index contains a temp column that is newly added, indicated by ChangeStateInfo
// 2. The index contains a old column changing its type in place, indicated by UsingChangingType
func isTempIndex(idxInfo *model.IndexInfo, tblInfo *model.TableInfo) bool {
	for _, idxCol := range idxInfo.Columns {
		if idxCol.UseChangingType || tblInfo.Columns[idxCol.Offset].ChangeStateInfo != nil {
			return true
		}
	}
	return false
}

func findIdxCol(idxInfo *model.IndexInfo, colName ast.CIStr) int {
	for offset, idxCol := range idxInfo.Columns {
		if idxCol.Name.L == colName.L {
			return offset
		}
	}
	return -1
}

func renameIndexes(tblInfo *model.TableInfo, from, to ast.CIStr) {
	for _, idx := range tblInfo.Indices {
		if idx.Name.L == from.L {
			idx.Name = to
		} else if isTempIndex(idx, tblInfo) &&
			(idx.GetChangingOriginName() == from.O ||
				idx.GetRemovingOriginName() == from.O) {
			idx.Name.L = strings.Replace(idx.Name.L, from.L, to.L, 1)
			idx.Name.O = strings.Replace(idx.Name.O, from.O, to.O, 1)
		}
		for _, col := range idx.Columns {
			originalCol := tblInfo.Columns[col.Offset]
			if originalCol.Hidden && getExpressionIndexOriginName(col.Name) == from.O {
				col.Name.L = strings.Replace(col.Name.L, from.L, to.L, 1)
				col.Name.O = strings.Replace(col.Name.O, from.O, to.O, 1)
			}
		}
	}
}

func renameHiddenColumns(tblInfo *model.TableInfo, from, to ast.CIStr) {
	for _, col := range tblInfo.Columns {
		if col.Hidden && getExpressionIndexOriginName(col.Name) == from.O {
			col.Name.L = strings.Replace(col.Name.L, from.L, to.L, 1)
			col.Name.O = strings.Replace(col.Name.O, from.O, to.O, 1)
		}
	}
}

// CheckAndBuildIndexConditionString validates whether the given expression is compatible with
// the table schema and returns a string representation of the expression.
func CheckAndBuildIndexConditionString(tblInfo *model.TableInfo, indexConditionExpr ast.ExprNode) (string, error) {
	if indexConditionExpr == nil {
		return "", nil
	}

	// Be careful, in `CREATE TABLE` statement, the `tblInfo.Partition` is always nil here. We have to
	// check it in `buildTablePartitionInfo` again.
	if tblInfo.Partition != nil {
		return "", dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
			"partial index on partitioned table is not supported")
	}

	// check partial index condition expression
	err := checkIndexCondition(tblInfo, indexConditionExpr)
	if err != nil {
		return "", errors.Trace(err)
	}

	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation | format.RestoreWithoutSchemaName | format.RestoreWithoutTableName
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
	sb.Reset()
	err = indexConditionExpr.Restore(restoreCtx)
	if err != nil {
		return "", errors.Trace(err)
	}

	return sb.String(), nil
}

func checkIndexCondition(tblInfo *model.TableInfo, indexCondition ast.ExprNode) error {
	// Only the following expressions are supported:
	// 1. column IS NULL
	// 2. column IS NOT NULL
	// 3. column = / != / > / < / >= / <= const
	// The column must be a visible column in the table, and the const must be a literal value with
	// the same type as the column.
	// The column must **NOT** be a generated column. We can loosen this restriction in the future.
	//
	// TODO: support more expressions in the future.
	if indexCondition == nil {
		return nil
	}

	switch cond := indexCondition.(type) {
	case *ast.IsNullExpr:
		// `IS NULL` and `IS NOT NULL` are both in this branch.
		columnName, ok := cond.Expr.(*ast.ColumnNameExpr)
		if !ok {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				"partial index condition must include a column name in the IS NULL expression")
		}
		columnInfo := model.FindColumnInfo(tblInfo.Columns, columnName.Name.Name.L)
		if columnInfo == nil {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("column name %s referenced in partial index condition is not found in table",
					columnName.Name.Name.L))
		}
		if columnInfo.IsGenerated() {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("generated column %s cannot be used in partial index condition", columnName.Name.Name.L))
		}

		return nil
	case *ast.BinaryOperationExpr:
		if cond.Op != opcode.EQ && cond.Op != opcode.NE && cond.Op != opcode.GT &&
			cond.Op != opcode.LT && cond.Op != opcode.GE && cond.Op != opcode.LE {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("binary operation %s is not supported", cond.Op.String()))
		}

		var columnName *ast.ColumnNameExpr
		var anotherSide ast.ExprNode
		columnName, ok := cond.L.(*ast.ColumnNameExpr)
		if !ok {
			// maybe the right side is a column name
			columnName, ok = cond.R.(*ast.ColumnNameExpr)
			if !ok {
				return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
					"partial index condition must include a column name in the binary operation")
			}

			anotherSide = cond.L
		} else {
			anotherSide = cond.R
		}
		columnInfo := model.FindColumnInfo(tblInfo.Columns, columnName.Name.Name.L)
		if columnInfo == nil {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("column name `%s` referenced in partial index condition is not found in table",
					columnName.Name.Name.L))
		}
		if columnInfo.IsGenerated() {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("generated column %s cannot be used in partial index condition", columnName.Name.Name.L))
		}

		// The another side must be a literal value, and it must have the same type as the column.
		constantExpr, ok := anotherSide.(ast.ValueExpr)
		if !ok {
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				"partial index condition must include a literal value on the other side of the binary operation")
		}
		// Reference `types.DefaultTypeForValue`, they are all possible types for literal values.
		// However, this switch-case still includes more types than the ones we have in that function
		// to avoid breaking in the future.
		//
		// Accept tiny type conversion as the type of the literal value is too limited. We shouldn't
		// force the user to use such a limited range of types.
		//
		// It'll allow precision / length difference in most of the cases.
		switch constantExpr.GetType().GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong,
			mysql.TypeInt24, mysql.TypeBit, mysql.TypeYear:
			// the target column must be an integer type or enum or set
			if columnInfo.GetType() != mysql.TypeTiny &&
				columnInfo.GetType() != mysql.TypeShort &&
				columnInfo.GetType() != mysql.TypeLong &&
				columnInfo.GetType() != mysql.TypeLonglong &&
				columnInfo.GetType() != mysql.TypeInt24 &&
				columnInfo.GetType() != mysql.TypeBit &&
				columnInfo.GetType() != mysql.TypeYear &&
				columnInfo.GetType() != mysql.TypeEnum &&
				columnInfo.GetType() != mysql.TypeSet {
				return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
					fmt.Sprintf("the type %s of the column `%s` in partial index condition is not compatible with the literal value type %s",
						columnInfo.FieldType.String(), columnName.Name.Name.L, constantExpr.GetType().String()))
			}
			return nil
		case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
			// the target column must be either a float or double type
			// TODO: consider whether need to support decimal type in this branch
			if columnInfo.GetType() != mysql.TypeFloat &&
				columnInfo.GetType() != mysql.TypeDouble &&
				columnInfo.GetType() != mysql.TypeNewDecimal {
				return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
					fmt.Sprintf("the type %s of the column `%s` in partial index condition is not compatible with the literal value type %s",
						columnInfo.FieldType.String(), columnName.Name.Name.L, constantExpr.GetType().String()))
			}
			return nil
		case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
			mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			if types.IsString(columnInfo.GetType()) {
				// check the collation of the column and the literal value
				if columnInfo.FieldType.GetCharset() != constantExpr.GetType().GetCharset() {
					return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
						fmt.Sprintf("the charset %s of the column `%s` in partial index condition is not compatible with the literal value charset %s",
							columnInfo.FieldType.GetCharset(), columnName.Name.Name.L, constantExpr.GetType().GetCharset()))
				}

				return nil
			}

			// Allow to compare a datetime type column with a string literal, because we don't have a datetime literal.
			// This branch will allow users to use datetime columns in index condition.
			if columnInfo.GetType() == mysql.TypeTimestamp ||
				columnInfo.GetType() == mysql.TypeDate ||
				columnInfo.GetType() == mysql.TypeDuration ||
				columnInfo.GetType() == mysql.TypeNewDate ||
				columnInfo.GetType() == mysql.TypeDatetime {
				return nil
			}

			// ENUM and SET are also allowed for string literal.
			if columnInfo.GetType() == mysql.TypeEnum || columnInfo.GetType() == mysql.TypeSet {
				return nil
			}

			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("the type %s of the column `%s` in partial index condition is not compatible with the literal value type %s",
					columnInfo.FieldType.String(), columnName.Name.Name.L, constantExpr.GetType().String()))
		case mysql.TypeNull:
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				"= NULL is not supported in partial index condition because it is always false")
		case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDuration, mysql.TypeNewDate,
			mysql.TypeDatetime, mysql.TypeJSON, mysql.TypeEnum, mysql.TypeSet:
			// The `DATE '2025-07-28'` is actually a `cast` function, so they are also not supported yet.
			intest.Assert(false, "should never generate literal values of these types")

			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("the type %s of the literal value in partial index condition is not supported",
					constantExpr.GetType().String()))
		default:
			return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
				fmt.Sprintf("the type %s of the literal value in partial index condition is not supported",
					constantExpr.GetType().String()))
		}
	default:
		return dbterror.ErrUnsupportedAddPartialIndex.GenWithStackByArgs(
			"the kind of partial index condition is not supported")
	}
}

func buildAffectColumn(idxInfo *model.IndexInfo, tblInfo *model.TableInfo) ([]*model.IndexColumn, error) {
	ectx := exprstatic.NewExprContext()

	// Build affect column for partial index.
	if idxInfo.HasCondition() {
		cols, err := tables.ExtractColumnsFromCondition(ectx, idxInfo, tblInfo, true)
		if err != nil {
			return nil, err
		}
		return tables.DedupIndexColumns(cols), nil
	}

	return nil, nil
}

// buildIndexConditionChecker builds an expression for evaluating the index condition based on
// the given columns.
func buildIndexConditionChecker(copCtx copr.CopContext, tblInfo *model.TableInfo, idxInfo *model.IndexInfo) (func(row chunk.Row) (bool, error), error) {
	schema, names := copCtx.GetBase().GetSchemaAndNames()

	exprCtx := copCtx.GetBase().ExprCtx
	expr, err := expression.ParseSimpleExpr(exprCtx, idxInfo.ConditionExprString, expression.WithInputSchemaAndNames(schema, names, tblInfo))
	if err != nil {
		return nil, err
	}

	return func(row chunk.Row) (bool, error) {
		datum, isNull, err := expr.EvalInt(exprCtx.GetEvalCtx(), row)
		if err != nil {
			return false, err
		}
		// If the result is NULL, it usually means the original column itself is NULL.
		// In this case, we should refuse to consider the index for partial index condition.
		return datum > 0 && !isNull, nil
	}, nil
}
