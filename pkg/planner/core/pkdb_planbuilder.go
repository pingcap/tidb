package core

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/generatedexpr"
)

// CreateLogReplication represents a CREATE LOG REPLICATION plan.
type CreateLogReplication struct {
	baseSchemaProducer

	Name              pmodel.CIStr
	Host              string
	Port              int
	User              string
	Password          string
	ProtectionMode    ast.ProtectionMode
	DegradeTimeoutSec uint64
	Detached          bool
	// LearnerCount is zero when CREATE should use the mode-independent default 1.
	LearnerCount uint32
}

// AlterLogReplication represents an ALTER LOG REPLICATION plan.
type AlterLogReplication struct {
	baseSchemaProducer

	Name pmodel.CIStr
	// NewSourceClusterID can't be set together with other options.
	NewSourceClusterID uint64
	ProtectionMode     ast.ProtectionMode
	DegradeTimeoutSec  uint64
	NewName            pmodel.CIStr
	// LearnerCount is zero when ALTER should preserve the current target in any mode.
	LearnerCount uint32
	// ProtectionModeSet distinguishes an omitted mode from explicit mode zero.
	ProtectionModeSet bool
}

// PauseLogReplication represents a PAUSE LOG REPLICATION plan.
type PauseLogReplication struct {
	baseSchemaProducer

	Name pmodel.CIStr
}

// ResumeLogReplication represents a RESUME LOG REPLICATION plan.
type ResumeLogReplication struct {
	baseSchemaProducer

	Name pmodel.CIStr
}

// DropLogReplication represents a DROP LOG REPLICATION plan.
type DropLogReplication struct {
	baseSchemaProducer

	Name pmodel.CIStr
}

// SwitchOverPrimary represents an ADMIN SWITCHOVER PRIMARY plan.
type SwitchOverPrimary struct {
	baseSchemaProducer

	NewPrimaryClusterID uint64
}

// SwitchOverAsPrimary represents an ADMIN SWITCHOVER AS PRIMARY plan.
type SwitchOverAsPrimary struct {
	baseSchemaProducer
}

// ActivateStandby represents an ADMIN ACTIVATE STANDBY plan.
type ActivateStandby struct {
	baseSchemaProducer

	Mode  ast.ActivateStandbyMode
	Force bool
}

// CancelLogReplicationWorkflow represents an ADMIN CANCEL LOG REPLICATION WORKFLOW plan.
type CancelLogReplicationWorkflow struct {
	baseSchemaProducer

	WorkflowID uint64
}

func (b *PlanBuilder) pkdbBuildAdmin(ctx context.Context, as *ast.AdminStmt) (base.Plan, error) {
	switch as.Tp {
	case ast.AdminCreateLogReplication:
		return b.buildAdminCreateLogReplication(ctx, as.CreateLogReplication)
	case ast.AdminAlterLogReplication:
		return b.buildAdminAlterLogReplication(ctx, as.AlterLogReplication)
	case ast.AdminPauseLogReplication:
		return b.buildAdminPauseLogReplication(ctx, as.PauseLogReplication)
	case ast.AdminResumeLogReplication:
		return b.buildAdminResumeLogReplication(ctx, as.ResumeLogReplication)
	case ast.AdminDropLogReplication:
		return b.buildAdminDropLogReplication(ctx, as.DropLogReplication)
	case ast.AdminSwitchOverPrimary:
		return b.buildAdminSwitchOverPrimary(ctx, as.SwitchOverPrimary)
	case ast.AdminSwitchOverAsPrimary:
		return b.buildAdminSwitchOverAsPrimary(ctx, as.SwitchOverAsPrimary)
	case ast.AdminActivateStandby:
		return b.buildAdminActivateStandby(ctx, as.ActivateStandby)
	case ast.AdminCancelLogReplicationWorkflow:
		return b.buildAdminCancelLogReplicationWorkflow(ctx, as.CancelLogReplicationWorkflow)
	default:
		return nil, plannererrors.ErrUnsupportedType.GenWithStack("Unsupported ast.AdminStmt(%T) for buildAdmin", as)
	}
}

func getLogReplOptsMap(opts []*ast.LogReplicationOpt) (map[ast.LogReplicationOptType]*ast.LogReplicationOpt, error) {
	optsMap := make(map[ast.LogReplicationOptType]*ast.LogReplicationOpt, len(opts))
	for _, opt := range opts {
		if _, ok := optsMap[opt.Tp]; ok {
			return nil, errors.Errorf("duplicate option type: %v", opt.Tp)
		}
		optsMap[opt.Tp] = opt
	}
	return optsMap, nil
}

// parseLogReplicationLearnerCount validates before narrowing the SQL uint64
// literal to the uint32 protocol field. Zero represents omission after validation.
func parseLogReplicationLearnerCount(opts map[ast.LogReplicationOptType]*ast.LogReplicationOpt) (uint32, error) {
	opt, ok := opts[ast.LogReplicationOptLearnerCount]
	if !ok {
		return 0, nil
	}
	if opt.UintValue == 0 || opt.UintValue > math.MaxUint32 {
		return 0, errors.Errorf("LEARNER_COUNT must be between 1 and %d, got %d", uint64(math.MaxUint32), opt.UintValue)
	}
	count := uint32(opt.UintValue)
	return count, nil
}

func validateProtectionMode(mode ast.ProtectionMode) error {
	switch mode {
	case ast.ProtectionModeMaximumPerformance, ast.ProtectionModeMaximumProtection,
		ast.ProtectionModeMaximumAvailability:
		return nil
	default:
		return errors.Errorf("invalid PROTECTION_MODE value: %d, expected one of 0, 1, 2", mode)
	}
}

func (*PlanBuilder) buildAdminCreateLogReplication(_ context.Context, as *ast.CreateLogReplication) (base.Plan, error) {
	optsMap, err := getLogReplOptsMap(as.Opts)
	if err != nil {
		return nil, err
	}
	p := &CreateLogReplication{
		Name: as.Name,
	}
	p.LearnerCount, err = parseLogReplicationLearnerCount(optsMap)
	if err != nil {
		return nil, err
	}
	if opt, ok := optsMap[ast.LogReplicationOptSourceHost]; ok {
		p.Host = opt.Value
	}
	if opt, ok := optsMap[ast.LogReplicationOptSourcePort]; ok {
		p.Port = int(opt.UintValue)
	}
	if opt, ok := optsMap[ast.LogReplicationOptSourceUser]; ok {
		p.User = opt.Value
	}
	if opt, ok := optsMap[ast.LogReplicationOptSourcePassword]; ok {
		p.Password = opt.Value
	}
	if opt, ok := optsMap[ast.LogReplicationOptProtectionMode]; ok {
		p.ProtectionMode = ast.ProtectionMode(opt.UintValue)
		if err := validateProtectionMode(p.ProtectionMode); err != nil {
			return nil, err
		}
	}
	if err := parseDegradeTimeoutOption(&p.DegradeTimeoutSec, p.ProtectionMode, optsMap); err != nil {
		return nil, err
	}
	if _, ok := optsMap[ast.LogReplicationOptDetached]; ok {
		p.Detached = true
		p.setSchemaAndNames(buildCreateLogReplicationDetachedFields())
	}
	return p, nil
}

func buildCreateLogReplicationDetachedFields() (*expression.Schema, types.NameSlice) {
	schema := newColumnsWithNames(1)
	schema.Append(buildColumnWithName("", "WORKFLOW_ID", mysql.TypeLonglong, 20))
	return schema.col2Schema(), schema.names
}

func (*PlanBuilder) buildAdminAlterLogReplication(_ context.Context, as *ast.AlterLogReplication) (base.Plan, error) {
	if as.NewName.L != "" {
		if as.NewSourceClusterID != nil || len(as.Opts) != 0 {
			return nil, errors.New("ALTER LOG REPLICATION RENAME cannot be combined with other alter options")
		}
		return &AlterLogReplication{
			Name:    as.Name,
			NewName: as.NewName,
		}, nil
	}
	if as.NewSourceClusterID != nil {
		if len(as.Opts) != 0 {
			return nil, errors.New("ALTER LOG REPLICATION CHANGE SOURCE cannot be combined with option list")
		}
		p := &AlterLogReplication{
			Name:               as.Name,
			NewSourceClusterID: *as.NewSourceClusterID,
		}
		return p, nil
	}
	optsMap, err := getLogReplOptsMap(as.Opts)
	if err != nil {
		return nil, err
	}
	p := &AlterLogReplication{
		Name: as.Name,
	}
	p.LearnerCount, err = parseLogReplicationLearnerCount(optsMap)
	if err != nil {
		return nil, err
	}
	if opt, ok := optsMap[ast.LogReplicationOptProtectionMode]; ok {
		p.ProtectionModeSet = true
		p.ProtectionMode = ast.ProtectionMode(opt.UintValue)
		if err := validateProtectionMode(p.ProtectionMode); err != nil {
			return nil, err
		}
	}
	if err := parseDegradeTimeoutOption(&p.DegradeTimeoutSec, p.ProtectionMode, optsMap); err != nil {
		return nil, err
	}
	delete(optsMap, ast.LogReplicationOptProtectionMode)
	delete(optsMap, ast.LogReplicationOptDegradeTimeout)
	delete(optsMap, ast.LogReplicationOptLearnerCount)
	for tp := range optsMap {
		return nil, errors.Errorf("%v is not supported in ALTER LOG REPLICATION", tp)
	}

	return p, nil
}

func (*PlanBuilder) buildAdminPauseLogReplication(_ context.Context, as *ast.PauseLogReplication) (base.Plan, error) {
	p := &PauseLogReplication{
		Name: as.Name,
	}
	return p, nil
}

func (*PlanBuilder) buildAdminResumeLogReplication(_ context.Context, as *ast.ResumeLogReplication) (base.Plan, error) {
	p := &ResumeLogReplication{
		Name: as.Name,
	}
	return p, nil
}

func (*PlanBuilder) buildAdminDropLogReplication(_ context.Context, as *ast.DropLogReplication) (base.Plan, error) {
	p := &DropLogReplication{
		Name: as.Name,
	}
	return p, nil
}

func (*PlanBuilder) buildAdminSwitchOverPrimary(_ context.Context, as *ast.SwitchOverPrimary) (base.Plan, error) {
	p := &SwitchOverPrimary{
		NewPrimaryClusterID: as.NewPrimaryClusterID,
	}
	return p, nil
}

func (*PlanBuilder) buildAdminSwitchOverAsPrimary(_ context.Context, _ *ast.SwitchOverAsPrimary) (base.Plan, error) {
	p := &SwitchOverAsPrimary{}
	return p, nil
}

func (*PlanBuilder) buildAdminActivateStandby(_ context.Context, a *ast.ActivateStandby) (base.Plan, error) {
	p := &ActivateStandby{Mode: a.Mode, Force: a.Force}
	return p, nil
}

func (*PlanBuilder) buildAdminCancelLogReplicationWorkflow(
	_ context.Context,
	as *ast.CancelLogReplicationWorkflow,
) (base.Plan, error) {
	p := &CancelLogReplicationWorkflow{
		WorkflowID: as.WorkflowID,
	}
	return p, nil
}

func parseDegradeTimeoutOption(planTimeoutSec *uint64, mode ast.ProtectionMode, optsMap map[ast.LogReplicationOptType]*ast.LogReplicationOpt) error {
	if opt, ok := optsMap[ast.LogReplicationOptDegradeTimeout]; ok {
		if mode != ast.ProtectionModeMaximumAvailability {
			return errors.New("DEGRADE_TIMEOUT is only allowed when PROTECTION_MODE is MAXIMUM_AVAILABILITY")
		}
		degradeTimeout, err := time.ParseDuration(opt.Value)
		if err != nil {
			return errors.Errorf("invalid DEGRADE_TIMEOUT value: %v", opt.Value)
		}
		if degradeTimeout <= 0 {
			return errors.New("DEGRADE_TIMEOUT must be greater than 0")
		}
		sec := uint64(degradeTimeout.Seconds())
		if sec == 0 {
			return errors.Errorf("the minimal value of DEGRADE_TIMEOUT is 1 second, got %v", opt.Value)
		}
		*planTimeoutSec = sec
	} else if mode == ast.ProtectionModeMaximumAvailability {
		return errors.Errorf("DEGRADE_TIMEOUT is required when PROTECTION_MODE is MAXIMUM_AVAILABILITY")
	}
	return nil
}

func buildDefaultOptionsForCTAS(logicalPlan base.LogicalPlan, idx int) ([]*ast.ColumnOption, error) {
	colInfo := findSourceColumnInfoForCTAS(logicalPlan, idx)
	if colInfo == nil {
		return nil, nil
	}

	expr, err := buildDefaultExprForCTAS(logicalPlan.SCtx().GetExprCtx(), colInfo)
	if err != nil || expr == nil {
		return nil, err
	}

	return []*ast.ColumnOption{{
		Tp:   ast.ColumnOptionDefaultValue,
		Expr: expr,
	}}, nil
}

func findSourceColumnInfoForCTAS(logicalPlan base.LogicalPlan, idx int) *model.ColumnInfo {
	if idx < 0 || idx >= logicalPlan.Schema().Len() {
		return nil
	}
	return findSourceColumnInfoInPlan(logicalPlan, logicalPlan.Schema().Columns[idx])
}

func findSourceColumnInfoInPlan(logicalPlan base.LogicalPlan, col *expression.Column) *model.ColumnInfo {
	switch p := logicalPlan.(type) {
	case *logicalop.DataSource:
		for i, dsCol := range p.Schema().Columns {
			if dsCol.UniqueID == col.UniqueID && i < len(p.Columns) {
				return p.Columns[i]
			}
		}
	case *logicalop.LogicalProjection:
		for i, projCol := range p.Schema().Columns {
			if projCol.UniqueID != col.UniqueID {
				continue
			}
			exprCol, ok := p.Exprs[i].(*expression.Column)
			if !ok || len(p.Children()) != 1 {
				return nil
			}
			return findSourceColumnInfoInPlan(p.Children()[0], exprCol)
		}
	default:
		for _, child := range p.Children() {
			for _, childCol := range child.Schema().Columns {
				if childCol.UniqueID == col.UniqueID {
					return findSourceColumnInfoInPlan(child, childCol)
				}
			}
		}
	}
	return nil
}

func buildDefaultExprForCTAS(exprCtx expression.BuildContext, colInfo *model.ColumnInfo) (ast.ExprNode, error) {
	defaultValue := colInfo.GetDefaultValue()
	if defaultValue == nil || mysql.HasNoDefaultValueFlag(colInfo.GetFlag()) {
		return nil, nil
	}

	if colInfo.DefaultIsExpr {
		defaultExpr, ok := defaultValue.(string)
		if !ok {
			return nil, errors.Errorf("default expression for column %s has unexpected type %T", colInfo.Name.O, defaultValue)
		}
		return generatedexpr.ParseExpression(defaultExpr)
	}

	if defaultStr, ok := defaultValue.(string); ok {
		switch defaultStr {
		case strings.ToUpper(ast.CurrentTimestamp):
			defaultExpr := strings.ToUpper(ast.CurrentTimestamp)
			if colInfo.GetDecimal() > 0 {
				defaultExpr = fmt.Sprintf("%s(%d)", defaultExpr, colInfo.GetDecimal())
			}
			return generatedexpr.ParseExpression(defaultExpr)
		case strings.ToUpper(ast.CurrentDate):
			return generatedexpr.ParseExpression(strings.ToUpper(ast.CurrentDate))
		}
	}

	if colInfo.GetType() == mysql.TypeTimestamp {
		datum, err := table.GetColDefaultValue(exprCtx, colInfo)
		if err != nil {
			return nil, err
		}
		return ast.NewValueExpr(datum.GetMysqlTime().String(), "", ""), nil
	}

	if colInfo.GetType() == mysql.TypeBit {
		defaultStr, ok := defaultValue.(string)
		if !ok {
			return nil, errors.Errorf("bit default for column %s has unexpected type %T", colInfo.Name.O, defaultValue)
		}
		return ast.NewValueExpr(types.BinaryLiteral(defaultStr), "", ""), nil
	}

	return ast.NewValueExpr(defaultValue, "", ""), nil
}

func (b *PlanBuilder) buildCTASSelectPlan(ctx context.Context, stmt *ast.CreateTableStmt) (base.LogicalPlan, error) {
	sctx, err := AsSctx(b.ctx)
	if err != nil {
		return nil, err
	}

	if stmt.CTASSnapshotTS == 0 {
		readTS, err := sessiontxn.GetTxnManager(sctx).GetStmtReadTS()
		if err != nil {
			return nil, err
		}
		stmt.CTASSnapshotTS = readTS
	}

	snapshotIS, err := domain.GetDomain(sctx).GetSnapshotInfoSchema(stmt.CTASSnapshotTS)
	if err != nil {
		return nil, err
	}
	snapshotIS = temptable.AttachLocalTemporaryTableInfoSchema(sctx, snapshotIS)

	sessionVars := sctx.GetSessionVars()
	origSnapshotTS := sessionVars.SnapshotTS
	origSnapshotIS := sessionVars.SnapshotInfoschema
	origIS := b.is
	sessionVars.SnapshotTS = stmt.CTASSnapshotTS
	sessionVars.SnapshotInfoschema = snapshotIS
	b.is = snapshotIS
	defer func() {
		sessionVars.SnapshotTS = origSnapshotTS
		sessionVars.SnapshotInfoschema = origSnapshotIS
		b.is = origIS
	}()

	switch selectStmt := stmt.Select.(type) {
	case *ast.SelectStmt:
		return b.buildSelect(ctx, selectStmt)
	case *ast.SetOprStmt:
		return b.buildSetOpr(ctx, selectStmt)
	default:
		return nil, errors.New("unsupported select stmt type: " + reflect.TypeOf(selectStmt).String())
	}
}
