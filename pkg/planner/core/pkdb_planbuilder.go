package core

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// CreateLogReplication represents a CREATE LOG REPLICATION plan.
type CreateLogReplication struct {
	baseSchemaProducer

	Name              model.CIStr
	Host              string
	Port              int
	User              string
	Password          string
	ProtectionMode    ast.ProtectionMode
	DegradeTimeoutSec uint64
}

// AlterLogReplication represents an ALTER LOG REPLICATION plan.
type AlterLogReplication struct {
	baseSchemaProducer

	Name model.CIStr
	// NewSourceClusterID can't be set together with other options.
	NewSourceClusterID uint64
	ProtectionMode     ast.ProtectionMode
	DegradeTimeoutSec  uint64
}

// PauseLogReplication represents a PAUSE LOG REPLICATION plan.
type PauseLogReplication struct {
	baseSchemaProducer

	Name model.CIStr
}

// ResumeLogReplication represents a RESUME LOG REPLICATION plan.
type ResumeLogReplication struct {
	baseSchemaProducer

	Name model.CIStr
}

// DropLogReplication represents a DROP LOG REPLICATION plan.
type DropLogReplication struct {
	baseSchemaProducer

	Name model.CIStr
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

	Mode ast.ActivateStandbyMode
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

func (*PlanBuilder) buildAdminCreateLogReplication(_ context.Context, as *ast.CreateLogReplication) (base.Plan, error) {
	optsMap, err := getLogReplOptsMap(as.Opts)
	if err != nil {
		return nil, err
	}
	p := &CreateLogReplication{
		Name: as.Name,
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
	}
	if err := parseDegradeTimeoutOption(&p.DegradeTimeoutSec, p.ProtectionMode, optsMap); err != nil {
		return nil, err
	}
	return p, nil
}

func (*PlanBuilder) buildAdminAlterLogReplication(_ context.Context, as *ast.AlterLogReplication) (base.Plan, error) {
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
	if opt, ok := optsMap[ast.LogReplicationOptProtectionMode]; ok {
		p.ProtectionMode = ast.ProtectionMode(opt.UintValue)
	}
	if err := parseDegradeTimeoutOption(&p.DegradeTimeoutSec, p.ProtectionMode, optsMap); err != nil {
		return nil, err
	}
	delete(optsMap, ast.LogReplicationOptProtectionMode)
	delete(optsMap, ast.LogReplicationOptDegradeTimeout)
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
	p := &ActivateStandby{Mode: a.Mode}
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
