package ast

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
)

type CreateLogReplication struct {
	Name model.CIStr
	Opts []*LogReplicationOpt
}

func (c *CreateLogReplication) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE LOG REPLICATION ")
	ctx.WriteName(c.Name.String())
	if len(c.Opts) == 0 {
		return errors.New("empty LogReplicationOptList")
	}
	for i, opt := range c.Opts {
		if opt == nil {
			return errors.Errorf("LogReplicationOptList[%d] is nil", i)
		}
		ctx.WritePlain(" ")
		if err := opt.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore CreateLogReplication.Opts[%d]", i)
		}
	}
	return nil
}

type AlterLogReplication struct {
	Name               model.CIStr
	Opts               []*LogReplicationOpt
	NewSourceClusterID *uint64
	NewName            model.CIStr
}

func (a *AlterLogReplication) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER LOG REPLICATION ")
	ctx.WriteName(a.Name.String())
	if a.NewName.L != "" {
		ctx.WriteKeyWord(" RENAME TO ")
		ctx.WriteName(a.NewName.String())
		return nil
	}
	if a.NewSourceClusterID != nil {
		ctx.WriteKeyWord(" CHANGE SOURCE TO ")
		ctx.WritePlainf("%d", *a.NewSourceClusterID)
		return nil
	}
	if len(a.Opts) == 0 {
		return errors.New("empty LogReplicationOptList")
	}
	for i, opt := range a.Opts {
		if opt == nil {
			return errors.Errorf("LogReplicationOptList[%d] is nil", i)
		}
		ctx.WritePlain(" ")
		if err := opt.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore AlterLogReplication.Opts[%d]", i)
		}
	}
	return nil
}

// SecureText implements SensitiveStmtNode for log replication admin statements with SOURCE_PASSWORD.
func (n *AdminStmt) SecureText() string {
	redactedStmt := *n
	switch n.Tp {
	case AdminCreateLogReplication:
		if n.CreateLogReplication == nil || !hasLogReplicationPasswordOpt(n.CreateLogReplication.Opts) {
			return n.Text()
		}
		redactedCreate := *n.CreateLogReplication
		redactedCreate.Opts = redactLogReplicationPasswordOpt(n.CreateLogReplication.Opts)
		redactedStmt.CreateLogReplication = &redactedCreate
	case AdminAlterLogReplication:
		if n.AlterLogReplication == nil || !hasLogReplicationPasswordOpt(n.AlterLogReplication.Opts) {
			return n.Text()
		}
		redactedAlter := *n.AlterLogReplication
		redactedAlter.Opts = redactLogReplicationPasswordOpt(n.AlterLogReplication.Opts)
		redactedStmt.AlterLogReplication = &redactedAlter
	default:
		return n.Text()
	}

	var sb strings.Builder
	_ = redactedStmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return sb.String()
}

func hasLogReplicationPasswordOpt(opts []*LogReplicationOpt) bool {
	for _, opt := range opts {
		if opt != nil && opt.Tp == LogReplicationOptSourcePassword {
			return true
		}
	}
	return false
}

func redactLogReplicationPasswordOpt(opts []*LogReplicationOpt) []*LogReplicationOpt {
	redactedOpts := make([]*LogReplicationOpt, len(opts))
	for i, opt := range opts {
		if opt == nil {
			continue
		}
		redactedOpt := *opt
		if redactedOpt.Tp == LogReplicationOptSourcePassword {
			redactedOpt.Value = "***"
		}
		redactedOpts[i] = &redactedOpt
	}
	return redactedOpts
}

type PauseLogReplication struct {
	Name model.CIStr
}

func (p *PauseLogReplication) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("PAUSE LOG REPLICATION ")
	ctx.WriteName(p.Name.String())
	return nil
}

type ResumeLogReplication struct {
	Name model.CIStr
}

func (r *ResumeLogReplication) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RESUME LOG REPLICATION ")
	ctx.WriteName(r.Name.String())
	return nil
}

type DropLogReplication struct {
	Name model.CIStr
}

func (d *DropLogReplication) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP LOG REPLICATION ")
	ctx.WriteName(d.Name.String())
	return nil
}

type SwitchOverPrimary struct {
	NewPrimaryClusterID uint64
}

func (s *SwitchOverPrimary) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SWITCHOVER PRIMARY TO ")
	ctx.WritePlainf("%d", s.NewPrimaryClusterID)
	return nil
}

// SwitchOverAsPrimary represents the AST node for ADMIN SWITCHOVER AS PRIMARY statement.
type SwitchOverAsPrimary struct{}

// Restore implements the Node interface for SwitchOverAsPrimary.
func (s *SwitchOverAsPrimary) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SWITCHOVER AS PRIMARY")
	return nil
}

type ActivateStandbyMode int

const (
	ActivateStandbyModeDefault ActivateStandbyMode = iota
	ActivateStandbyModeFlashback
	ActivateStandbyModeForceCommit
)

func (m ActivateStandbyMode) String() string {
	switch m {
	case ActivateStandbyModeDefault:
		return "DEFAULT"
	case ActivateStandbyModeFlashback:
		return "FLASHBACK"
	case ActivateStandbyModeForceCommit:
		return "FORCE_COMMIT"
	default:
		return "UNKNOWN"
	}
}

type ActivateStandby struct {
	Mode  ActivateStandbyMode
	Force bool
}

func (a *ActivateStandby) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ACTIVATE STANDBY")
	if a.Force {
		ctx.WriteKeyWord(" FORCE")
		return nil
	}
	switch a.Mode {
	case ActivateStandbyModeDefault, ActivateStandbyModeFlashback, ActivateStandbyModeForceCommit:
		ctx.WriteKeyWord(" MODE = ")
		ctx.WriteKeyWord(a.Mode.String())
		return nil
	default:
		return errors.Errorf("unknown ActivateStandbyMode: %d", a.Mode)
	}
}

type CancelLogReplicationWorkflow struct {
	WorkflowID uint64
}

func (c *CancelLogReplicationWorkflow) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CANCEL LOG REPLICATION WORKFLOW ")
	ctx.WritePlainf("%d", c.WorkflowID)
	return nil
}

type LogReplicationOptType int

const (
	LogReplicationOptSourceHost LogReplicationOptType = iota
	LogReplicationOptSourcePort
	LogReplicationOptSourceUser
	LogReplicationOptSourcePassword
	LogReplicationOptProtectionMode
	LogReplicationOptDegradeTimeout
	LogReplicationOptDetached
	// LogReplicationOptLearnerCount specifies target learners per source region.
	LogReplicationOptLearnerCount
)

func (t LogReplicationOptType) String() string {
	switch t {
	case LogReplicationOptSourceHost:
		return "SOURCE_HOST"
	case LogReplicationOptSourcePort:
		return "SOURCE_PORT"
	case LogReplicationOptSourceUser:
		return "SOURCE_USER"
	case LogReplicationOptSourcePassword:
		return "SOURCE_PASSWORD"
	case LogReplicationOptProtectionMode:
		return "PROTECTION_MODE"
	case LogReplicationOptDegradeTimeout:
		return "DEGRADE_TIMEOUT"
	case LogReplicationOptDetached:
		return "DETACHED"
	case LogReplicationOptLearnerCount:
		return "LEARNER_COUNT"
	default:
		return "UNKNOWN"
	}
}

type LogReplicationOpt struct {
	Tp        LogReplicationOptType
	Value     string
	UintValue uint64
}

func (o *LogReplicationOpt) Restore(ctx *format.RestoreCtx) error {
	switch o.Tp {
	case LogReplicationOptSourceHost, LogReplicationOptSourcePort, LogReplicationOptSourceUser,
		LogReplicationOptSourcePassword, LogReplicationOptProtectionMode, LogReplicationOptDegradeTimeout,
		LogReplicationOptDetached, LogReplicationOptLearnerCount:
		// ok
	default:
		return errors.Errorf("unknown LogReplicationOptType: %d", o.Tp)
	}

	ctx.WriteKeyWord(o.Tp.String())
	if o.Tp == LogReplicationOptDetached {
		return nil
	}
	ctx.WritePlain(" = ")
	switch o.Tp {
	case LogReplicationOptSourcePort, LogReplicationOptLearnerCount:
		ctx.WritePlainf("%d", o.UintValue)
	case LogReplicationOptProtectionMode:
		pm := ProtectionMode(o.UintValue)
		switch pm {
		case ProtectionModeMaximumAvailability, ProtectionModeMaximumPerformance,
			ProtectionModeMaximumProtection:
			ctx.WriteKeyWord(pm.String())
		default:
			return errors.Errorf("unknown ProtectionMode: %d", pm)
		}
	default:
		ctx.WriteString(o.Value)
	}
	return nil
}

type ProtectionMode int

const (
	ProtectionModeMaximumPerformance ProtectionMode = iota
	ProtectionModeMaximumAvailability
	ProtectionModeMaximumProtection
)

func (m ProtectionMode) String() string {
	switch m {
	case ProtectionModeMaximumPerformance:
		return "MAXIMUM_PERFORMANCE"
	case ProtectionModeMaximumAvailability:
		return "MAXIMUM_AVAILABILITY"
	case ProtectionModeMaximumProtection:
		return "MAXIMUM_PROTECTION"
	default:
		return "UNKNOWN"
	}
}
