package ast

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
)

type CreateLogReplication struct {
	Name model.CIStr
	Opts []*LogReplicationOpt
}

func (c *CreateLogReplication) Restore(ctx *format.RestoreCtx) error {
	if c == nil {
		return errors.New("CreateLogReplication is nil")
	}
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
}

func (a *AlterLogReplication) Restore(ctx *format.RestoreCtx) error {
	if a == nil {
		return errors.New("AlterLogReplication is nil")
	}
	ctx.WriteKeyWord("ALTER LOG REPLICATION ")
	ctx.WriteName(a.Name.String())
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

type PauseLogReplication struct {
	Name model.CIStr
}

func (p *PauseLogReplication) Restore(ctx *format.RestoreCtx) error {
	if p == nil {
		return errors.New("PauseLogReplication is nil")
	}
	ctx.WriteKeyWord("PAUSE LOG REPLICATION ")
	ctx.WriteName(p.Name.String())
	return nil
}

type ResumeLogReplication struct {
	Name model.CIStr
}

func (r *ResumeLogReplication) Restore(ctx *format.RestoreCtx) error {
	if r == nil {
		return errors.New("ResumeLogReplication is nil")
	}
	ctx.WriteKeyWord("RESUME LOG REPLICATION ")
	ctx.WriteName(r.Name.String())
	return nil
}

type DropLogReplication struct {
	Name model.CIStr
}

func (d *DropLogReplication) Restore(ctx *format.RestoreCtx) error {
	if d == nil {
		return errors.New("DropLogReplication is nil")
	}
	ctx.WriteKeyWord("DROP LOG REPLICATION ")
	ctx.WriteName(d.Name.String())
	return nil
}

type SwitchOverPrimary struct {
	NewPrimaryClusterID uint64
}

func (s *SwitchOverPrimary) Restore(ctx *format.RestoreCtx) error {
	if s == nil {
		return errors.New("SwitchOverPrimary is nil")
	}
	ctx.WriteKeyWord("SWITCHOVER PRIMARY TO ")
	ctx.WritePlainf("%d", s.NewPrimaryClusterID)
	return nil
}

type ActivateStandbyMode int

const (
	ActivateStandbyModeUnknown ActivateStandbyMode = iota
	ActivateStandbyModeFlashback
	ActivateStandbyModeForceCommit
)

func (m ActivateStandbyMode) String() string {
	switch m {
	case ActivateStandbyModeFlashback:
		return "FLASHBACK"
	case ActivateStandbyModeForceCommit:
		return "FORCE_COMMIT"
	default:
		return "UNKNOWN"
	}
}

type ActivateStandby struct {
	Mode ActivateStandbyMode
}

func (a *ActivateStandby) Restore(ctx *format.RestoreCtx) error {
	if a == nil {
		return errors.New("ActivateStandby is nil")
	}
	ctx.WriteKeyWord("ACTIVATE STANDBY")
	switch a.Mode {
	case ActivateStandbyModeUnknown:
		return errors.New("unspecified ActivateStandby mode")
	case ActivateStandbyModeFlashback, ActivateStandbyModeForceCommit:
		ctx.WriteKeyWord(" MODE = ")
		ctx.WriteKeyWord(a.Mode.String())
		return nil
	default:
		return errors.Errorf("unknown ActivateStandbyMode: %d", a.Mode)
	}
}

type LogReplicationOptType int

const (
	LogReplicationOptSourceHost LogReplicationOptType = iota
	LogReplicationOptSourcePort
	LogReplicationOptSourceUser
	LogReplicationOptSourcePassword
	LogReplicationOptProtectionMode
	LogReplicationOptDegradeTimeout
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
	if o == nil {
		return errors.New("LogReplicationOpt is nil")
	}
	switch o.Tp {
	case LogReplicationOptSourceHost, LogReplicationOptSourcePort, LogReplicationOptSourceUser,
		LogReplicationOptSourcePassword, LogReplicationOptProtectionMode, LogReplicationOptDegradeTimeout:
		// ok
	default:
		return errors.Errorf("unknown LogReplicationOptType: %d", o.Tp)
	}

	ctx.WriteKeyWord(o.Tp.String())
	ctx.WritePlain(" = ")
	switch o.Tp {
	case LogReplicationOptSourcePort:
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
	ProtectionModeMaximumProtection
	ProtectionModeMaximumAvailability
)

func (m ProtectionMode) String() string {
	switch m {
	case ProtectionModeMaximumPerformance:
		return "MAXIMUM_PERFORMANCE"
	case ProtectionModeMaximumProtection:
		return "MAXIMUM_PROTECTION"
	case ProtectionModeMaximumAvailability:
		return "MAXIMUM_AVAILABILITY"
	default:
		return "UNKNOWN"
	}
}
