package executor

import (
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

func (b *executorBuilder) pkdbBuild(p base.Plan) exec.Executor {
	switch v := p.(type) {
	case *plannercore.CreateLogReplication:
		return &CreateLogReplicationExec{
			BaseExecutor:      exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
			Name:              v.Name,
			Host:              v.Host,
			Port:              v.Port,
			User:              v.User,
			Password:          v.Password,
			ProtectionMode:    v.ProtectionMode,
			DegradeTimeoutSec: v.DegradeTimeoutSec,
			Detached:          v.Detached,
		}
	case *plannercore.AlterLogReplication:
		return &AlterLogReplicationExec{
			BaseExecutor:       exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
			Name:               v.Name,
			NewSourceClusterID: v.NewSourceClusterID,
			ProtectionMode:     v.ProtectionMode,
			DegradeTimeoutSec:  v.DegradeTimeoutSec,
		}

	case *plannercore.PauseLogReplication:
		return &PauseLogReplicationExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
			Name:         v.Name,
		}
	case *plannercore.ResumeLogReplication:
		return &ResumeLogReplicationExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
			Name:         v.Name,
		}
	case *plannercore.DropLogReplication:
		return &DropLogReplicationExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
			Name:         v.Name,
		}
	case *plannercore.SwitchOverPrimary:
		return &SwitchOverPrimaryExec{
			BaseExecutor:        exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
			NewPrimaryClusterID: v.NewPrimaryClusterID,
		}
	case *plannercore.SwitchOverAsPrimary:
		return &SwitchOverAsPrimaryExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
		}
	case *plannercore.ActivateStandby:
		return &ActivateStandbyExec{
			BaseExecutor: exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID()),
			Mode:         v.Mode,
		}
	default:
		return nil
	}
}
