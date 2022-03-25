package restore

import "github.com/pingcap/tidb/parser/model"

var incrementalRestoreActionBlacklist = []model.ActionType{
	model.ActionSetTiFlashReplica,
	model.ActionUpdateTiFlashReplicaStatus,
	model.ActionLockTable,
	model.ActionUnlockTable,
}
