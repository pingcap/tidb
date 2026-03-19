package infoschema

import "github.com/pingcap/tidb/pkg/meta/autoid"

func init() {
	tableIDMap[TableUserLoginHistory] = autoid.ReservedTablesBaseID
	tableIDMap[ClusterTableAuditLog] = autoid.ReservedTablesBaseID + 1
	tableIDMap[TableAuditLog] = autoid.ReservedTablesBaseID + 2
	// there's a hole of autoid.ReservedTablesBaseID + 3. Can be reused later
	tableIDMap[TableRegions] = autoid.ReservedTablesBaseID + 4

	init2()
}
