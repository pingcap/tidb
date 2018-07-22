package domain

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/printer"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

//GetServerInfo gets self DDL server static information.
func GetServerInfo(ddlID string) *util.DDLServerInfo {
	cfg := config.GetGlobalConfig()
	info := &util.DDLServerInfo{
		ID:         ddlID,
		IP:         cfg.AdvertiseAddress,
		StatusPort: cfg.Status.StatusPort,
		Lease:      cfg.Lease,
	}
	info.Version = mysql.ServerVersion
	info.GitHash = printer.TiDBGitHash
	return info
}

// GetOwnerServerInfo gets owner DDL server static information from PD.
func GetOwnerServerInfo(d ddl.DDL) (*util.DDLServerInfo, error) {
	ctx := context.Background()
	ddlOwnerID, err := d.OwnerManager().GetOwnerID(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ownerInfo, err := d.SchemaSyncer().GetServerInfoFromPD(ctx, ddlOwnerID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ownerInfo, nil
}

// GetAllServerInfo gets all DDL servers static information from PD.
func GetAllServerInfo(d ddl.DDL) (map[string]*util.DDLServerInfo, error) {
	ctx := context.Background()
	AllDDLServerInfo, err := d.SchemaSyncer().GetAllServerInfoFromPD(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return AllDDLServerInfo, nil
}

// StoreServerInfoToPD stores self DDL server static information to PD when domain Init.
func StoreServerInfoToPD(d ddl.DDL) error {
	info := GetServerInfo(d.GetID())
	ctx := context.Background()
	return d.SchemaSyncer().StoreSelfServerInfo(ctx, info)
}

// RemoveServerInfoFromPD remove self DDL server static information from PD when domain close.
func RemoveServerInfoFromPD(d ddl.DDL) {
	err := d.SchemaSyncer().RemoveSelfServerInfo()
	if err != nil {
		log.Errorf("[ddl] remove self server info failed %v", err)
	}
}
