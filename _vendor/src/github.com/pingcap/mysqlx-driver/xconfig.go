package mysql

import (
	"crypto/tls"
	"time"
)

type xconfig struct {
	user                    string
	passwd                  string
	net                     string
	addr                    string
	dbname                  string
	params                  map[string]string
	loc                     *time.Location
	tls                     *tls.Config
	timeout                 time.Duration
	collation               uint8
	allowAllFiles           bool
	allowCleartextPasswords bool
	columnsWithAlias        bool
	interpolateParams       bool
	useXProtocol            bool // use X protocol rather than native protocol
	useGetCapabilities      bool // for X protocol, do we send a GetCapabilities message to query server capabilities?  default: true
}

func NewXconfigFromConfig(cfg *config) *xconfig {
	return &xconfig{
		user:                    cfg.user,
		passwd:                  cfg.passwd,
		net:                     cfg.net,
		addr:                    cfg.addr,
		dbname:                  cfg.dbname,
		params:                  cfg.params,
		loc:                     cfg.loc,
		tls:                     cfg.tls,
		timeout:                 cfg.timeout,
		collation:               cfg.collation,
		allowAllFiles:           cfg.allowAllFiles,
		allowCleartextPasswords: cfg.allowCleartextPasswords,
		columnsWithAlias:        cfg.columnsWithAlias,
		interpolateParams:       cfg.interpolateParams,
		useXProtocol:            true,
	}
}
