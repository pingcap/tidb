// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package gluetikv

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/driver"
	pd "github.com/tikv/pd/client"
)

// Asserting Glue implements glue.ConsoleGlue and glue.Glue at compile time.
var (
	_ glue.ConsoleGlue = Glue{}
	_ glue.Glue        = Glue{}
)

// Glue is an implementation of glue.Glue that accesses only TiKV without TiDB.
type Glue struct{}

// GetDomain implements glue.Glue.
func (Glue) GetDomain(store kv.Storage) (*domain.Domain, error) {
	return nil, nil
}

// CreateSession implements glue.Glue.
func (Glue) CreateSession(store kv.Storage) (glue.Session, error) {
	return nil, nil
}

// Open implements glue.Glue.
func (Glue) Open(path string, option pd.SecurityOption) (kv.Storage, error) {
	if option.CAPath != "" {
		conf := config.GetGlobalConfig()
		conf.Security.ClusterSSLCA = option.CAPath
		conf.Security.ClusterSSLCert = option.CertPath
		conf.Security.ClusterSSLKey = option.KeyPath
		config.StoreGlobalConfig(conf)
	}
	return driver.TiKVDriver{}.Open(path)
}

// OwnsStorage implements glue.Glue.
func (Glue) OwnsStorage() bool {
	return true
}

// StartProgress implements glue.Glue.
func (Glue) StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) glue.Progress {
	return utils.StartProgress(ctx, cmdName, total, redirectLog, nil)
}

// Record implements glue.Glue.
func (Glue) Record(name string, val uint64) {
	summary.CollectSuccessUnit(name, 1, val)
}

// GetVersion implements glue.Glue.
func (Glue) GetVersion() string {
	return "BR\n" + build.Info()
}

func (Glue) Print(args ...interface{}) {
	fmt.Print(args...)
}

// SupportsScan checks whether the shell supports input.
func (Glue) SupportsScan() bool {
	return true
}

func (Glue) Scanln(args ...interface{}) (int, error) {
	return fmt.Scanln(args...)
}
