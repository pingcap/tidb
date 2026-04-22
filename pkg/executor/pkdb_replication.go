package executor

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	pd "github.com/tikv/pd/client"
)

const (
	tikvConfigReplicatorEnabledKey = "replicator.enable"
	tikvConfigLogArchiveEnabledKey = "raft-engine.enable-log-archive"

	selectTiKVInstancesFromClusterInfoSQL = "SELECT DISTINCT `instance` FROM information_schema.cluster_info WHERE `type`='tikv'"
)

var (
	requiredTiKVConfigKeysSQL     = fmt.Sprintf("'%s','%s'", tikvConfigReplicatorEnabledKey, tikvConfigLogArchiveEnabledKey)
	requiredTiKVConfigKeysMessage = fmt.Sprintf("'%s' and '%s'", tikvConfigReplicatorEnabledKey, tikvConfigLogArchiveEnabledKey)
	selectTiKVConfigSQL           = fmt.Sprintf("SELECT `instance`, `key`, `value` FROM information_schema.cluster_config WHERE `type`='tikv' AND `key` IN (%s)", requiredTiKVConfigKeysSQL)
	requiredTiKVConfigKeys        = []string{
		tikvConfigReplicatorEnabledKey,
		tikvConfigLogArchiveEnabledKey,
	}
)

// CreateLogReplicationExec executes CREATE LOG REPLICATION statement.
type CreateLogReplicationExec struct {
	exec.BaseExecutor

	Name              model.CIStr
	Host              string
	Port              int
	User              string
	Password          string
	ProtectionMode    ast.ProtectionMode
	DegradeTimeoutSec uint64
}

// Open implements the Executor Open interface.
func (e *CreateLogReplicationExec) Open(ctx context.Context) error {
	do := domain.GetDomain(e.Ctx())
	infoSchema := do.InfoSchema()

	for _, schemaName := range infoSchema.AllSchemaNames() {
		if tidbutil.IsMemOrSysDB(schemaName.L) {
			continue
		}
		userDataName := schemaName.O
		if schemaName.L == "test" {
			tableInfos, err := infoSchema.SchemaTableInfos(ctx, schemaName)
			if err != nil {
				return err
			}
			if len(tableInfos) == 0 {
				continue
			}
			userDataName = "test." + tableInfos[0].Name.O
		}
		return errors.Errorf(
			"cluster contains existing user data (%s). Create log replication cannot proceed until all user-created data are removed",
			userDataName,
		)
	}

	if err := validateLogReplicationTiKVConfig(ctx, e.Ctx(), e.Host, e.Port, e.User, e.Password); err != nil {
		return err
	}

	pdCli := do.GetPDClient()
	opts := &pd.LogReplicationOptions{
		SourceHost:        e.Host,
		SourcePort:        uint16(e.Port),
		SourceUser:        e.User,
		SourcePassword:    e.Password,
		ProtectionMode:    astProtectionModeToPB(e.ProtectionMode),
		DegradeTimeoutSec: e.DegradeTimeoutSec,
	}
	return pdCli.CreateLogReplication(ctx, e.Name.L, opts)
}

func validateLogReplicationTiKVConfig(
	ctx context.Context,
	sctx sessionctx.Context,
	sourceHost string,
	sourcePort int,
	sourceUser string,
	sourcePassword string,
) error {
	instances, configValues, err := fetchTiKVConfigFromCurrentCluster(ctx, sctx)
	if err != nil {
		return err
	}
	if err := validateTiKVConfigEnabled("current", instances, configValues); err != nil {
		return err
	}

	instances, configValues, err = fetchTiKVConfigFromSourceCluster(ctx, sourceHost, sourcePort, sourceUser, sourcePassword)
	if err != nil {
		return err
	}
	return validateTiKVConfigEnabled("source", instances, configValues)
}

func fetchTiKVConfigFromCurrentCluster(
	ctx context.Context,
	sctx sessionctx.Context,
) ([]string, map[string]map[string]bool, error) {
	instances, err := fetchTiKVInstancesFromRestrictedSQL(ctx, sctx)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	configValues, err := fetchTiKVConfigValuesFromRestrictedSQL(ctx, sctx)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	slices.Sort(instances)
	return instances, configValues, nil
}

func fetchTiKVConfigFromSourceCluster(
	ctx context.Context,
	sourceHost string,
	sourcePort int,
	sourceUser string,
	sourcePassword string,
) ([]string, map[string]map[string]bool, error) {
	db, err := dbutil.OpenDB(dbutil.DBConfig{
		Host:     sourceHost,
		Port:     sourcePort,
		User:     sourceUser,
		Password: sourcePassword,
	}, nil)
	if err != nil {
		return nil, nil, errors.Annotate(err, "open source cluster connection for log replication config check")
	}
	defer func() {
		_ = dbutil.CloseDB(db)
	}()

	instances, err := fetchTiKVInstancesFromSQLDB(ctx, db)
	if err != nil {
		return nil, nil, errors.Annotate(err, "query source cluster tikv instances for log replication config check")
	}
	configValues, err := fetchTiKVConfigValuesFromSQLDB(ctx, db)
	if err != nil {
		return nil, nil, errors.Annotate(err, "query source cluster tikv config for log replication check")
	}
	slices.Sort(instances)
	return instances, configValues, nil
}

func fetchTiKVInstancesFromRestrictedSQL(ctx context.Context, sctx sessionctx.Context) ([]string, error) {
	executor := sctx.GetRestrictedSQLExecutor()
	instanceRows, _, err := executor.ExecRestrictedSQL(ctx, nil, selectTiKVInstancesFromClusterInfoSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	instances := make([]string, 0, len(instanceRows))
	for _, row := range instanceRows {
		if row.IsNull(0) {
			continue
		}
		instances = append(instances, row.GetString(0))
	}
	return instances, nil
}

func fetchTiKVConfigValuesFromRestrictedSQL(ctx context.Context, sctx sessionctx.Context) (map[string]map[string]bool, error) {
	executor := sctx.GetRestrictedSQLExecutor()
	configRows, _, err := executor.ExecRestrictedSQL(ctx, nil, selectTiKVConfigSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	configValues := make(map[string]map[string]bool, len(requiredTiKVConfigKeys))
	for _, row := range configRows {
		if row.IsNull(0) || row.IsNull(1) {
			continue
		}
		instance := row.GetString(0)
		key := row.GetString(1)
		value := ""
		if !row.IsNull(2) {
			value = row.GetString(2)
		}
		setTiKVConfigValue(configValues, instance, key, value)
	}
	return configValues, nil
}

func fetchTiKVInstancesFromSQLDB(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, selectTiKVInstancesFromClusterInfoSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var instances []string
	for rows.Next() {
		var instance sql.NullString
		if err := rows.Scan(&instance); err != nil {
			return nil, errors.Trace(err)
		}
		if instance.Valid {
			instances = append(instances, instance.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return instances, nil
}

func fetchTiKVConfigValuesFromSQLDB(ctx context.Context, db *sql.DB) (map[string]map[string]bool, error) {
	rows, err := db.QueryContext(ctx, selectTiKVConfigSQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		_ = rows.Close()
	}()

	configValues := make(map[string]map[string]bool, len(requiredTiKVConfigKeys))
	for rows.Next() {
		var instance sql.NullString
		var key sql.NullString
		var value sql.NullString
		if err := rows.Scan(&instance, &key, &value); err != nil {
			return nil, errors.Trace(err)
		}
		if !instance.Valid || !key.Valid {
			continue
		}
		rawValue := ""
		if value.Valid {
			rawValue = value.String
		}
		setTiKVConfigValue(configValues, instance.String, key.String, rawValue)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return configValues, nil
}

func setTiKVConfigValue(configValues map[string]map[string]bool, instance, key, rawValue string) {
	perKey := configValues[key]
	if perKey == nil {
		perKey = make(map[string]bool)
		configValues[key] = perKey
	}
	perKey[instance] = parseConfigBool(rawValue)
}

func parseConfigBool(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	enabled, err := strconv.ParseBool(value)
	if err != nil {
		return false
	}
	return enabled
}

func validateTiKVConfigEnabled(cluster string, instances []string, configValues map[string]map[string]bool) error {
	invalidCount := 0
	for _, instance := range instances {
		if !isTiKVConfigEnabled(instance, configValues) {
			invalidCount++
		}
	}
	if invalidCount == 0 {
		return nil
	}
	return errors.Errorf(
		"log replication requires %s enabled on all TiKV nodes in %s cluster (%d nodes not enabled)",
		requiredTiKVConfigKeysMessage,
		cluster,
		invalidCount,
	)
}

func isTiKVConfigEnabled(instance string, configValues map[string]map[string]bool) bool {
	for _, key := range requiredTiKVConfigKeys {
		if !configValues[key][instance] {
			return false
		}
	}
	return true
}

func astProtectionModeToPB(mode ast.ProtectionMode) pdpb.ProtectionMode {
	pbProtectionMode := pdpb.ProtectionMode_MaximumPerformance
	switch mode {
	case ast.ProtectionModeMaximumAvailability:
		pbProtectionMode = pdpb.ProtectionMode_MaximumAvailability
	case ast.ProtectionModeMaximumPerformance:
		pbProtectionMode = pdpb.ProtectionMode_MaximumPerformance
	case ast.ProtectionModeMaximumProtection:
		pbProtectionMode = pdpb.ProtectionMode_MaximumProtection
	}
	return pbProtectionMode
}

// AlterLogReplicationExec executes ALTER LOG REPLICATION statement.
type AlterLogReplicationExec struct {
	exec.BaseExecutor

	Name model.CIStr
	// NewSourceClusterID can't be set together with other options.
	NewSourceClusterID uint64
	ProtectionMode     ast.ProtectionMode
	DegradeTimeoutSec  uint64
}

// Open implements the Executor Open interface.
func (e *AlterLogReplicationExec) Open(ctx context.Context) error {
	do := domain.GetDomain(e.Ctx())
	pdCli := do.GetPDClient()

	opts := pd.LogReplicationOptions{
		ProtectionMode:     astProtectionModeToPB(e.ProtectionMode),
		DegradeTimeoutSec:  e.DegradeTimeoutSec,
		NewSourceClusterID: e.NewSourceClusterID,
	}
	return pdCli.AlterLogReplication(ctx, e.Name.L, &opts)
}

// PauseLogReplicationExec executes PAUSE LOG REPLICATION statement.
type PauseLogReplicationExec struct {
	exec.BaseExecutor

	Name model.CIStr
}

// Open implements the Executor Open interface.
func (e *PauseLogReplicationExec) Open(ctx context.Context) error {
	do := domain.GetDomain(e.Ctx())
	pdCli := do.GetPDClient()

	return pdCli.PauseLogReplication(ctx, e.Name.L)
}

// ResumeLogReplicationExec executes RESUME LOG REPLICATION statement.
type ResumeLogReplicationExec struct {
	exec.BaseExecutor

	Name model.CIStr
}

// Open implements the Executor Open interface.
func (e *ResumeLogReplicationExec) Open(ctx context.Context) error {
	do := domain.GetDomain(e.Ctx())
	pdCli := do.GetPDClient()

	return pdCli.ResumeLogReplication(ctx, e.Name.L)
}

// DropLogReplicationExec executes DROP LOG REPLICATION statement.
type DropLogReplicationExec struct {
	exec.BaseExecutor

	Name model.CIStr
}

// Open implements the Executor Open interface.
func (e *DropLogReplicationExec) Open(ctx context.Context) error {
	do := domain.GetDomain(e.Ctx())
	pdCli := do.GetPDClient()

	return pdCli.DropLogReplication(ctx, e.Name.L)
}

// SwitchOverPrimaryExec executes ADMIN SWITCHOVER PRIMARY statement.
type SwitchOverPrimaryExec struct {
	exec.BaseExecutor

	NewPrimaryClusterID uint64
}

// Open implements the Executor Open interface.
func (e *SwitchOverPrimaryExec) Open(ctx context.Context) error {
	do := domain.GetDomain(e.Ctx())
	pdCli := do.GetPDClient()

	return pdCli.SwitchOverPrimary(ctx, e.NewPrimaryClusterID)
}

// SwitchOverAsPrimaryExec executes ADMIN SWITCHOVER AS PRIMARY statement.
// This statement can only be executed on a standby cluster.
type SwitchOverAsPrimaryExec struct {
	exec.BaseExecutor
}

// Open implements the Executor Open interface.
func (e *SwitchOverAsPrimaryExec) Open(ctx context.Context) error {
	do := domain.GetDomain(e.Ctx())
	pdCli := do.GetPDClient()

	// Check if the current cluster is a standby.
	localStatus, err := pdCli.GetLogReplLocalStatus(ctx)
	if err != nil {
		return errors.Annotate(err, "failed to get log replication local status")
	}
	if localStatus.GetStatus().GetSourceClusterId() == 0 {
		return errors.New("ADMIN SWITCHOVER AS PRIMARY can only be executed on a standby cluster")
	}

	// Get current cluster ID and trigger switchover.
	clusterID := pdCli.GetClusterID(ctx)
	return pdCli.SwitchOverPrimary(ctx, clusterID)
}

// ActivateStandbyExec executes ADMIN ACTIVATE STANDBY statement.
type ActivateStandbyExec struct {
	exec.BaseExecutor

	Mode ast.ActivateStandbyMode
}

// Open implements the Executor Open interface.
func (e *ActivateStandbyExec) Open(ctx context.Context) error {
	do := domain.GetDomain(e.Ctx())
	pdCli := do.GetPDClient()

	opts := &pd.LogReplicationOptions{
		ActivateMode: pdpb.ActivateStandbyMode_ACTIVATE_MODE_UNKNOWN,
	}
	switch e.Mode {
	case ast.ActivateStandbyModeUnknown:
		opts.ActivateMode = pdpb.ActivateStandbyMode_ACTIVATE_MODE_UNKNOWN
	case ast.ActivateStandbyModeFlashback:
		opts.ActivateMode = pdpb.ActivateStandbyMode_ACTIVATE_MODE_FLASHBACK
	case ast.ActivateStandbyModeForceCommit:
		opts.ActivateMode = pdpb.ActivateStandbyMode_ACTIVATE_MODE_FORCE_COMMIT
	}
	return pdCli.ActivateStandby(ctx, opts)
}
