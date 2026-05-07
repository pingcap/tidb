package executor

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	pkdbrepl "github.com/pingcap/tidb/pkg/domain/pkdb_repl"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/pkdb"
	"go.uber.org/zap"
)

const (
	defaultHTTPTimeout = 30 * time.Second

	tikvConfigReplicatorEnabledKey = "replicator.enable"
	tikvConfigLogArchiveEnabledKey = "raft-engine.enable-log-archive"
)

var (
	requiredTiKVConfigKeysMessage = fmt.Sprintf("'%s' and '%s'", tikvConfigReplicatorEnabledKey, tikvConfigLogArchiveEnabledKey)
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
	Detached          bool

	workflowID uint64
	dataFilled bool
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

	sourcePDAddrs, err := fetchSourcePDAddrsFromTiDB(ctx, e.Host, e.Port, e.User, e.Password)
	if err != nil {
		return err
	}

	pdCli := do.GetPDClient()
	if err := validateLogReplicationTiKVConfig(ctx, pdCli, sourcePDAddrs); err != nil {
		return err
	}

	opts := &pd.LogReplicationOptions{
		SourcePDAddrs:     sourcePDAddrs,
		ProtectionMode:    astProtectionModeToPB(e.ProtectionMode),
		DegradeTimeoutSec: e.DegradeTimeoutSec,
	}

	pkdbrepl.HoldRestart()
	defer pkdbrepl.ReleaseRestart()

	workflowID, err := pdCli.CreateLogReplication(ctx, e.Name.L, opts)
	if err != nil {
		return err
	}
	if e.Detached {
		e.workflowID = workflowID
		return nil
	}

	return pollWorkflowComplete(ctx, pdCli, workflowID, &e.Ctx().GetSessionVars().SQLKiller)
}

// Next implements the Executor Next interface.
func (e *CreateLogReplicationExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.MaxChunkSize())
	if !e.Detached || e.dataFilled {
		return nil
	}
	req.AppendUint64(0, e.workflowID)
	e.dataFilled = true
	return nil
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

	// wait until lag is low before switching to sync replication
	if e.ProtectionMode != ast.ProtectionModeMaximumPerformance {
		maxLagSec := int64(variable.DefTiDBAlterSyncMaxLagSeconds)
		if s, err := e.Ctx().GetSessionVars().GetSessionOrGlobalSystemVar(ctx, variable.TiDBAlterSyncMaxLagSeconds); err == nil {
			if v, err := strconv.ParseInt(s, 10, 64); err == nil && v >= 0 {
				maxLagSec = v
			}
		}

	waitLagRetryLoop:
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := e.Ctx().GetSessionVars().SQLKiller.HandleSignal(); err != nil {
				return err
			}

			statuses, err := pdCli.ListLogReplStatuses(ctx)
			if err != nil {
				return err
			}

			var targetStatus *pdpb.LogReplicationStatus
			for _, status := range statuses {
				if status.GetName() == e.Name.L {
					targetStatus = status
				}
			}
			if targetStatus == nil {
				return errors.Errorf("no replication status found for %s", e.Name)
			}

			lagSec := int64(-1)
			replicationMode := targetStatus.GetReplicationMode()
			replicationState := targetStatus.GetState()
			switch replicationState {
			case pkdb.LogReplicationStatePaused:
				return errors.Errorf("replication status for %s is %s, can't ALTER", e.Name.L, replicationState)
			case pkdb.LogReplicationStateInitializing:
				// leave lagSec = -1 to retry
			case pkdb.LogReplicationStateReplicating:
				switch replicationMode {
				case pkdb.LogReplicationModeSync:
					break waitLagRetryLoop
				case pkdb.LogReplicationModeAsync:
					lagSec = targetStatus.GetCheckpointLagSec()
				}
			default:
				// leave lagSec = -1 to retry
				logutil.Logger(ctx).Warn("unknown replication state, will retry", zap.String("replication_state", replicationState))
			}

			if lagSec < 0 || lagSec > maxLagSec {
				logutil.Logger(ctx).Warn("lag is too large or unavailable, will wait lag decreased",
					zap.String("replicationMode", replicationMode),
					zap.Int64("currentLagSec", lagSec),
					zap.Int64("maxLagSec", maxLagSec))
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
					continue
				}
			}

			break
		}
	}

	opts := pd.LogReplicationOptions{
		ProtectionMode:     astProtectionModeToPB(e.ProtectionMode),
		DegradeTimeoutSec:  e.DegradeTimeoutSec,
		NewSourceClusterID: e.NewSourceClusterID,
	}

	pkdbrepl.HoldRestart()
	defer pkdbrepl.ReleaseRestart()

	workflowID, err := pdCli.AlterLogReplication(ctx, e.Name.L, &opts)
	if err != nil {
		return err
	}

	return pollWorkflowComplete(ctx, pdCli, workflowID, &e.Ctx().GetSessionVars().SQLKiller)
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

	workflowID, err := pdCli.PauseLogReplication(ctx, e.Name.L)
	if err != nil {
		return err
	}

	return pollWorkflowComplete(ctx, pdCli, workflowID, &e.Ctx().GetSessionVars().SQLKiller)
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

	workflowID, err := pdCli.ResumeLogReplication(ctx, e.Name.L)
	if err != nil {
		return err
	}

	return pollWorkflowComplete(ctx, pdCli, workflowID, &e.Ctx().GetSessionVars().SQLKiller)
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

	workflowID, err := pdCli.DropLogReplication(ctx, e.Name.L)
	if err != nil {
		return err
	}

	return pollWorkflowComplete(ctx, pdCli, workflowID, &e.Ctx().GetSessionVars().SQLKiller)
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

	pkdbrepl.HoldRestart()
	defer pkdbrepl.ReleaseRestart()

	workflowID, err := pdCli.SwitchOverPrimary(ctx, e.NewPrimaryClusterID)
	if err != nil {
		return err
	}

	return pollWorkflowComplete(ctx, pdCli, workflowID, &e.Ctx().GetSessionVars().SQLKiller)
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

	pkdbrepl.HoldRestart()
	defer pkdbrepl.ReleaseRestart()

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
	workflowID, err := pdCli.SwitchOverPrimary(ctx, clusterID)

	if err != nil {
		return err
	}

	return pollWorkflowComplete(ctx, pdCli, workflowID, &e.Ctx().GetSessionVars().SQLKiller)
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

	pkdbrepl.HoldRestart()
	defer pkdbrepl.ReleaseRestart()

	workflowID, err := pdCli.ActivateStandby(ctx, opts)
	if err != nil {
		return err
	}

	return pollWorkflowComplete(ctx, pdCli, workflowID, &e.Ctx().GetSessionVars().SQLKiller)
}

func fetchSourcePDAddrsFromTiDB(
	ctx context.Context,
	sourceHost string,
	sourcePort int,
	sourceUser string,
	sourcePassword string,
) ([]string, error) {
	db, err := openSourceTiDB(ctx, sourceHost, sourcePort, sourceUser, sourcePassword)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = db.Close()
	}()

	const checkPrivSQL = `SELECT COUNT(*)
		FROM information_schema.USER_PRIVILEGES
		WHERE PRIVILEGE_TYPE = 'SUPER'
		AND GRANTEE = CONCAT("'", REPLACE(CURRENT_USER(), '@', "'@'"), "'")`

	var count int
	if err := db.QueryRowContext(ctx, checkPrivSQL).Scan(&count); err != nil {
		return nil, errors.Annotate(err, "check source TiDB SUPER privilege for log replication")
	}
	if count == 0 {
		return nil, errors.New("the provided user does not have SUPER privilege in source TiDB")
	}

	rows, err := db.QueryContext(
		ctx,
		"SELECT VALUE FROM information_schema.cluster_config WHERE TYPE = 'pd' AND `KEY` = 'advertise-client-urls'",
	)
	if err != nil {
		return nil, errors.Annotate(err, "query source PD addresses from source TiDB")
	}
	defer func() {
		_ = rows.Close()
	}()

	var pdAddrs []string
	for rows.Next() {
		var value sql.NullString
		if err := rows.Scan(&value); err != nil {
			return nil, errors.Annotate(err, "scan source PD addresses from source TiDB")
		}
		if !value.Valid {
			continue
		}
		addrs, err := splitAndTrimPDAddrs(value.String)
		if err != nil {
			return nil, errors.Annotate(err, "parse source PD addresses from source TiDB")
		}
		pdAddrs = append(pdAddrs, addrs...)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Annotate(err, "scan source PD addresses from source TiDB")
	}
	if len(pdAddrs) == 0 {
		return nil, errors.New("no source PD addresses found in source TiDB")
	}
	return pdAddrs, nil
}

func validateLogReplicationTiKVConfig(
	ctx context.Context,
	currentPD pd.Client,
	sourcePDAddrs []string,
) error {
	tlsCfg, err := clusterTLSConfig()
	if err != nil {
		return err
	}

	instances, configValues, err := fetchTiKVConfigFromPD(ctx, "current", currentPD, tlsCfg)
	if err != nil {
		return err
	}
	if err := validateTiKVConfigEnabled("current", instances, configValues); err != nil {
		return err
	}

	sourcePD, err := pd.NewClientWithContext(ctx, sourcePDAddrs, clusterPDSecurityOption())
	if err != nil {
		return errors.Annotate(err, "connect source PD for log replication config check")
	}
	defer sourcePD.Close()

	instances, configValues, err = fetchTiKVConfigFromPD(ctx, "source", sourcePD, tlsCfg)
	if err != nil {
		return err
	}
	return validateTiKVConfigEnabled("source", instances, configValues)
}

func validateTiKVConfigEnabled(cluster string, instances []string, configValues map[string]map[string]bool) error {
	invalidCount := 0
	for _, instance := range instances {
		for _, key := range requiredTiKVConfigKeys {
			if !configValues[key][instance] {
				invalidCount++
				break
			}
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

func clusterTLSConfig() (*tls.Config, error) {
	clusterSecurity := config.GetGlobalConfig().Security.ClusterSecurity()
	tlsCfg, err := clusterSecurity.ToTLSConfig()
	if err != nil {
		return nil, errors.Annotate(err, "build cluster TLS config for log replication")
	}
	return tlsCfg, nil
}

func clusterPDSecurityOption() pd.SecurityOption {
	cfg := config.GetGlobalConfig()
	return pd.SecurityOption{
		CAPath:   cfg.Security.ClusterSSLCA,
		CertPath: cfg.Security.ClusterSSLCert,
		KeyPath:  cfg.Security.ClusterSSLKey,
	}
}

func openSourceTiDB(
	ctx context.Context,
	sourceHost string,
	sourcePort int,
	sourceUser string,
	sourcePassword string,
) (*sql.DB, error) {
	tlsCfg, err := sourceTiDBSQLTLSConfig()
	if err != nil {
		return nil, err
	}

	driverCfg := mysql.NewConfig()
	driverCfg.Params = map[string]string{"charset": "utf8mb4"}
	driverCfg.User = sourceUser
	driverCfg.Passwd = sourcePassword
	driverCfg.AllowNativePasswords = true
	driverCfg.Net = "tcp"
	driverCfg.Addr = net.JoinHostPort(sourceHost, strconv.Itoa(sourcePort))
	driverCfg.TLS = tlsCfg

	connector, err := mysql.NewConnector(driverCfg)
	if err != nil {
		return nil, errors.Annotate(err, "open source TiDB connection for log replication")
	}
	db := sql.OpenDB(connector)
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, errors.Annotate(err, "open source TiDB connection for log replication")
	}
	return db, nil
}

func sourceTiDBSQLTLSConfig() (*tls.Config, error) {
	security := config.GetGlobalConfig().Security
	tlsCfg, err := tidbutil.ToTLSConfig(security.SSLCA, security.SSLCert, security.SSLKey)
	if err != nil {
		return nil, errors.Annotate(err, "build source TiDB SQL TLS config for log replication")
	}
	return tlsCfg, nil
}

func splitAndTrimPDAddrs(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		addr := strings.TrimSpace(part)
		if addr == "" {
			return nil, errors.New("source PD address is empty")
		}
		addrs = append(addrs, addr)
	}
	if len(addrs) == 0 {
		return nil, errors.New("source PD address is empty")
	}
	return addrs, nil
}

type logReplStoreLister interface {
	GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error)
}

func fetchTiKVConfigFromPD(
	ctx context.Context,
	cluster string,
	storeLister logReplStoreLister,
	tlsCfg *tls.Config,
) ([]string, map[string]map[string]bool, error) {
	stores, err := storeLister.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return nil, nil, errors.Annotate(err, "query TiKV stores from PD")
	}

	httpCli := tidbutil.ClientWithTLS(tlsCfg)
	httpCli.Timeout = defaultHTTPTimeout

	instances := make([]string, 0, len(stores))
	configValues := make(map[string]map[string]bool, len(requiredTiKVConfigKeys))
	for _, store := range stores {
		if engine.IsTiFlash(store) || engine.IsReplicator(store) {
			continue
		}
		if store.GetState() != metapb.StoreState_Up {
			return nil, nil, errors.Errorf("%s TiKV store %d is not in Up state", cluster, store.GetId())
		}
		instance := store.GetAddress()
		if instance == "" {
			return nil, nil, errors.Errorf("%s TiKV store %d has empty address", cluster, store.GetId())
		}
		statusAddr := store.GetStatusAddress()
		if statusAddr == "" {
			return nil, nil, errors.Errorf("%s TiKV store %d has empty status address", cluster, store.GetId())
		}

		flatConfig, err := fetchTiKVFlattenedConfigFromStatusAddr(ctx, httpCli, statusAddr, tlsCfg != nil)
		if err != nil {
			return nil, nil, errors.Annotatef(
				err,
				"fetch %s TiKV config from status address %s for store %d",
				cluster,
				statusAddr,
				store.GetId(),
			)
		}
		instances = append(instances, instance)
		for _, key := range requiredTiKVConfigKeys {
			configByKey := configValues[key]
			if configByKey == nil {
				configByKey = make(map[string]bool)
				configValues[key] = configByKey
			}
			enabled, _ := strconv.ParseBool(strings.TrimSpace(fmt.Sprint(flatConfig[key])))
			configByKey[instance] = enabled
		}
	}
	slices.Sort(instances)
	return instances, configValues, nil
}

func fetchTiKVFlattenedConfigFromStatusAddr(
	ctx context.Context,
	httpCli *http.Client,
	statusAddr string,
	useTLS bool,
) (map[string]any, error) {
	configURL := tikvStatusConfigURL(statusAddr, useTLS)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, configURL, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp, err := httpCli.Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("request %s failed: %s", configURL, resp.Status)
	}

	var nested map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&nested); err != nil {
		return nil, errors.Trace(err)
	}
	return config.FlattenConfigItems(nested), nil
}

func tikvStatusConfigURL(statusAddr string, useTLS bool) string {
	if strings.HasPrefix(statusAddr, "http://") || strings.HasPrefix(statusAddr, "https://") {
		return strings.TrimRight(statusAddr, "/") + "/config"
	}
	scheme := "http"
	if useTLS {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s/config", scheme, statusAddr)
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

type logReplWorkflowLister interface {
	ListLogReplWorkflows(ctx context.Context) ([]*pdpb.WorkflowInfo, error)
}

func newWorkflowNotCancelableErr(workflowID uint64) error {
	return errors.Errorf("workflow %d cannot be cancelled at the moment", workflowID)
}

func pollWorkflowComplete(
	ctx context.Context,
	workflowLister logReplWorkflowLister,
	workflowID uint64,
	sqlKiller *sqlkiller.SQLKiller,
) error {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 100 * time.Millisecond
	bo.RandomizationFactor = 0
	bo.MaxInterval = time.Second * 2
	bo.Multiplier = 1.5
	bo.MaxElapsedTime = 0
	bo.Reset()

	err := backoff.RetryNotify(func() error {
		if err := sqlKiller.HandleSignal(); err != nil {
			return backoff.Permanent(err)
		}

		// TODO: optimize by adding a PD API to query workflow status by ID,
		// instead of listing all workflows and find the target one.
		workflows, err := workflowLister.ListLogReplWorkflows(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return backoff.Permanent(err)
			}
			return err
		}
		for _, wf := range workflows {
			if wf.Id == workflowID {
				if wf.State == "COMPLETED" {
					return nil
				}
				if wf.State == "CANCELLED" {
					return backoff.Permanent(errors.Errorf("workflow %d is cancelled", workflowID))
				}
				return fmt.Errorf("workflow %d not completed, state=%s", workflowID, wf.State)
			}
		}
		// workflow not found in the list, treat as completed
		return nil
	}, backoff.WithContext(bo, ctx), func(err error, next time.Duration) {
		logutil.Logger(ctx).Info(
			"poll log replication workflow state",
			zap.Uint64("workflowID", workflowID),
			zap.Duration("nextPollIn", next),
			zap.Error(err),
		)
	})
	if stderrors.Is(err, context.Canceled) {
		return newWorkflowNotCancelableErr(workflowID)
	}
	return err
}
