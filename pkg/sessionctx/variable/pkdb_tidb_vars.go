package variable

import (
	"time"

	"go.uber.org/atomic"
)

const (
	// TiDBCreateFromSelectUsingImport indicates whether to use import into to create table as select.
	TiDBCreateFromSelectUsingImport = "tidb_create_from_select_using_import"

	// TiDBXEnableScheduleLeaderRule indicates whether to enable region leader in one store.
	TiDBXEnableScheduleLeaderRule = "tidbx_enable_schedule_leader_rule"
	// TiDBXEnableTiKVLocalCall indicates whether to enable TiKV local calls.
	TiDBXEnableTiKVLocalCall = "tidbx_enable_tikv_local_call"
	// TiDBXEnablePDLocalCall indicates whether to use Inter-Process Call for PD.
	TiDBXEnablePDLocalCall = "tidbx_enable_pd_local_call"
	// TiDBXShmLocalCall indicates whether to enable TiKV local calls with shared memory ring.
	TiDBXShmLocalCall = "tidbx_shm_local_call"
	// TiDBXLocalCallNoMarshall indicates whether to enable TiKV local calls with marshal reprc.
	TiDBXLocalCallNoMarshall = "tidbx_local_call_no_marshal"
	// TiDBXStoreBatchGet indicates whether to enable store batch get.
	TiDBXStoreBatchGet = "tidbx_store_batch_get"
	// TiDBXEnableFastPath indicates whether to enable the fast path, for performance testing.
	TiDBXEnableFastPath = "tidbx_fast_path"
)

// Default TiDB system variable values.
const (
	DefTiDBXEnableLocalRPCOpt          = false
	DefTiDBXEnableScheduleLeaderRule   = false
	DefTiDBEnableLabelSecurity         = false
	DefTiDBEnableLoginHistory          = false
	DefTiDBLoginHistoryRetainDuration  = time.Hour * 24 * 90 // default 90 days.
	DefStoredProgramCacheSize          = 256
	DefTiDBEnableProcedure             = false
	DefTiDBEnableDutySeparationMode    = false
	DefTiDBEnableUDVSubstitute         = false
	DefTiDBEnableSPParamSubstitute     = false
	DefTiDBCreateFromSelectUsingImport = false
	DefTiDBXShmLocalCallOpt            = false
	DefTiDBXLocalCallNoMarshallOpt     = false
	DefTiDBXStoreBatchGetOpt           = false
	DefTiDBXFastPath                   = false
)

// UnspecifiedServerID indicates the unspecified server id.
const UnspecifiedServerID = 0

// Process global variables.
var (
	EnableScheduleLeaderRule                = atomic.NewBool(DefTiDBXEnableScheduleLeaderRule)
	EnableScheduleLeaderRuleFn func(v bool) = nil
	EnableLabelSecurity                     = atomic.NewBool(DefTiDBEnableLabelSecurity)

	EnableLoginHistory         = atomic.NewBool(DefTiDBEnableLoginHistory)
	LoginHistoryRetainDuration = atomic.NewDuration(DefTiDBLoginHistoryRetainDuration)
	StoredProgramCacheSize     = atomic.NewInt64(DefStoredProgramCacheSize)
	TiDBEnableSPAstReuse       = atomic.NewBool(true)
	TiDBEnableProcedureValue   = atomic.NewBool(DefTiDBEnableProcedure)
	AutomaticSPPrivileges      = atomic.NewBool(true)
	EnableDutySeparationMode   = atomic.NewBool(DefTiDBEnableDutySeparationMode)
	EnableFastPath             = atomic.NewBool(DefTiDBXFastPath)
)
