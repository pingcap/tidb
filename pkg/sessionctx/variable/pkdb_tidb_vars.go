package variable

import (
	"time"

	"go.uber.org/atomic"
)

const (
	// TiDBEnableSPPlanCache indicates whether to enable the stored-routine hidden prepared plan-cache path
	// and the related compatibility behavior added for it.
	TiDBEnableSPPlanCache = "tidb_enable_sp_plan_cache"
	// TiDBCreateFromSelectUsingImport indicates whether to use import into to create table as select.
	TiDBCreateFromSelectUsingImport = "tidb_create_from_select_using_import"
	// TiDBAlterSyncMaxLagSeconds controls the maximum checkpoint lag (in seconds) allowed when switching to sync
	// replication in ALTER LOG REPLICATION.
	TiDBAlterSyncMaxLagSeconds = "tidb_alter_sync_max_lag_seconds"
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
	// TiDBXEnableIndexLookUpPushDown indicates whether to enable index lookup push down optimization.
	TiDBXEnableIndexLookUpPushDown = "tidbx_enable_index_lookup_push_down"
	// TiDBXEnableSingleStoreTxn1PC indicates whether to enable single store transaction 1PC optimization.
	TiDBXEnableSingleStoreTxn1PC = "tidbx_enable_single_store_txn_1pc"
	// PKDBEnableWhitelist indicates whether to enable the whitelist feature.
	PKDBEnableWhitelist = "pkdb_whitelist"
	// PKDBExtraDataType indicates whether to enable extra data types.
	PKDBExtraDataType = "pkdb_extra_data_type"
	// PKDBEnableEAL indicates whether to enable the EAL feature.
	PKDBEnableEAL = "pkdb_eal"
	// TiDBEnableLBAC is used to enable or disable LBAC enforcement on TiDB.
	TiDBEnableLBAC = "pkdb_lbac"
)

// Default TiDB system variable values.
const (
	DefTiDBXEnableLocalRPCOpt          = false
	DefTiDBEnableLabelSecurity         = false
	DefTiDBEnableLBAC                  = false
	DefTiDBEnableLoginHistory          = false
	DefTiDBLoginHistoryRetainDuration  = time.Hour * 24 * 90 // default 90 days.
	DefStoredProgramCacheSize          = 256
	DefTiDBEnableProcedure             = false
	DefTiDBEnableSPPlanCache           = false
	DefTiDBEnableDutySeparationMode    = false
	DefTiDBEnableUDVSubstitute         = false
	DefTiDBEnableSPParamSubstitute     = false
	DefTiDBCreateFromSelectUsingImport = false
<<<<<<< HEAD
	DefTiDBXShmLocalCallOpt            = false
	DefTiDBXLocalCallNoMarshallOpt     = false
	DefTiDBXStoreBatchGetOpt           = false
	DefTiDBXFastPath                   = false
	DefTiDBXEnableIndexLookUpPushDown  = false
	DefTiDBXEnableSingleStoreTxn1PC    = false
||||||| bea0668079
=======
	DefTiDBAlterSyncMaxLagSeconds      = 10
>>>>>>> d1ce84d007974170f98e644ab39fd5b7bd4d7bcb
	DefPKDBEnableWhitelist             = false
	DefPKDBExtraDataType               = false
	DefPKDBEnableEAL                   = false
)

// UnspecifiedServerID indicates the unspecified server id.
const UnspecifiedServerID = 0

// Process global variables.
var (
	EnableLabelSecurity = atomic.NewBool(DefTiDBEnableLabelSecurity)
	EnableLBAC          = atomic.NewBool(DefTiDBEnableLBAC)

	EnableLoginHistory         = atomic.NewBool(DefTiDBEnableLoginHistory)
	LoginHistoryRetainDuration = atomic.NewDuration(DefTiDBLoginHistoryRetainDuration)
	StoredProgramCacheSize     = atomic.NewInt64(DefStoredProgramCacheSize)
	TiDBEnableSPAstReuse       = atomic.NewBool(true)
	TiDBEnableProcedureValue   = atomic.NewBool(DefTiDBEnableProcedure)
	AutomaticSPPrivileges      = atomic.NewBool(true)
	EnableDutySeparationMode   = atomic.NewBool(DefTiDBEnableDutySeparationMode)
	EnableFastPath             = atomic.NewBool(DefTiDBXFastPath)
	EnableWhitelist            = atomic.NewBool(DefPKDBEnableWhitelist)
	EnableEAL                  = atomic.NewBool(DefPKDBEnableEAL)
)
