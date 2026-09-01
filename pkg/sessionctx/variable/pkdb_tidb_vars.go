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
	// TiDBImportIntoPrecheckConcurrency controls the worker concurrency of IMPORT INTO conflict precheck.
	TiDBImportIntoPrecheckConcurrency = "tidb_import_into_precheck_concurrency"
	// TiDBXEnableTiKVLocalCall indicates whether to enable TiKV local calls.
	TiDBXEnableTiKVLocalCall = "tidbx_enable_tikv_local_call"
	// TiDBXEnablePDLocalCall indicates whether to use Inter-Process Call for PD.
	TiDBXEnablePDLocalCall = "tidbx_enable_pd_local_call"
	// PKDBEnableWhitelist indicates whether to enable the whitelist feature.
	PKDBEnableWhitelist = "pkdb_whitelist"
	// PKDBExtraDataType indicates whether to enable extra data types.
	PKDBExtraDataType = "pkdb_extra_data_type"
	// PKDBEnableEAL indicates whether to enable the EAL feature.
	PKDBEnableEAL = "pkdb_eal"
	// PKDBCommentByteLengthLimit controls the byte-length limit override for table and column comments.
	// Value 0 means keeping the built-in MySQL-compatible baseline.
	PKDBCommentByteLengthLimit = "pkdb_comment_byte_length_limit"
	// TiDBEnableLBAC is used to enable or disable LBAC enforcement on TiDB.
	TiDBEnableLBAC = "pkdb_lbac"
	// PKDBSequenceCacheEpoch is the global invalidation epoch for sequence cache.
	PKDBSequenceCacheEpoch = "pkdb_sequence_cache_epoch"
	// TiDBSlowLogEscapeNewline controls whether slow logs escape SQL line breaks as literal `\n`.
	TiDBSlowLogEscapeNewline = "tidb_slow_log_escape_newline"
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
	DefPKDBEnableWhitelist             = false
	DefPKDBExtraDataType               = false
	DefPKDBEnableEAL                   = false
	DefPKDBCommentByteLengthLimit      = int64(0)

	DefTiDBImportIntoPrecheckConcurrency = 8
	DefTiDBSlowLogEscapeNewline          = false

	// fusion mode related variables
	DefPKDBSequenceCacheEpoch = uint64(0)
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
	EnableWhitelist            = atomic.NewBool(DefPKDBEnableWhitelist)
	EnableEAL                  = atomic.NewBool(DefPKDBEnableEAL)
	CommentByteLengthLimit     = atomic.NewInt64(DefPKDBCommentByteLengthLimit)
	SlowLogEscapeNewline       = atomic.NewBool(DefTiDBSlowLogEscapeNewline)

	sequenceCacheRuntimeEpoch = atomic.NewUint64(DefPKDBSequenceCacheEpoch)
)

// SetSequenceCacheEpoch updates the process-local sequence cache invalidation epoch.
// Epoch normally only moves forward. Setting it to 0 is reserved for manual maintenance reset.
func SetSequenceCacheEpoch(epoch uint64) {
	if epoch == DefPKDBSequenceCacheEpoch {
		sequenceCacheRuntimeEpoch.Store(epoch)
		return
	}
	for current := sequenceCacheRuntimeEpoch.Load(); epoch > current; current = sequenceCacheRuntimeEpoch.Load() {
		if sequenceCacheRuntimeEpoch.CAS(current, epoch) {
			return
		}
	}
}

// CurrentSequenceCacheEpoch returns the current epoch for logical sequence cache invalidation.
func CurrentSequenceCacheEpoch() uint64 {
	return sequenceCacheRuntimeEpoch.Load()
}
