package validatorapi

import "github.com/tikv/client-go/v2/txnkv/transaction"

// Result represents the result of info schema validation.
type Result int

const (
	// ResultSucc means schemaValidator's check is passing.
	ResultSucc Result = iota
	// ResultFail means schemaValidator's check is fail.
	ResultFail
	// ResultUnknown means schemaValidator doesn't know the check would be success or fail.
	ResultUnknown
)

// Validator is the interface for checking the validity of schema version.
type Validator interface {
	// Update the schema validator, add a new item, delete the expired deltaSchemaInfos.
	// The latest schemaVer is valid within leaseGrantTime plus lease duration.
	// Add the changed table IDs to the new schema information,
	// which is produced when the oldSchemaVer is updated to the newSchemaVer.
	Update(leaseGrantTime uint64, oldSchemaVer, newSchemaVer int64, change *transaction.RelatedSchemaChange)
	// Check is it valid for a transaction to use schemaVer and related tables, at timestamp txnTS.
	Check(txnTS uint64, schemaVer int64, relatedPhysicalTableIDs []int64, needCheckSchema bool) (*transaction.RelatedSchemaChange, Result)
	// Stop stops checking the valid of transaction.
	Stop()
	// Restart restarts the schema validator after it is stopped.
	Restart(currSchemaVer int64)
	// Reset resets Validator to initial state.
	Reset()
	// IsStarted indicates whether Validator is started.
	IsStarted() bool
	// IsLeaseExpired checks whether the current lease has expired
	IsLeaseExpired() bool
}
