package schema_checker

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/metrics"
)

type schemaLeaseChecker struct {
	domain.SchemaValidator
	schemaVer       int64
	relatedTableIDs []int64
}

var (
	// SchemaOutOfDateRetryInterval is the sleeping time when we fail to try.
	SchemaOutOfDateRetryInterval = int64(500 * time.Millisecond)
	// SchemaOutOfDateRetryTimes is upper bound of retry times when the schema is out of date.
	SchemaOutOfDateRetryTimes = int32(10)
)

func NewSchemaChecker(do *domain.Domain, schemaVer int64, relatedTableIDs []int64) *schemaLeaseChecker {
	return &schemaLeaseChecker{
		SchemaValidator: do.SchemaValidator,
		schemaVer:       schemaVer,
		relatedTableIDs: relatedTableIDs,
	}
}

func (s *schemaLeaseChecker) Check(txnTS uint64) error {
	schemaOutOfDateRetryInterval := atomic.LoadInt64(&SchemaOutOfDateRetryInterval)
	schemaOutOfDateRetryTimes := int(atomic.LoadInt32(&SchemaOutOfDateRetryTimes))
	for i := 0; i < schemaOutOfDateRetryTimes; i++ {
		result := s.SchemaValidator.Check(txnTS, s.schemaVer, s.relatedTableIDs)
		switch result {
		case domain.ResultSucc:
			return nil
		case domain.ResultFail:
			metrics.SchemaLeaseErrorCounter.WithLabelValues("changed").Inc()
			return domain.ErrInfoSchemaChanged
		case domain.ResultUnknown:
			metrics.SchemaLeaseErrorCounter.WithLabelValues("outdated").Inc()
			time.Sleep(time.Duration(schemaOutOfDateRetryInterval))
		}

	}
	return domain.ErrInfoSchemaExpired
}
