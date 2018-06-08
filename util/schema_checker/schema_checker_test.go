package schema_checker

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/terror"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct{}

func (s *testSuite) TestSchemaCheckerSimple(c *C) {
	lease := 5 * time.Millisecond
	validator := domain.NewSchemaValidator(lease)
	checker := &schemaLeaseChecker{SchemaValidator: validator}

	// Add some schema versions and delta table IDs.
	ts := uint64(time.Now().UnixNano())
	validator.Update(ts, 0, 2, []int64{1})
	validator.Update(ts, 2, 4, []int64{2})

	// checker's schema version is the same as the current schema version.
	checker.schemaVer = 4
	err := checker.Check(ts)
	c.Assert(err, IsNil)

	// checker's schema version is less than the current schema version, and it doesn't exist in validator's items.
	// checker's related table ID isn't in validator's changed table IDs.
	checker.schemaVer = 2
	checker.relatedTableIDs = []int64{3}
	err = checker.Check(ts)
	c.Assert(err, IsNil)
	// The checker's schema version isn't in validator's items.
	checker.schemaVer = 1
	checker.relatedTableIDs = []int64{3}
	err = checker.Check(ts)
	c.Assert(terror.ErrorEqual(err, domain.ErrInfoSchemaChanged), IsTrue)
	// checker's related table ID is in validator's changed table IDs.
	checker.relatedTableIDs = []int64{2}
	err = checker.Check(ts)
	c.Assert(terror.ErrorEqual(err, domain.ErrInfoSchemaChanged), IsTrue)

	// validator's latest schema version is expired.
	time.Sleep(lease + time.Microsecond)
	checker.schemaVer = 4
	checker.relatedTableIDs = []int64{3}
	err = checker.Check(ts)
	c.Assert(err, IsNil)
	nowTS := uint64(time.Now().UnixNano())
	// Use checker.SchemaValidator.Check instead of checker.Check here because backoff make CI slow.
	result := checker.SchemaValidator.Check(nowTS, checker.schemaVer, checker.relatedTableIDs)
	c.Assert(result, Equals, domain.ResultUnknown)
}
