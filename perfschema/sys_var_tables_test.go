package perfschema

import (
	"testing"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func (*testSuite) TestSessionStatus(c *C) {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open("memory")
	c.Assert(err, IsNil)

	ctx := mock.NewContext()
	ctx.Store = store
	handle := newPerfHandle()

	names := []string{TableSessionStatus, TableGlobalStatus}
	for _, tableName := range names {
		tb, _ := handle.GetTable(tableName)
		c.Assert(tb, NotNil)

		sessionStatusHandle := createSysVarHandle(tableName)
		rows, err := sessionStatusHandle.GetRows(ctx, tb.Cols())
		c.Assert(err, IsNil)

		c.Assert(findSpecialStatus(rows, "Ssl_cipher"), IsNil)
	}
}

func findSpecialStatus(rows [][]types.Datum, name string) error {
	err := errors.New("cant find Ssl_cipher.")
	for _, row := range rows {
		statusNames, _ := row[0].ToString()
		if statusNames == name {
			err = nil
			break
		}
	}

	return err
}
