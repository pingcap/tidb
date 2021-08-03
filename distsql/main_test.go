package distsql

import (
	"github.com/pingcap/tidb/util/testbridge"
	"go.uber.org/goleak"
	"testing"
)

func TestMain(m *testing.M) {
	testbridge.WorkaroundGoCheckFlags()
	goleak.VerifyTestMain(m)
}
