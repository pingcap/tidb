package errno

import (
	"os"
	"testing"

	"github.com/pingcap/tidb/util/testbridge"
)

func TestMain(m *testing.M) {
	testbridge.WorkaroundGoCheckFlags()
	os.Exit(m.Run())
}
