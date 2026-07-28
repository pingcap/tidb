package audit

import (
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/intest"
)

type testCleanup interface {
	Cleanup(func())
}

var globalManagerForTest *LogManager

// Register4Test registers the audit extension for tests and resets it during cleanup.
func Register4Test(t testCleanup) {
	resetForTest()
	t.Cleanup(resetForTest)
	enableAuditExtension = true
	err := extension.RegisterFactory(ExtensionName, extensionOptionsFactory)
	terror.MustNil(err)
}

func resetForTest() {
	extension.Reset()
	if intest.InTest {
		globalManagerForTest = nil
	}
}

// GlobalLogManagerForTest returns the latest registered audit log manager for tests.
func GlobalLogManagerForTest() *LogManager {
	return globalManagerForTest
}
