//revive:disable:duplicated-imports Synthetic aliases exercise AST fixture-access discovery.
package fixture

import (
	"os"
	. "os"
	stdos "os"
	"path/filepath"
	"testing"
)

// os.ReadFile("comment-only.fixture")
var commentOnly = "os.Open(\"string-only.fixture\")"

//go:embed literal.fixture
var embeddedLiteral string

//go:embed testdata/*.fixture
var embeddedPattern string

func TestFixtureAccesses(t *testing.T) {
	_, _ = os.ReadFile("literal.fixture")
	_, _ = os.Open("open.fixture")
	_, _ = os.OpenFile("open-file.fixture", os.O_RDONLY, 0)
	_, _ = os.Stat("stat.fixture")
	_, _ = os.ReadDir("dir.fixture")
	_, _ = stdos.ReadFile("aliased.fixture")
	_, _ = ReadFile("dot-import.fixture")
	_, _ = os.ReadFile(filepath.Join("testdata", "joined.fixture"))
	_, _ = os.ReadFile(dynamicFixturePath())
	_, _ = os.ReadFile("../../../outside.fixture")
}

func dynamicFixturePath() string { return "helper.fixture" }
