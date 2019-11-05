package code_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint/code"
)

func TestNewRestorer(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&restorerSuite{path: "tmp/restore/"})

type restorerSuite struct {
	path string
}

func (s *restorerSuite) TestRestore(c *C) {
	restorer := code.NewRestorer("not-exists-path")
	err := restorer.Restore()
	c.Assert(err, ErrorMatches, `lstat not-exists-path: no such file or directory`)
}

func (s *restorerSuite) TestRestoreModification(c *C) {
	var cases = []struct {
		filepath string
		original string
		modified string
		expected string
	}{
		{
			filepath: "modified-test.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", func(val failpoint.Value) {
		fmt.Println("unit-test", val)
	})
}
`,
			modified: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
		fmt.Println("extra add line")
		fmt.Println("unit-test", val)
	}
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", func(val failpoint.Value) {
		fmt.Println("extra add line")
		fmt.Println("unit-test", val)
	})
}
`,
		},

		{
			filepath: "modified-test-2.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", func(val failpoint.Value) {
		fmt.Println("unit-test", val)
	})
}
`,
			modified: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
		fmt.Println("extra add line")
		fmt.Println("unit-test", val)
	}
	fmt.Println("extra add line2")
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", func(val failpoint.Value) {
		fmt.Println("extra add line")
		fmt.Println("unit-test", val)
	})
	fmt.Println("extra add line2")
}
`,
		},

		{
			filepath: "modified-test-3.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", func(val failpoint.Value) {
		fmt.Println("unit-test", val)
	})
}
`,
			modified: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if val, ok := failpoint.Eval(_curpkg_("failpoint-name-extra-part")); ok {
		fmt.Println("extra add line")
		fmt.Println("unit-test", val)
	}
	fmt.Println("extra add line2")
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name-extra-part", func(val failpoint.Value) {
		fmt.Println("extra add line")
		fmt.Println("unit-test", val)
	})
	fmt.Println("extra add line2")
}
`,
		},
	}

	// Create temp files
	err := os.MkdirAll(s.path, os.ModePerm)
	c.Assert(err, IsNil)
	for _, cs := range cases {
		original := filepath.Join(s.path, cs.filepath)
		err := ioutil.WriteFile(original, []byte(cs.original), os.ModePerm)
		c.Assert(err, IsNil)
	}

	// Clean all temp files
	defer func() {
		err := os.RemoveAll(s.path)
		c.Assert(err, IsNil)
	}()

	rewriter := code.NewRewriter(s.path)
	err = rewriter.Rewrite()
	c.Assert(err, IsNil)

	for _, cs := range cases {
		modified := filepath.Join(s.path, cs.filepath)
		err := ioutil.WriteFile(modified, []byte(cs.modified), os.ModePerm)
		c.Assert(err, IsNil)
	}

	// Restore workspace
	restorer := code.NewRestorer(s.path)
	err = restorer.Restore()
	c.Assert(err, IsNil)

	for _, cs := range cases {
		expected := filepath.Join(s.path, cs.filepath)
		content, err := ioutil.ReadFile(expected)
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(string(content)), Equals, strings.TrimSpace(cs.expected), filenameComment(cs.filepath))
	}
}

func (s *restorerSuite) TestRestoreModificationBad(c *C) {
	var cases = []struct {
		filepath string
		original string
		modified string
	}{
		{
			filepath: "bad-modification-test.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", func(val failpoint.Value) {
		fmt.Println("unit-test", val)
	})
}
`,
			modified: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if val, ok := failpoint.EvalContext(nil, _curpkg_("failpoint-name-extra-part")); ok {
		fmt.Println("extra add line")
		fmt.Println("unit-test", val)
	}
}
`,
		},
	}

	// Create temp files
	err := os.MkdirAll(s.path, os.ModePerm)
	c.Assert(err, IsNil)
	for _, cs := range cases {
		original := filepath.Join(s.path, cs.filepath)
		err := ioutil.WriteFile(original, []byte(cs.original), os.ModePerm)
		c.Assert(err, IsNil)
	}

	// Clean all temp files
	defer func() {
		err := os.RemoveAll(s.path)
		c.Assert(err, IsNil)
	}()

	rewriter := code.NewRewriter(s.path)
	err = rewriter.Rewrite()
	c.Assert(err, IsNil)

	for _, cs := range cases {
		modified := filepath.Join(s.path, cs.filepath)
		err := ioutil.WriteFile(modified, []byte(cs.modified), os.ModePerm)
		c.Assert(err, IsNil)
	}

	restorer := code.NewRestorer(s.path)
	err = restorer.Restore()
	c.Assert(err, NotNil)
	c.Assert(strings.HasPrefix(err.Error(), `cannot merge modifications back automatically`), IsTrue)
}
