// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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

type filenameComment string

func (c filenameComment) CheckCommentString() string {
	return string(c)
}

func TestNewRewriter(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&rewriterSuite{path: "tmp/rewrite/"})

type rewriterSuite struct {
	path string
}

func (s *rewriterSuite) TestRewrite(c *C) {
	var cases = []struct {
		filepath string
		original string
		expected string
	}{
		{
			filepath: "basic-test.go",
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
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
		fmt.Println("unit-test", val)
	}
}
`,
		},

		{
			filepath: "basic-test2.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", func() {
		fmt.Println("unit-test")
	})
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
		fmt.Println("unit-test")
	}
}
`,
		},

		{
			filepath: "basic-test-ignore-val.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", func(_ failpoint.Value) {
		fmt.Println("unit-test")
	})
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
		fmt.Println("unit-test")
	}
}
`,
		},

		{
			filepath: "basic-test-with-ctx.go",
			original: `
package rewriter_test

import (
	"context"
	"fmt"

	"github.com/pingcap/failpoint"
)

var ctx = context.Background()

func unittest() {
	failpoint.InjectContext(ctx, "failpoint-name", func(val failpoint.Value) {
		fmt.Println("unit-test", val)
	})
}
`,
			expected: `
package rewriter_test

import (
	"context"
	"fmt"

	"github.com/pingcap/failpoint"
)

var ctx = context.Background()

func unittest() {
	if val, ok := failpoint.EvalContext(ctx, _curpkg_("failpoint-name")); ok {
		fmt.Println("unit-test", val)
	}
}
`,
		},

		{
			filepath: "basic-test-with-ctx-ignore.go",
			original: `
package rewriter_test

import (
	"context"
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.InjectContext(nil, "failpoint-name", func(val failpoint.Value) {
		fmt.Println("unit-test", val)
	})
}
`,
			expected: `
package rewriter_test

import (
	"context"
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if val, ok := failpoint.EvalContext(nil, _curpkg_("failpoint-name")); ok {
		fmt.Println("unit-test", val)
	}
}
`,
		},

		{
			filepath: "basic-test-with-ctx-ignore-all.go",
			original: `
package rewriter_test

import (
	"context"
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.InjectContext(nil, "failpoint-name", func(_ failpoint.Value) {
		fmt.Println("unit-test")
	})
}
`,
			expected: `
package rewriter_test

import (
	"context"
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if _, ok := failpoint.EvalContext(nil, _curpkg_("failpoint-name")); ok {
		fmt.Println("unit-test")
	}
}
`,
		},

		{
			filepath: "simple-assign-with-function.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	var _, f1, f2 = 10, func() {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}, func() {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}
	f1()
	f2()
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	var _, f1, f2 = 10, func() {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}, func() {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}
	f1()
	f2()
}
`,
		},

		{
			filepath: "simple-assign-with-function-2.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	_, f1, f2 := 10, func() {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}, func() {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}
	f1()
	f2()
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	_, f1, f2 := 10, func() {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}, func() {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}
	f1()
	f2()
}
`,
		},

		{
			filepath: "simple-go-statement.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	go func() {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}()
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	go func() {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}()
}
`,
		},

		{
			filepath: "complicate-go-statement.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	go func(_ func()) {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}(func() {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	})
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	go func(_ func()) {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}(func() {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	})
}
`,
		},

		{
			filepath: "simple-defer-statement.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	defer func() {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}()
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	defer func() {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}()
}
`,
		},

		{
			filepath: "complicate-defer-statement.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	defer func(_ func()) {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}(func() {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	})
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	defer func(_ func()) {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}(func() {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	})
}
`,
		},

		{
			filepath: "return-statement.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	return func() (func(), int) {
			failpoint.Inject("failpoint-name", func(val failpoint.Value) {
				fmt.Println("unit-test", val)
			})
		}, func() int {
			failpoint.Inject("failpoint-name", func(val failpoint.Value) {
				fmt.Println("unit-test", val)
			})
			return 1000
		}()
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	return func() (func(), int) {
			if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
				fmt.Println("unit-test", val)
			}
		}, func() int {
			if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
				fmt.Println("unit-test", val)
			}
			return 1000
		}()
}
`,
		},

		{
			filepath: "if-statement.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	x := rand.Float32()
	if x > 0.5 {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	} else if x > 0.2 {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	} else {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	x := rand.Float32()
	if x > 0.5 {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	} else if x > 0.2 {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	} else {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}
}
`,
		},

		{
			filepath: "if-statement-2.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if a, b := func() {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}, func() int { return rand.Intn(200) }(); b > 100 {
		a()
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if a, b := func() {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}, func() int { return rand.Intn(200) }(); b > 100 {
		a()
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}
}
`,
		},

		{
			filepath: "if-statement-3.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if a, b := func() {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}, func() int { return rand.Intn(200) }(); b > func() int {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return rand.Intn(3000)
	}() && b < func() int {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return rand.Intn(6000)
	}() {
		a()
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if a, b := func() {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}, func() int { return rand.Intn(200) }(); b > func() int {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return rand.Intn(3000)
	}() && b < func() int {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return rand.Intn(6000)
	}() {
		a()
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}
}
`,
		},

		{
			filepath: "if-statement-4.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

const success = 200

func unittest() {
	var i int
	if func(v *int) {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		*v = success
	}(&i); i == success {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		fmt.Printf("i = %d success\n", i)
	}
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

const success = 200

func unittest() {
	var i int
	if func(v *int) {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		*v = success
	}(&i); i == success {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		fmt.Printf("i = %d success\n", i)
	}
}
`,
		},

		{
			filepath: "switch-statement.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	switch x, y := rand.Intn(10), func() int { return rand.Intn(1000) }(); x - y + func() int {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return rand.Intn(50)
	}() {
	case func() int {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return rand.Intn(5)
	}(), func() int {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return rand.Intn(8)
	}():
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	default:
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	switch x, y := rand.Intn(10), func() int { return rand.Intn(1000) }(); x - y + func() int {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return rand.Intn(50)
	}() {
	case func() int {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return rand.Intn(5)
	}(), func() int {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return rand.Intn(8)
	}():
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	default:
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}
}
`,
		},

		{
			filepath: "switch-statement-2.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	switch x, y := rand.Intn(10), func() int {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return rand.Intn(1000)
	}(); func(x, y int) int {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return rand.Intn(50) + x + y
	}(x, y) {
	case func() int {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return rand.Intn(5)
	}(), func() int {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return rand.Intn(8)
	}():
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	default:
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		fn := func() {
			failpoint.Inject("failpoint-name", func(val failpoint.Value) {
				fmt.Println("unit-test", val)
			})
		}
		fn()
	}
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	switch x, y := rand.Intn(10), func() int {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return rand.Intn(1000)
	}(); func(x, y int) int {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return rand.Intn(50) + x + y
	}(x, y) {
	case func() int {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return rand.Intn(5)
	}(), func() int {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return rand.Intn(8)
	}():
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	default:
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		fn := func() {
			if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
				fmt.Println("unit-test", val)
			}
		}
		fn()
	}
}
`,
		},

		{
			filepath: "type-switch-statement.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	typeSwitch := func(i interface{}) {
		inner := func(i interface{}) interface{} {
			failpoint.Inject("failpoint-name", func(val failpoint.Value) {
				fmt.Println("unit-test", val)
			})
			return i
		}
		switch t := inner(i).(type) {
		case int:
			failpoint.Inject("failpoint-name", func(val failpoint.Value) {
				fmt.Println("unit-test", val)
			})
			fmt.Println("int type")
		default:
			failpoint.Inject("failpoint-name", func(val failpoint.Value) {
				fmt.Println("unit-test", val)
			})
			fmt.Printf("unsupported type %T\n", t)
		}
	}

	typeSwitch2 := func(i interface{}) {
		switch i.(type) {
		case int:
			failpoint.Inject("failpoint-name", func(val failpoint.Value) {
				fmt.Println("unit-test", val)
			})
			fmt.Println("int type")
		}
	}

	typeSwitch3 := func(i interface{}) {
		switch func(inf interface{}){
			failpoint.Inject("failpoint-name", func(val failpoint.Value) {
				fmt.Println("unit-test", val)
			})
			return inf
		}(i).(type) {
		case int:
			failpoint.Inject("failpoint-name", func(val failpoint.Value) {
				fmt.Println("unit-test", val)
			})
			fmt.Println("int type")
		}
	}

	num := 42
	typeSwitch(num)
	typeSwitch2(num)
	typeSwitch3(num)
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	typeSwitch := func(i interface{}) {
		inner := func(i interface{}) interface{} {
			if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
				fmt.Println("unit-test", val)
			}
			return i
		}
		switch t := inner(i).(type) {
		case int:
			if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
				fmt.Println("unit-test", val)
			}
			fmt.Println("int type")
		default:
			if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
				fmt.Println("unit-test", val)
			}
			fmt.Printf("unsupported type %T\n", t)
		}
	}

	typeSwitch2 := func(i interface{}) {
		switch i.(type) {
		case int:
			if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
				fmt.Println("unit-test", val)
			}
			fmt.Println("int type")
		}
	}

	typeSwitch3 := func(i interface{}) {
		switch func(inf interface{}) {
			if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
				fmt.Println("unit-test", val)
			}
			return inf
		}(i).(type) {
		case int:
			if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
				fmt.Println("unit-test", val)
			}
			fmt.Println("int type")
		}
	}

	num := 42
	typeSwitch(num)
	typeSwitch2(num)
	typeSwitch3(num)
}
`,
		},

		{
			filepath: "select-statement.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	select {
	case ch := <-func() chan bool {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return make(chan bool)
	}():
		fmt.Println(ch)
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})

	case <-func() chan bool {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return make(chan bool)
	}():
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})

	case <-func() chan bool {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return make(chan bool)
	}():
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	default:
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	select {
	case ch := <-func() chan bool {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return make(chan bool)
	}():
		fmt.Println(ch)
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}

	case <-func() chan bool {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return make(chan bool)
	}():
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}

	case <-func() chan bool {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return make(chan bool)
	}():
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	default:
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}
}
`,
		},

		{
			filepath: "for-statement.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	for i := func() int {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return rand.Intn(100)
	}(); i < func() int {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return rand.Intn(10000)
	}(); i += func() int {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return rand.Intn(100)
	}() {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	for i := func() int {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return rand.Intn(100)
	}(); i < func() int {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return rand.Intn(10000)
	}(); i += func() int {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return rand.Intn(100)
	}() {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}
}
`,
		},

		{
			filepath: "for-statement2.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

type Iterator struct {
	count int
	max   int
}

func (i *Iterator) Begin(fn func()) int {
	i.count = 0
	return i.count
}

func (i *Iterator) Next(fn func()) int {
	if i.count >= i.max {
		panic("iterator to end")
	}
	i.count++
	return i.count
}

func (i *Iterator) End(fn func()) bool {
	return i.count == i.max
}

func unittest() {
	iter := &Iterator{max: 10}
	for iter.Begin(func() {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}); !iter.End(func() {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
	}); {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		i := iter.Next(func() {})
		fmt.Printf("get value: %d\n", i)
	}
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

type Iterator struct {
	count	int
	max	int
}

func (i *Iterator) Begin(fn func()) int {
	i.count = 0
	return i.count
}

func (i *Iterator) Next(fn func()) int {
	if i.count >= i.max {
		panic("iterator to end")
	}
	i.count++
	return i.count
}

func (i *Iterator) End(fn func()) bool {
	return i.count == i.max
}

func unittest() {
	iter := &Iterator{max: 10}
	for iter.Begin(func() {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}); !iter.End(func() {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
	}); {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		i := iter.Next(func() {})
		fmt.Printf("get value: %d\n", i)
	}
}
`,
		},

		{
			filepath: "range-statement.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	for x, y := range func() map[int]int {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		return make(map[int]int, rand.Intn(10))
	}() {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) {
			fmt.Println("unit-test", val)
		})
		fn := func() {
			failpoint.Inject("failpoint-name", func(val failpoint.Value) {
				fmt.Println("unit-test", val, x, y)
			})
		}
		fn()
	}
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	for x, y := range func() map[int]int {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		return make(map[int]int, rand.Intn(10))
	}() {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test", val)
		}
		fn := func() {
			if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
				fmt.Println("unit-test", val, x, y)
			}
		}
		fn()
	}
}
`,
		},

		{
			filepath: "control-flow-statement.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Label("outer")
	for i := 0; i < 100; i++ {
		failpoint.Label("inner")
		for j := 0; j < 1000; j++ {
			switch rand.Intn(j) + i {
			case j / 3:
				failpoint.Break("inner")
			case j / 4:
				failpoint.Break("outer")
			case j / 5:
				failpoint.Break()
			case j / 6:
				failpoint.Continue("inner")
			case j / 7:
				failpoint.Continue("outer")
			case j / 8:
				failpoint.Continue()
			case j / 9:
				failpoint.Fallthrough()
			case j / 10:
				failpoint.Goto("outer")
			default:
				failpoint.Inject("failpoint-name", func(val failpoint.Value) {
					fmt.Println("unit-test", val.(int))
					if val == j/11 {
						failpoint.Goto("inner")
					} else {
						failpoint.Goto("outer")
					}
				})
			}
		}
	}
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
outer:
	for i := 0; i < 100; i++ {
	inner:
		for j := 0; j < 1000; j++ {
			switch rand.Intn(j) + i {
			case j / 3:
				break inner
			case j / 4:
				break outer
			case j / 5:
				break
			case j / 6:
				continue inner
			case j / 7:
				continue outer
			case j / 8:
				continue
			case j / 9:
				fallthrough
			case j / 10:
				goto outer
			default:
				if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
					fmt.Println("unit-test", val.(int))
					if val == j/11 {
						goto inner
					} else {
						goto outer
					}
				}
			}
		}
	}
}
`,
		},

		{
			filepath: "test-block-statement.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	{
		failpoint.Inject("failpoint-name", func() {
			fmt.Println("unit-test")
		})
	}
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	{
		if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			fmt.Println("unit-test")
		}
	}
}
`,
		},

		{
			filepath: "test-send-statement.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	func() chan struct{} {
		failpoint.Inject("failpoint-name", func() chan struct{}{
			return make(chan struct{}, 1)
		})
		return make(chan struct{}, 1)
	}() <- func() struct{} {
		failpoint.Inject("failpoint-name", func() struct{} {
			return struct{}{}
		})
		return struct{}{}
	}()
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	func() chan struct{} {
		if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			return make(chan struct{}, 1)
		}
		return make(chan struct{}, 1)
	}() <- func() struct{} {
		if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			return struct{}{}
		}
		return struct{}{}
	}()
}
`,
		},

		{
			filepath: "test-label-statement.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	labelSend:
	func() chan struct{} {
		failpoint.Inject("failpoint-name", func() chan struct{} {
			return make(chan struct{}, 1)
		})
		return make(chan struct{}, 1)
	}() <- func() struct{} {
		failpoint.Inject("failpoint-name", func() struct{} {
			return struct{}{}
		})
		return struct{}{}
	}()
	if rand.Intn(10) > 5 {
		goto labelSend
	}

	switch rand.Intn(10) {
	case 1:
		goto labelFor
	case 2:
		goto labelCall
	}

labelFor:
	for i := range []int{10, 20} {
		if i%rand.Intn(2) == i {
		labelIf:
			if rand.Intn(1000) > 500 {
				failpoint.Inject("failpoint-name", func() {
					fmt.Println("output in failpoint")
				})
				goto labelIf
			}
			goto labelFor
		}
		if rand.Intn(20) > 10 {
			goto labelBreak
		}
	labelBreak:
		break
	}

labelCall:
	failpoint.Inject("failpoint-name", func() {
		fmt.Println("output in failpoint")
	})
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
labelSend:
	func() chan struct{} {
		if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			return make(chan struct{}, 1)
		}
		return make(chan struct{}, 1)
	}() <- func() struct{} {
		if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			return struct{}{}
		}
		return struct{}{}
	}()
	if rand.Intn(10) > 5 {
		goto labelSend
	}

	switch rand.Intn(10) {
	case 1:
		goto labelFor
	case 2:
		goto labelCall
	}

labelFor:
	for i := range []int{10, 20} {
		if i%rand.Intn(2) == i {
		labelIf:
			if rand.Intn(1000) > 500 {
				if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
					fmt.Println("output in failpoint")
				}
				goto labelIf
			}
			goto labelFor
		}
		if rand.Intn(20) > 10 {
			goto labelBreak
		}
	labelBreak:
		break
	}

labelCall:
	if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
		fmt.Println("output in failpoint")
	}
}
`,
		},

		{
			filepath: "test-index-expression.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	x := func() []int {
		failpoint.Inject("failpoint-name", func() []int {
			return make([]int, 1)
		})
		return make([]int, 10)
	}()[func() int {
		failpoint.Inject("failpoint-name", func() int {
			return rand.Intn(1)
		})
		return rand.Intn(10)
	}()]
	fmt.Println(x)
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	x := func() []int {
		if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			return make([]int, 1)
		}
		return make([]int, 10)
	}()[func() int {
		if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			return rand.Intn(1)
		}
		return rand.Intn(10)
	}()]
	fmt.Println(x)
}
`,
		},

		{
			filepath: "test-slice-expression.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	x := func() []int {
		failpoint.Inject("failpoint-name", func() []int {
			return make([]int, 1)
		})
		return make([]int, 10)
	}()[func() int {
		failpoint.Inject("failpoint-name", func() int {
			return rand.Intn(1)
		})
		return rand.Intn(10)
	}():func() int {
		failpoint.Inject("failpoint-name", func() int {
			return rand.Intn(1)
		})
		return rand.Intn(10)
	}()]
	fmt.Println(x)
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	x := func() []int {
		if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			return make([]int, 1)
		}
		return make([]int, 10)
	}()[func() int {
		if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			return rand.Intn(1)
		}
		return rand.Intn(10)
	}():func() int {
		if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			return rand.Intn(1)
		}
		return rand.Intn(10)
	}()]
	fmt.Println(x)
}
`,
		},

		{
			filepath: "test-star-expression.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	type X struct {
		A string
	}
	x := *func() *X {
		failpoint.Inject("failpoint-name", func() *X {
			return &X{A: "from-failpoint"}
		})
		return &X{A: "normal path"}
	}()
	fmt.Println(x.A)
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	type X struct {
		A string
	}
	x := *func() *X {
		if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			return &X{A: "from-failpoint"}
		}
		return &X{A: "normal path"}
	}()
	fmt.Println(x.A)
}
`,
		},

		{
			filepath: "test-kv-expression.go",
			original: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	type X struct {
		A string
	}
	x := X{
		A: func() string {
			failpoint.Inject("failpoint-name", func() string {
				return "from-failpoint"
			})
			return "from-normal-path"
		}(),
	}
	fmt.Println(x.A)
}
`,
			expected: `
package rewriter_test

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/failpoint"
)

func unittest() {
	type X struct {
		A string
	}
	x := X{
		A: func() string {
			if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
				return "from-failpoint"
			}
			return "from-normal-path"
		}(),
	}
	fmt.Println(x.A)
}
`,
		},

		{
			filepath: "test-nil-closure.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", nil)
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Eval(_curpkg_("failpoint-name"))
}
`,
		},

		{
			filepath: "test-nil-closure-ctx.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.InjectContext(nil, "failpoint-name", nil)
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.EvalContext(nil, _curpkg_("failpoint-name"))
}
`,
		},

		{
			filepath: "test-return-marker.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() (int, int, error) {
	failpoint.Inject("failpoint-name", func() {
		failpoint.Return(123, 456, errors.New("something"))
	})
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() (int, int, error) {
	if _, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
		return 123, 456, errors.New("something")
	}
}
`,
		},

		{
			filepath: "test-return-marker-with-value.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() (int, int, error) {
	failpoint.Inject("failpoint-name", func(val failpoint.Value) {
		failpoint.Return(val.(int), 456, errors.New("something"))
	})
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() (int, int, error) {
	if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
		return val.(int), 456, errors.New("something")
	}
}
`,
		},

		{
			filepath: "test-inc-dec-statement.go",
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	type X struct {
		Y int
	}
	func() *X {
		failpoint.Inject("failpoint-name", func(val failpoint.Value) *X {
			return &X{Y: val.(int)}
		})
		return &X{Y: 100}
	}().Y++
}
`,
			expected: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	type X struct {
		Y int
	}
	func() *X {
		if val, ok := failpoint.Eval(_curpkg_("failpoint-name")); ok {
			return &X{Y: val.(int)}
		}
		return &X{Y: 100}
	}().Y++
}
`,
		},

		{
			filepath: "test-empty-body.go",
			original: `
package rewriter_test

import (
	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", func() {})
	failpoint.Inject("failpoint-name", nil)
}
`,
			expected: `
package rewriter_test

import (
	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Eval(_curpkg_("failpoint-name"))
	failpoint.Eval(_curpkg_("failpoint-name"))
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
		expected := filepath.Join(s.path, cs.filepath)
		content, err := ioutil.ReadFile(expected)
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(string(content)), Equals, strings.TrimSpace(cs.expected), filenameComment(cs.filepath))
	}

	// Restore workspace
	restorer := code.NewRestorer(s.path)
	err = restorer.Restore()
	c.Assert(err, IsNil)

	for _, cs := range cases {
		original := filepath.Join(s.path, cs.filepath)
		content, err := ioutil.ReadFile(original)
		c.Assert(err, IsNil)
		c.Assert(string(content), Equals, cs.original)
	}
}

func (s *rewriterSuite) TestRewriteBad(c *C) {
	var cases = []struct {
		filepath string
		errormsg string
		original string
	}{

		{
			filepath: "bad-basic-test.go",
			errormsg: `failpoint\.Inject: invalid signature.*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", func(val int) {
		fmt.Println("unit-test", val)
	})
}
`,
		},

		{
			filepath: "bad-basic-test2.go",
			errormsg: `failpoint\.Inject: closure signature illegal .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", func(ctx context.Context, val int) {
		fmt.Println("unit-test", val)
	})
}
`,
		},

		{
			filepath: "bad-basic-test3.go",
			errormsg: `failpoint\.Inject: closure signature illegal .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", func(ctx context.Context, val int, val2 string) {
		fmt.Println("unit-test", val)
	})
}
`,
		},

		{
			filepath: "bad-basic-test4.go",
			errormsg: `failpoint\.Inject: expect 2 arguments but got 3.*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", "invalid string", func(ctx context.Context, val int, val2 string) {
		fmt.Println("unit-test", val)
	})
}
`,
		},

		{
			filepath: "bad-basic-test5.go",
			errormsg: `failpoint\.Inject: closure signature illegal .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", func(val int, val2 int) {
		fmt.Println("unit-test", val)
	})
}
`,
		},

		{
			filepath: "bad-basic-test5-1.go",
			errormsg: `failpoint\.Inject: closure signature illegal .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", func(val, val2 int) {
		fmt.Println("unit-test", val)
	})
}
`,
		},

		{
			filepath: "bad-basic-test6.go",
			errormsg: `failpoint\.Inject: first argument expect string literal in.*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject(func(val int, val2 int) {
		fmt.Println("unit-test", val)
	}, "failpoint-name")
}
`,
		},

		{
			filepath: "bad-basic-test7.go",
			errormsg: `failpoint\.Inject: second argument expect closure in.*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Inject("failpoint-name", "failpoint-name")
}
`,
		},

		{
			filepath: "bad-basic-ctx-test1.go",
			errormsg: `failpoint\.InjectContext: expect 3 arguments but got 4.*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.InjectContext("failpoint-name", nil, nil, func(val int, val2 int) {
		fmt.Println("unit-test", val)
	})
}
`,
		},

		{
			filepath: "bad-basic-ctx-test2.go",
			errormsg: `failpoint\.InjectContext: first argument expect context in.*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.InjectContext(func(){}, nil, func(val int, val2 int) {
		fmt.Println("unit-test", val)
	})
}
`,
		},

		{
			filepath: "bad-basic-ctx-test3.go",
			errormsg: `failpoint\.InjectContext: second argument expect string literal in.*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.InjectContext(nil, func(){}, func(val int, val2 int) {
		fmt.Println("unit-test", val)
	})
}
`,
		},

		{
			filepath: "bad-basic-ctx-test4.go",
			errormsg: `failpoint\.InjectContext: third argument expect closure in.*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.InjectContext(nil, "failpoint-name", "string literal")
}
`,
		},

		{
			filepath: "bad-basic-ctx-test5.go",
			errormsg: `failpoint\.InjectContext: closure signature illegal.*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.InjectContext(nil, "failpoint-name", func(val int, val int) {})
}
`,
		},

		{
			filepath: "bad-basic-ctx-test6.go",
			errormsg: `failpoint\.InjectContext: closure signature illegal.*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.InjectContext(nil, "failpoint-name", func(val, val int) {})
}
`,
		},

		{
			filepath: "bad-case-break.go",
			errormsg: `failpoint\.Break expect 1 or 0 arguments, but got.*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Break("11", "22")
}
`,
		},

		{
			filepath: "bad-case-continue.go",
			errormsg: `failpoint\.Continue expect 1 or 0 arguments, but got.*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Continue("11", "22")
}
`,
		},

		{
			filepath: "bad-case-label.go",
			errormsg: `failpoint\.Label expect 1 arguments, but got.*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Label("11", "22")
}
`,
		},

		{
			filepath: "bad-case-goto.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	failpoint.Goto("11", "22")
}
`,
		},

		{
			filepath: "bad-case-func-literal.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	x = func() {
		failpoint.Goto("11", "22")
	}
	x()
}
`,
		},

		{
			filepath: "bad-case-if-stmt-init.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if x := func() int {
		failpoint.Goto("11", "22")
		return 10
	}; x() > rand.Intn(20) {
		failpoint.Goto("11", "22")
	}
}
`,
		},

		{
			filepath: "bad-case-if-stmt-cond.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if x := rand.Intn(20); x > func() int {
		failpoint.Goto("11", "22")
		return 10
	}() {
		failpoint.Goto("11", "22")
	}
}
`,
		},

		{
			filepath: "bad-case-if-stmt-body.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	if x := rand.Intn(20); x > 10 {
		failpoint.Goto("11", "22")
	}
}
`,
		},

		{
			filepath: "bad-IndexExpr.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	_ := func() []int {
		failpoint.Goto("11", "22")
		return []int{1}
	}()[0]
}
`,
		},

		{
			filepath: "bad-rewriteExpr-SliceExpr-Low.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	_ := []int{1,2,3}[func() int {
		failpoint.Goto("11", "22")
		return 0
	}():1]
}
`,
		},

		{
			filepath: "bad-rewriteExpr-SliceExpr-High.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	_ := []int{1,2,3}[0:func() int {
		failpoint.Goto("11", "22")
		return 1
	}()]
}
`,
		},

		{
			filepath: "bad-rewriteExpr-SliceExpr-Max.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	arr := []int{1,2,3}
	_ := arr[0:1:func() int {
		failpoint.Goto("11", "22")
		return 2
	}()]
}
`,
		},

		{
			filepath: "bad-rewriteExpr-CompositeLit-Elt.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	_ := []int{func() int {
		failpoint.Goto("11", "22")
		return 1
	}()}
}
`,
		},

		{
			filepath: "bad-rewriteExpr-CallExpr-Fun.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	return func() {
		failpoint.Goto("11", "22")
	}()
}
`,
		},

		{
			filepath: "bad-rewriteExpr-CallExpr-Args.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	return func(_ func()) {} (func() {
		failpoint.Goto("11", "22")
	})
}
`,
		},

		{
			filepath: "bad-rewriteExpr-BinaryExpr-X.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	_ := func() bool {
		failpoint.Goto("11", "22")
		return true
	}() && true
}
`,
		},

		{
			filepath: "bad-rewriteExpr-ParenExpr.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	_ := (func () {
		failpoint.Goto("11", "22")
	}())
}
`,
		},

		{
			filepath: "bad-rewriteExprs.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() int {
	return func() int {
		failpoint.Goto("11", "22")
		return 1
	}()
}
`,
		},

		{
			filepath: "bad-rewriteStmts.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	var a, b = 1, func() {
		failpoint.Goto("11", "22")
	}
	_, _ = a, b
}
`,
		},

		{
			filepath: "bad-rewriteStmts-GoStmt.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	go func() {
		failpoint.Goto("11", "22")
	}()
}
`,
		},

		{
			filepath: "bad-rewriteStmts-DeferStmt.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	defer func() {
		failpoint.Goto("11", "22")
	}()
}
`,
		},

		{
			filepath: "bad-rewriteStmts-BlockStmt.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	{
		failpoint.Goto("11", "22")
	}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-CaseClause-List.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	switch rand.Intn(10) {
	case (func () int {
		failpoint.Goto("11", "22")
		return 1
	}()):
		return
	}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-CaseClause-Body.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	switch rand.Intn(10) {
	case 1:
		failpoint.Goto("11", "22")
	}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-SwitchStmt-Init.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	switch a := (func () {
		failpoint.Goto("11", "22")
	}()); {
	case 1:
	}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-SwitchStmt-Tag.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	switch (func () {
		failpoint.Goto("11", "22")
	}()) {
	case 1:
	}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-CommClause-AssignStmt.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	select {
	case ch := func () chan bool {
		failpoint.Goto("11", "22")
		return make(chan bool, 1)
	}():
	}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-CommClause-ExprStmt.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	select {
	case <- func () chan bool {
		failpoint.Goto("11", "22")
		return make(chan bool, 1)
	}():
	}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-CommClause-Body.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	select {
	case <- ch:
		failpoint.Goto("11", "22")
	}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-SelectStmt-empty.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	select {}
	failpoint.Goto("11", "22")
}
`,
		},

		{
			filepath: "bad-rewriteStmts-ForStmt-Init.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	for i := (func () {
		failpoint.Goto("11", "22")
	}());; {}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-ForStmt-Cond.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	for i < (func () {
		failpoint.Goto("11", "22")
	}()) {}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-ForStmt-Post.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	for ;;i+=(func () {
		failpoint.Goto("11", "22")
	}()) {}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-ForStmt-Body.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	for {
		failpoint.Goto("11", "22")
	}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-RangeStmt-X.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	for x := range (func () {
		failpoint.Goto("11", "22")
	}()) {}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-RangeStmt-Body.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	for x := range y {
		failpoint.Goto("11", "22")
	}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-TypeSwitchStmt-AssignStmt.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	switch x := (func () {
		failpoint.Goto("11", "22")
	}()).(type) {}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-TypeSwitchStmt-ExprStmt.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	switch (func () {
		failpoint.Goto("11", "22")
	}()).(type) {}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-TypeSwitchStmt-Body.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	switch y.(type) {
	case x:
		failpoint.Goto("11", "22")
	}
}
`,
		},

		{
			filepath: "bad-rewriteStmts-SendStmt.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
	a <- func () {
		failpoint.Goto("11", "22")
	}()
}
`,
		},

		{
			filepath: "bad-rewriteStmts-LabeledStmt.go",
			errormsg: `failpoint\.Goto expect 1 arguments, but got .*`,
			original: `
package rewriter_test

import (
	"fmt"

	"github.com/pingcap/failpoint"
)

func unittest() {
label:
	failpoint.Goto("11", "22")
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

	// Workspace should keep clean if some error occurred
	for _, cs := range cases {
		original := filepath.Join(s.path, cs.filepath)
		rewriter := code.NewRewriter(original)
		err = rewriter.Rewrite()
		c.Assert(err, ErrorMatches, cs.errormsg, filenameComment(cs.filepath))
		content, err := ioutil.ReadFile(original)
		c.Assert(err, IsNil)
		c.Assert(string(content), Equals, cs.original, filenameComment(cs.filepath))
	}
}
