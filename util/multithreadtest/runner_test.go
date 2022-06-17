// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multithreadtest

import (
	"fmt"
	"testing"
)

func TestRunner(t *testing.T) {
	runner := NewThreadsRunner()
	t1 := runner.Thread("s1").Start(func(ch *Chan, args []any) []any {
		fmt.Println("s1.1")
		ch.Stop("after s1.1")
		fmt.Println("s1.2")
		ch.Stop("after s1.2")
		fmt.Println("s1.3")
		return nil
	})

	t2 := runner.Thread("s2").Start(func(ch *Chan, args []any) []any {
		fmt.Println("s2.1")
		ch.Stop("after s2.1")
		fmt.Println("s2.2")
		ch.Stop("after s2.2")
		fmt.Println("s2.3")
		return nil
	})

	t1.Step()
	t2.Step()
	t1.Step()
	t2.Step()
	t1.Step()
	t2.Step()
	fmt.Println(runner.path)
}
