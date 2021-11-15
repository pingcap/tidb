package main

import (
	"fmt"
	"github.com/pingcap/tidb/util"
	"math"
)

func main() {
	a, b :=1.1, 1.0
	fmt.Println(math.Trunc(a)==b)    // 这样可以判断浮点数是不是没有小数

	wg := util.WaitGroupWrapper{}
	wg.Run(func() {
		panic("tailingxiang panic")
	})
}
