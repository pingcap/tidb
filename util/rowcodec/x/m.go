package main

import (
	"fmt"
	"github.com/pingcap/tidb/util/rowcodec"
	"io/ioutil"
	"time"
)

func main() {
	b, err := ioutil.ReadFile("/home/robi/Downloads/value.txt")
	if err != nil {
		panic(err)
	}
	m := make(map[int64]int)
	d := rowcodec.NewByteDecoder(nil, -1, nil, time.UTC)
	dd, err := d.DecodeToBytesNoHandle(m, b)
	if err != nil {
		panic(err)
	}
	fmt.Println(dd)
}
