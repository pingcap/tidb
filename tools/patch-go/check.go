package main

import _ "unsafe"

//go:linkname grunningnanos runtime.grunningnanos
func grunningnanos() int64

func main() {
	grunningnanos()
}
