package lightstep

import (
	"runtime"
	"time"

	"github.com/lightstep/lightstep-tracer-go/lightstep/rand"
)

var (
	// create a random pool with size equal to 16 generators or number of CPU Cores which ever is higher to spread
	// random int call loads across multiple go routines. This number is obtained via local benchmarking
	// where any number more than 16 reaches a point of diminishing return given the test scenario.
	randompool = rand.NewPool(time.Now().UnixNano(), uint64(max(16, runtime.NumCPU())))
)

// max returns the larger value among a and b
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func genSeededGUID() uint64 {
	return uint64(randompool.Pick().Int63())
}

func genSeededGUID2() (uint64, uint64) {
	n1, n2 := randompool.Pick().TwoInt63()
	return uint64(n1), uint64(n2)
}
