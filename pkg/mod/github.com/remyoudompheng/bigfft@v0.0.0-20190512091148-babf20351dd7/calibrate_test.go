// Usage: go test -run=TestCalibrate -calibrate

package bigfft

import (
	"flag"
	"fmt"
	"testing"
	"time"
)

var calibrate = flag.Bool("calibrate", false, "run calibration test")

// measureMul benchmarks math/big versus FFT for a given input size
// (in bits).
func measureMul(th int) (tBig, tFFT time.Duration) {
	bigLoad := func(b *testing.B) { benchmarkMulBig(b, th, th) }
	fftLoad := func(b *testing.B) { benchmarkMulFFT(b, th, th) }

	res1 := testing.Benchmark(bigLoad)
	res2 := testing.Benchmark(fftLoad)
	tBig = time.Duration(res1.NsPerOp())
	tFFT = time.Duration(res2.NsPerOp())
	return
}

func roundDur(d time.Duration) time.Duration {
	if d > 100*time.Millisecond {
		return d / time.Millisecond * time.Millisecond
	} else {
		return d / time.Microsecond * time.Microsecond
	}
}

func TestCalibrateThreshold(t *testing.T) {
	if !*calibrate {
		t.Log("not calibrating, use -calibrate to do so.")
		return
	}

	lower := int(1e3)   // math/big is faster at this size.
	upper := int(300e3) // FFT is faster at this size.

	var sizes [9]int
	var speedups [9]float64
	for i := 0; i < 3; i++ {
		for idx := 1; idx <= 9; idx++ {
			sz := ((10-idx)*lower + idx*upper) / 10
			big, fft := measureMul(sz)
			spd := float64(big) / float64(fft)
			sizes[idx-1] = sz
			speedups[idx-1] = spd
			fmt.Printf("speedup of FFT over math/big at size %d bits: %.2f (%s vs %s)\n",
				sz, spd, roundDur(big), roundDur(fft))
		}
		narrow := false
		for idx, s := range speedups {
			if s < .98 {
				lower = sizes[idx]
				narrow = true
			} else {
				break
			}
		}
		for idx := range speedups {
			if speedups[8-idx] > 1.02 {
				upper = sizes[8-idx]
				narrow = true
			} else {
				break
			}
		}
		if lower >= upper {
			panic("impossible")
		}
		if !narrow || (upper-lower) <= 10 {
			break
		}
	}
	fmt.Printf("sizes: %d\n", sizes)
	fmt.Printf("speedups: %.2f\n", speedups)
}

func measureFFTSize(w int, k uint) time.Duration {
	load := func(b *testing.B) {
		x := rndNat(w)
		y := rndNat(w)
		for i := 0; i < b.N; i++ {
			m := (w+w)>>k + 1
			xp := polyFromNat(x, k, m)
			yp := polyFromNat(y, k, m)
			rp := xp.Mul(&yp)
			_ = rp.Int()
		}
	}
	res := testing.Benchmark(load)
	return time.Duration(res.NsPerOp())
}

func TestCalibrateFFT(t *testing.T) {
	if !*calibrate {
		t.Log("not calibrating, use -calibrate to do so.")
		return
	}

	lows := [...]int{10, 10, 10, 10,
		20, 50, 100, 200, 500, // 8
		1000, 2000, 5000, 10000, // 12
		20000, 50000, 100e3, 200e3, // 16
	}
	his := [...]int{100, 100, 100, 200,
		500, 1000, 2000, 5000, 10000, // 8
		50e3, 100e3, 200e3, 800e3, // 12
		2e6, 5e6, 10e6, 20e6, // 16
	}
	for k := uint(3); k <= 16; k++ {
		// Measure the speedup between k and k+1
		low := lows[k] // FFT of size 1<<k known to be faster
		hi := his[k]   // FFT of size 2<<k known to be faster
		var sizes [9]int
		var speedups [9]float64
		for i := 0; i < 3; i++ {
			for idx := 1; idx <= 9; idx++ {
				sz := ((10-idx)*low + idx*hi) / 10
				t1, t2 := measureFFTSize(sz, k), measureFFTSize(sz, k+1)
				spd := float64(t1) / float64(t2)
				sizes[idx-1] = sz
				speedups[idx-1] = spd
				fmt.Printf("speedup of %d vs %d at size %d words: %.2f (%s vs %s)\n",
					k+1, k, sz, spd, roundDur(t1), roundDur(t2))
			}
			narrow := false
			for idx, s := range speedups {
				if s < .98 {
					low = sizes[idx]
					narrow = true
				} else {
					break
				}
			}
			for idx := range speedups {
				if speedups[8-idx] > 1.02 {
					hi = sizes[8-idx]
					narrow = true
				} else {
					break
				}
			}
			if low >= hi {
				panic("impossible")
			}
			if !narrow || (hi-low) <= 10 {
				break
			}
		}
		fmt.Printf("sizes: %d\n", sizes)
		fmt.Printf("speedups: %.2f\n", speedups)
	}
}
