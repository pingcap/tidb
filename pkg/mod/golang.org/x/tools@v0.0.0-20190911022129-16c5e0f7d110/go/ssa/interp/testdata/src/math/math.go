package math

func NaN() float64

func Inf(int) float64

func IsNaN(float64) bool

func Float64bits(float64) uint64

func Signbit(x float64) bool {
	return Float64bits(x)&(1<<63) != 0
}
