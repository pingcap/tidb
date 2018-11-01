package rand

// NumberGenerator defines an interface to generate numbers.
type NumberGenerator interface {
	Int63() int64
	TwoInt63() (int64, int64)
}
