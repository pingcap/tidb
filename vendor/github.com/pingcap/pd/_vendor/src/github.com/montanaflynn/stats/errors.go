package stats

type statsErr struct {
	err string
}

func (s statsErr) Error() string {
	return s.err
}

// These are the package-wide error values.
// All error identification should use these values.
var (
	EmptyInput  = statsErr{"Input must not be empty."}
	SampleSize  = statsErr{"Samples number must be less than input length."}
	NaNErr      = statsErr{"Not a number"}
	NegativeErr = statsErr{"Slice must not contain negative values."}
	ZeroErr     = statsErr{"Slice must not contain zero values."}
	BoundsErr   = statsErr{"Input is outside of range."}
	SizeErr     = statsErr{"Slices must be the same length."}
)
