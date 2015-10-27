package optimizer

// Aggregator is the interface to
// compute aggregate function result.
type Aggregator interface {
	// Input add a input value to aggregator.
	// The input values are accumulated in the aggregator.
	Input(in []interface{}) error
	// Output use input values to compute the aggregated result.
	Output() interface{}
	// Clear clears the input values.
	Clear()
}
