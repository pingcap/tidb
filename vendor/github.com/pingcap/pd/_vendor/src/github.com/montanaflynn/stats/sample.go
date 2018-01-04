package stats

import "math/rand"

// Sample returns sample from input with replacement or without
func Sample(input Float64Data, takenum int, replacement bool) ([]float64, error) {

	if input.Len() == 0 {
		return nil, EmptyInput
	}

	length := input.Len()
	if replacement {

		result := Float64Data{}
		rand.Seed(unixnano())

		// In every step, randomly take the num for
		for i := 0; i < takenum; i++ {
			idx := rand.Intn(length)
			result = append(result, input[idx])
		}

		return result, nil

	} else if !replacement && takenum <= length {

		rand.Seed(unixnano())

		// Get permutation of number of indexies
		perm := rand.Perm(length)
		result := Float64Data{}

		// Get element of input by permutated index
		for _, idx := range perm[0:takenum] {
			result = append(result, input[idx])
		}

		return result, nil

	}

	return nil, BoundsErr
}
