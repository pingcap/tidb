package window

import "golang.org/x/exp/constraints"

// Sum the values within the window.
func Sum[T constraints.Integer | constraints.Float](iterator Iterator[T]) T {
	var result T
	for iterator.Next() {
		bucket := iterator.Bucket()
		for _, p := range bucket.Points {
			result = result + p
		}
	}
	return result
}

// Avg the values within the window.
func Avg[T constraints.Integer | constraints.Float](iterator Iterator[T]) T {
	var result T
	var count T
	for iterator.Next() {
		bucket := iterator.Bucket()
		for _, p := range bucket.Points {
			result = result + p
			count = count + 1
		}
	}
	return result / count
}

// Min the values within the window.
func Min[T constraints.Integer | constraints.Float](iterator Iterator[T]) T {
	var result T
	var started = false
	for iterator.Next() {
		bucket := iterator.Bucket()
		for _, p := range bucket.Points {
			if !started {
				result = p
				started = true
				continue
			}
			if p < result {
				result = p
			}
		}
	}
	return result
}

// Max the values within the window.
func Max[T constraints.Integer | constraints.Float](iterator Iterator[T]) T {
	var result T
	var started = false
	for iterator.Next() {
		bucket := iterator.Bucket()
		for _, p := range bucket.Points {
			if !started {
				result = p
				started = true
				continue
			}
			if p > result {
				result = p
			}
		}
	}
	return result
}

// Count sums the count value within the window.
func Count[T constraints.Integer | constraints.Float](iterator Iterator[T]) int64 {
	var result int64
	for iterator.Next() {
		bucket := iterator.Bucket()
		result += bucket.Count
	}
	return result
}
