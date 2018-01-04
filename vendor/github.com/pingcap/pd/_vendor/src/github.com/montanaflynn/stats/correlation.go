package stats

import "math"

// Correlation describes the degree of relationship between two sets of data
func Correlation(data1, data2 Float64Data) (float64, error) {

	l1 := data1.Len()
	l2 := data2.Len()

	if l1 == 0 || l2 == 0 {
		return math.NaN(), EmptyInput
	}

	if l1 != l2 {
		return math.NaN(), SizeErr
	}

	sdev1, _ := StandardDeviationPopulation(data1)
	sdev2, _ := StandardDeviationPopulation(data2)

	if sdev1 == 0 || sdev2 == 0 {
		return 0, nil
	}

	covp, _ := CovariancePopulation(data1, data2)
	return covp / (sdev1 * sdev2), nil
}

// Pearson calculates the Pearson product-moment correlation coefficient between two variables.
func Pearson(data1, data2 Float64Data) (float64, error) {
	return Correlation(data1, data2)
}
