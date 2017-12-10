package stats

import (
	"strconv"
	"time"
)

// LoadRawData parses and converts a slice of mixed data types to floats
func LoadRawData(raw interface{}) (f Float64Data) {
	var r []interface{}
	var s Float64Data

	switch t := raw.(type) {
	case []interface{}:
		r = t
	case []uint:
		for _, v := range t {
			s = append(s, float64(v))
		}
		return s

	case []bool:
		for _, v := range t {
			if v == true {
				s = append(s, 1.0)
			} else {
				s = append(s, 0.0)
			}
		}
		return s
	case []float64:
		return Float64Data(t)
	case []int:
		for _, v := range t {
			s = append(s, float64(v))
		}
		return s
	case []string:
		for _, v := range t {
			r = append(r, v)
		}
	case []time.Duration:
		for _, v := range t {
			r = append(r, v)
		}
	case map[int]int:
		for i := 0; i < len(t); i++ {
			s = append(s, float64(t[i]))
		}
		return s
	case map[int]string:
		for i := 0; i < len(t); i++ {
			r = append(r, t[i])
		}
	case map[int]uint:
		for i := 0; i < len(t); i++ {
			s = append(s, float64(t[i]))
		}
		return s
	case map[int]bool:
		for i := 0; i < len(t); i++ {
			if t[i] == true {
				s = append(s, 1.0)
			} else {
				s = append(s, 0.0)
			}
		}
		return s
	case map[int]float64:
		for i := 0; i < len(t); i++ {
			s = append(s, t[i])
		}
		return s
	}

	for _, v := range r {
		switch t := v.(type) {
		case int:
			a := float64(t)
			f = append(f, a)
		case uint:
			f = append(f, float64(t))
		case float64:
			f = append(f, t)
		case string:
			fl, err := strconv.ParseFloat(t, 64)
			if err == nil {
				f = append(f, fl)
			}
		case bool:
			if t == true {
				f = append(f, 1.0)
			} else {
				f = append(f, 0.0)
			}
		case time.Duration:
			f = append(f, float64(t))
		}
	}
	return f
}
