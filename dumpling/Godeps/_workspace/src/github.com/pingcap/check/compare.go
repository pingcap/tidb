package check

import (
	"bytes"
	"fmt"
	"reflect"
	"time"
)

type compareFunc func(v1 interface{}, v2 interface{}) (bool, error)

type valueCompare struct {
	Name string

	Func compareFunc

	Operator string
}

// v1 and v2 must have the same type
// return >0 if v1 > v2
// return 0 if v1 = v2
// return <0 if v1 < v2
// now we only support int, uint, float64, string and []byte comparison
func compare(v1 interface{}, v2 interface{}) (int, error) {
	value1 := reflect.ValueOf(v1)
	value2 := reflect.ValueOf(v2)

	switch v1.(type) {
	case int, int8, int16, int32, int64:
		a1 := value1.Int()
		a2 := value2.Int()
		if a1 > a2 {
			return 1, nil
		} else if a1 == a2 {
			return 0, nil
		}
		return -1, nil
	case uint, uint8, uint16, uint32, uint64:
		a1 := value1.Uint()
		a2 := value2.Uint()
		if a1 > a2 {
			return 1, nil
		} else if a1 == a2 {
			return 0, nil
		}
		return -1, nil
	case float32, float64:
		a1 := value1.Float()
		a2 := value2.Float()
		if a1 > a2 {
			return 1, nil
		} else if a1 == a2 {
			return 0, nil
		}
		return -1, nil
	case string:
		a1 := value1.String()
		a2 := value2.String()
		if a1 > a2 {
			return 1, nil
		} else if a1 == a2 {
			return 0, nil
		}
		return -1, nil
	case []byte:
		a1 := value1.Bytes()
		a2 := value2.Bytes()
		return bytes.Compare(a1, a2), nil
	case time.Time:
		a1 := v1.(time.Time)
		a2 := v2.(time.Time)
		if a1.After(a2) {
			return 1, nil
		} else if a1.Equal(a2) {
			return 0, nil
		}
		return -1, nil
	case time.Duration:
		a1 := v1.(time.Duration)
		a2 := v2.(time.Duration)
		if a1 > a2 {
			return 1, nil
		} else if a1 == a2 {
			return 0, nil
		}
		return -1, nil
	default:
		return 0, fmt.Errorf("type %T is not supported now", v1)
	}
}

func less(v1 interface{}, v2 interface{}) (bool, error) {
	n, err := compare(v1, v2)
	if err != nil {
		return false, err
	}

	return n < 0, nil
}

func lessEqual(v1 interface{}, v2 interface{}) (bool, error) {
	n, err := compare(v1, v2)
	if err != nil {
		return false, err
	}

	return n <= 0, nil
}

func greater(v1 interface{}, v2 interface{}) (bool, error) {
	n, err := compare(v1, v2)
	if err != nil {
		return false, err
	}

	return n > 0, nil
}

func greaterEqual(v1 interface{}, v2 interface{}) (bool, error) {
	n, err := compare(v1, v2)
	if err != nil {
		return false, err
	}

	return n >= 0, nil
}

func (v *valueCompare) Check(params []interface{}, names []string) (bool, string) {
	if len(params) != 2 {
		return false, fmt.Sprintf("%s needs 2 arguments", v.Name)
	}

	v1 := params[0]
	v2 := params[1]
	v1Type := reflect.TypeOf(v1)
	v2Type := reflect.TypeOf(v2)

	if v1Type.Kind() != v2Type.Kind() {
		return false, fmt.Sprintf("%s needs two same type, but %s != %s", v.Name, v1Type.Kind(), v2Type.Kind())
	}

	b, err := v.Func(v1, v2)
	if err != nil {
		return false, fmt.Sprintf("%s check err %v", v.Name, err)
	}

	return b, ""
}

func (v *valueCompare) Info() *CheckerInfo {
	return &CheckerInfo{
		Name:   v.Name,
		Params: []string{"compare_one", "compare_two"},
	}
}

var Less = &valueCompare{Name: "Less", Func: less, Operator: "<"}
var LessEqual = &valueCompare{Name: "LessEqual", Func: lessEqual, Operator: "<="}
var Greater = &valueCompare{Name: "Greater", Func: greater, Operator: ">"}
var GreaterEqual = &valueCompare{Name: "GreaterEqual", Func: greaterEqual, Operator: ">="}
