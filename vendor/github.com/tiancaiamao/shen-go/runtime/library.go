package runtime

import (
	"math"
	"os"
	"path"
)

func PackagePath() string {
	gopath := os.Getenv("GOPATH")
	return path.Join(gopath, "src/github.com/tiancaiamao/cora")
}

func cadr(o Obj) Obj {
	return car(cdr(o))
}

func caddr(o Obj) Obj {
	return car(cdr(cdr(o)))
}

func cdddr(o Obj) Obj {
	return cdr(cdr(cdr(o)))
}

func cadddr(o Obj) Obj {
	return car(cdr(cdr(cdr(o))))
}

func reverse(o Obj) Obj {
	ret := Nil
	for o != Nil {
		ret = cons(car(o), ret)
		o = cdr(o)
	}
	return ret
}

func equal(x, y Obj) Obj {
	if *x != *y {
		return False
	}

	switch *x {
	case scmHeadNumber:
		if mustNumber(x).val != mustNumber(y).val {
			return False
		}
	case scmHeadString:
		if mustString(x) != mustString(y) {
			return False
		}
	case scmHeadBoolean:
		if x != y {
			return False
		}
	case scmHeadSymbol:
		if mustSymbol(x).offset != mustSymbol(y).offset {
			return False
		}
	case scmHeadNull:
		if *y != scmHeadNull {
			return False
		}
	case scmHeadPair:
		// TODO: maybe cycle exists!
		if x != y {
			if equal(car(x), car(y)) == False {
				return False
			}
			if equal(cdr(x), cdr(y)) == False {
				return False
			}
		}
	case scmHeadStream, scmHeadClosure, scmHeadPrimitive:
		if x != y {
			return False
		}
	case scmHeadVector:
		v1 := mustVector(x)
		v2 := mustVector(y)
		if len(v1) != len(v2) {
			return False
		}
		for i := 0; i < len(v1); i++ {
			if v1[i] != nil || v2[i] != nil {
				if equal(v1[i], v2[i]) != True {
					return False
				}
			}
		}
	}

	return True
}

func listLength(l Obj) int {
	count := 0
	for *l == scmHeadPair {
		count++
		l = cdr(l)
	}
	return count
}

func ListToSlice(l Obj) []Obj {
	var ret []Obj
	for *l == scmHeadPair {
		ret = append(ret, car(l))
		l = cdr(l)
	}
	return ret
}

func GetInteger(o Obj) int {
	return int(mustNumber(o).val)
}

func Cadr(o Obj) Obj {
	return cadr(o)
}

func Car(o Obj) Obj {
	return car(o)
}

func Cdr(o Obj) Obj {
	return cdr(o)
}

func Cons(x, y Obj) Obj {
	return cons(x, y)
}

// isInteger determinate whether a float64 is actually a precise integer.
// Judge is according to IEEE754 standard.
func isPreciseInteger(f float64) bool {
	exp := math.Ilogb(f)
	if exp < 0 && exp != math.MinInt32 {
		return false
	}

	if exp >= 52 {
		return true
	}

	bits := math.Float64bits(f)
	return (bits << uint(12+exp)) == 0
}
