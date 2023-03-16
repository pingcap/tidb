package pipeline_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/pingcap/tidb/br/pkg/utils/pipeline"
	"github.com/stretchr/testify/require"
)

func TestMagic(t *testing.T) {
	a := pipeline.TransformFunc[int, int](func(a int) int { return a + 4 })
	aa := any(a)

	va := reflect.ValueOf(aa)
	met := va.MethodByName("MainLoop")
	require.True(t, met.IsValid())

	fmt.Println(met.Type().In(0))
	fmt.Println(met.Type().In(1).Elem())
	t.Fail()
}

func TestBasic(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	a := pipeline.TransformFunc[int, int](func(a int) int { return a + 4 })
	b := pipeline.TransformFunc[int, string](func(a int) string {
		return fmt.Sprintf("hello, little %d", a)
	})
	c := pipeline.TransformFunc[string, string](strings.ToUpper)

	pipe := pipeline.Append[int, string, string](pipeline.Append[int, int, string](a, b), c)

	mgr := pipeline.Execute(ctx, pipe)

	for i := 0; i < 10; i++ {
		mgr.Input() <- i
	}
	close(mgr.Input())
	mgr.Wait()

OUTER:
	for {
		select {
		case item, ok := <-mgr.Output():
			if !ok {
				break OUTER
			}
			fmt.Println(item)
		default:
			break OUTER
		}
	}

	req.FailNow("PASSED")
}
