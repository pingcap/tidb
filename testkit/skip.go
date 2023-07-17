package testkit

import "testing"

// SkipUnderShort skips the test if the -short flag is set.
func SkipUnderShort(t testing.T, args ...interface{}) {
	t.Helper()
	if testing.Short() {
		t.Skip(append([]interface{}{"disabled under -short"}, args...))
	}
}
