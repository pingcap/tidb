package pkdbrepl

import (
	"errors"
	"testing"
)

func TestIsRetryableResolveLocksError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", want: false},
		{name: "scan lock response", err: errors.New("unexpected scanlock error: error:<locked:<lock_version:1> >"), want: true},
		{name: "internal key locked name", err: errors.New("KeyIsLocked"), want: false},
		{name: "future timestamp", err: errors.New("cannot set read timestamp to a future time"), want: false},
		{name: "context canceled", err: errors.New("context canceled"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryableResolveLocksError(tt.err); got != tt.want {
				t.Fatalf("isRetryableResolveLocksError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
