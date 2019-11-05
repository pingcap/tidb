package main

import (
	"os"
	"runtime"
	"testing"
)

func TestExpandUser(t *testing.T) {
	env := "HOME"
	if runtime.GOOS == "windows" {
		env = "USERPROFILE"
	} else if runtime.GOOS == "plan9" {
		env = "home"
	}

	oldenv := os.Getenv(env)
	os.Setenv(env, "/home/gopher")
	defer os.Setenv(env, oldenv)

	tests := []struct {
		input string
		want  string
	}{
		{input: "~/foo", want: "/home/gopher/foo"},
		{input: "${HOME}/foo", want: "/home/gopher/foo"},
		{input: "/~/foo", want: "/~/foo"},
	}
	for _, tt := range tests {
		got := expandUser(tt.input)
		if got != tt.want {
			t.Fatalf("want %q, but %q", tt.want, got)
		}
	}
}
