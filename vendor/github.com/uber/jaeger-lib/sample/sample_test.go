package sample

import "testing"

func ExampleSayHello() {
	SayHello()
	// Output: Hello, playground
}

func TestSayHello(t *testing.T) {
	SayHello()
}
