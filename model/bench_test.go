package model

import "testing"

func BenchmarkString(b *testing.B) {
	actions := make([]ActionType, 16)
	for i := 0; i < len(actions); i++ {
		actions[i] = ActionType(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := 0; i < len(actions); i++ {
			_ = actions[i].String()
		}
	}
}
