package utils

import (
	"testing"
)

func TestIsSysOrTempSysDB(t *testing.T) {
	tests := []struct {
		name     string
		db       string
		expected bool
	}{
		{
			name:     "mysql system db",
			db:       "mysql",
			expected: true,
		},
		{
			name:     "sys system db",
			db:       "sys",
			expected: true,
		},
		{
			name:     "temporary mysql db",
			db:       "__TiDB_BR_Temporary_mysql",
			expected: true,
		},
		{
			name:     "temporary sys db",
			db:       "__TiDB_BR_Temporary_sys",
			expected: true,
		},
		{
			name:     "normal db",
			db:       "test",
			expected: false,
		},
		{
			name:     "temporary normal db",
			db:       "__TiDB_BR_Temporary_test",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsSysOrTempSysDB(tt.db)
			if result != tt.expected {
				t.Errorf("IsSysOrTempSysDB(%q) = %v, want %v", tt.db, result, tt.expected)
			}
		})
	}
}
