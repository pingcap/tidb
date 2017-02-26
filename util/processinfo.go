package util

type SessionManagerKey int

func (s SessionManagerKey) String() string {
	return "SessionManagerKey"
}

type ProcessInfo struct {
	ID      uint64
	User    string
	Host    string
	DB      string
	Command string
	Time    uint64
	State   string
	Info    string
}

// SessionManager is an interface for session manage. Show processlist and
// kill statement rely on this interface.
type SessionManager interface {
	ShowProcessList() []ProcessInfo
	Kill(connectionID uint64, query bool)
}
