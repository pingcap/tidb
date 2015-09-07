package server

// Config contains configuration options.
type Config struct {
	Addr     string `json:"addr" toml:"addr"`
	User     string `json:"user" toml:"user"`
	Password string `json:"password" toml:"password"`
	LogLevel string `json:"log_level" toml:"log_level"`
	SkipAuth bool   `json:"skip_auth" toml:"skip_auth"`
}
