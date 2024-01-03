package server

// TODO: Wire up auth
func (s *FlightSQLServer) Validate(username, password string) (string, error) {

	return "todo", nil
}

func (s *FlightSQLServer) IsValid(bearerToken string) (interface{}, error) {
	return nil, nil
}
