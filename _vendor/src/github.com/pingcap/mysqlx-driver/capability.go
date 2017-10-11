package mysql

import (
	"fmt"
)

// Capabilities are indexed by name and can be one of 7 protobuf types (including nesting...).
// - see: mysqlx_datatypes.proto 'Type'
// So rather complex and messy even if in reality most combinations won't be used.
// - currently handled combinations are:
//   - scalar string
//   - scalar bool
//   - array of string

type capabilityType uint8

const (
	// CapabilityString is a string
	CapabilityString capabilityType = iota
	// CapabilityBool is a boolean
	CapabilityBool
)

// ugly!
type capability struct {
	capabilityType   capabilityType
	capabilityBool   bool
	capabilityString string
}

func (c *capability) Type() string {
	var t string
	if c != nil {
		switch c.capabilityType {
		case CapabilityBool:
			t = "bool"
		case CapabilityString:
			t = "string"
		}
	}
	return t
}

func (c *capability) String() string {
	if c != nil && c.capabilityType == CapabilityString {
		return c.capabilityString
	}
	return ""
}

func (c *capability) Bool() bool {
	if c != nil && c.capabilityType == CapabilityBool {
		return c.capabilityBool
	}
	return false
}

// Values contains an array of capabilities
type Values []capability

// ServerCapabilities is a named map of capability values
type ServerCapabilities map[string]Values

// NewServerCapabilities returns a structure containing the named capabilities of the server
func NewServerCapabilities() ServerCapabilities {
	return make(map[string]Values)
}

// Exists returns true if the named capability exists
func (sc ServerCapabilities) Exists(name string) bool {
	if sc == nil {
		return false
	}
	_, found := sc[name]

	return found
}

// Values returns the named Values
func (sc ServerCapabilities) Values(name string) Values {
	if sc == nil {
		return nil
	}

	// get the values
	values, found := sc[name]
	// not necessary to do this explicitly?
	if !found {
		return nil
	}
	return values
}

// AddScalarString adds the given string value to the named capability
func (sc ServerCapabilities) AddScalarString(name string, value string) error {
	// debug.Msg("ServerCapabilities.AddScalarString(%q,%q)", name, value)
	if sc == nil {
		return fmt.Errorf("ServerCapabilities.AddScalarString() on nil value")
	}
	values := sc.Values(name)
	values = append(values, capability{capabilityType: CapabilityString, capabilityString: value})
	sc[name] = values

	return nil
}

// AddScalarBool adds the given boolean value to the named capability
func (sc ServerCapabilities) AddScalarBool(name string, value bool) error {
	// debug.Msg("ServerCapabilities.AddScalar(%q,%+v)", name, value)
	if sc == nil {
		return fmt.Errorf("ServerCapabilities.AddScalarBool() on nil value")
	}
	values := sc.Values(name)
	values = append(values, capability{capabilityType: CapabilityBool, capabilityBool: value})
	sc[name] = values

	return nil
}

// AddArrayString adds the given array of strings to the named capability
func (sc ServerCapabilities) AddArrayString(name string, values []string) error {
	// debug.Msg("ServerCapabilities.AddArrayString(%q,%+v)", name, values)
	if sc == nil {
		return fmt.Errorf("ServerCapabilities.AddArrayString() on nil value")
	}
	for i := range values {
		if err := sc.AddScalarString(name, values[i]); err != nil {
			return err
		}
	}

	return nil
}
