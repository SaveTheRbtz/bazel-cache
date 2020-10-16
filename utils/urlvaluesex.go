package utils

import (
	"net/url"
	"strconv"
)

func URLValuesGetBool(u url.Values, name string, defaultValue bool) bool {
	if v := u.Get(name); v != "" {
		switch v[0] {
		case 'y', 't', 'T', '1':
			return true
		}
	}
	return defaultValue
}

func URLValuesGetInt(u url.Values, name string, defaultValue int) int {
	if v, err := strconv.Atoi(u.Get(name)); err == nil {
		return v
	}
	return defaultValue
}
