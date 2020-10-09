package utils

import (
	"net/url"
	"strconv"
)

func URLValuesGetBool(u url.Values, name string) bool {
	v := u.Get(name)
	if v == "" {
		return false
	}
	switch v[0] {
	case 'y', 't', 'T', '1':
		return true
	}
	return false
}

func URLValuesGetInt(u url.Values, name string) int {
	if v, err := strconv.Atoi(u.Get(name)); err == nil {
		return v
	}
	return 0
}
