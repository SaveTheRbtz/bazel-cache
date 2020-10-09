package utils

import (
	"errors"
	"regexp"
)

const (
	hashKeyLength = 64
	emptySha256   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

var (
	hashKeyRegexp = regexp.MustCompile("^[a-f0-9]{64}$")

	ErrHashInvalidLength = errors.New("hash is of invalid length")
	ErrHashInvalidFormat = errors.New("hash if of invalid format")
)

func ValidateHash(hash string, size int64) error {
	if size == int64(0) && hash == emptySha256 {
		return nil
	}

	if len(hash) != hashKeyLength {
		return ErrHashInvalidLength
	}

	if hashKeyRegexp.MatchString(hash) == false {
		return ErrHashInvalidFormat
	}

	return nil
}

func IsEmptyHash(hash string) bool {
	return hash == emptySha256
}
