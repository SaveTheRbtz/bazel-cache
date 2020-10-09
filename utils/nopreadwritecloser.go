package utils

import (
	"io"
)

var (
	NopReadWriteCloser = &nopReadWriteCloser{}
)

type nopReadWriteCloser struct {
	io.ReadWriteCloser
}

func (nopReadWriteCloser) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (nopReadWriteCloser) Write(p []byte) (int, error) {
	return len(p), nil
}

func (nopReadWriteCloser) Close() error {
	return nil
}
