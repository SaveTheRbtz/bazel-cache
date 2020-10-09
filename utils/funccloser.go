package utils

import (
	"io"
)

type FuncCloser struct {
	io.Reader
	io.Writer
	io.Closer

	closerFunc func() error
}

func NewFuncCloser(r io.Reader, w io.Writer, closerFunc func() error) io.ReadWriteCloser {
	return &FuncCloser{
		Reader:     r,
		Writer:     w,
		closerFunc: closerFunc,
	}
}

func (fc *FuncCloser) Close() error {
	return fc.closerFunc()
}
