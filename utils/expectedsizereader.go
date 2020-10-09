package utils

import (
	"errors"
	"io"
)

type ExpectedSizeReader struct {
	rwc          *ReadWriteCounter
	expectedSize int64
}

func NewExpectedSizeReader(r io.Reader, expectedSize int64) *ExpectedSizeReader {
	return &ExpectedSizeReader{
		rwc: NewReadWriteCounter(r, nil),
	}
}

func (cr *ExpectedSizeReader) Read(p []byte) (int, error) {
	read, err := cr.rwc.Read(p)
	if err != nil {
		if errors.Is(err, io.EOF) && cr.rwc.TotalRead() != cr.expectedSize {
			return read, io.ErrUnexpectedEOF
		}
	}
	return read, err
}
