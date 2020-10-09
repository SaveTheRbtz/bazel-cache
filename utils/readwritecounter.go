package utils

import (
	"io"
	"sync/atomic"
)

type ReadWriteCounter struct {
	io.Reader
	io.Writer

	totalRead    int64
	totalWritten int64
}

func NewReadWriteCounter(r io.Reader, w io.Writer) *ReadWriteCounter {
	return &ReadWriteCounter{
		Reader: r,
		Writer: w,
	}
}

func (rwc *ReadWriteCounter) Read(p []byte) (int, error) {
	read, err := rwc.Reader.Read(p)
	atomic.AddInt64(&rwc.totalRead, int64(read))
	return read, err
}

func (rwc *ReadWriteCounter) Write(p []byte) (int, error) {
	written, err := rwc.Writer.Write(p)
	atomic.AddInt64(&rwc.totalWritten, int64(written))
	return written, err
}

func (rwc *ReadWriteCounter) TotalRead() int64 {
	return atomic.LoadInt64(&rwc.totalRead)
}

func (rwc *ReadWriteCounter) TotalWritten() int64 {
	return atomic.LoadInt64(&rwc.totalWritten)
}
