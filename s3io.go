package s3io

import (
	"errors"
	"io"
	"log/slog"
)

const (
	MinChunkSize     = 1024 * 1024 * 5
	DefaultChunkSize = MinChunkSize

	defaultRetries     = 5
	defaultConcurrency = 5
)

var (
	ErrMinChunkSize = errors.New("given value is less than minimum chunksize of 5mb")

	noopLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
)

type concurrencyLock struct {
	l chan struct{}
}

func newConcurrencyLock(size int) *concurrencyLock {
	return &concurrencyLock{
		l: make(chan struct{}, size),
	}
}

func (c *concurrencyLock) Lock() {
	c.l <- struct{}{}
}

func (c *concurrencyLock) Unlock() {
	<-c.l
}

func (c *concurrencyLock) Close() {
	close(c.l)
}
