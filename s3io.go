package s3io

import (
	"errors"
)

const (
	MinChunkSize     = 1024 * 1024 * 5
	DefaultChunkSize = 1024 * 1024 * 5
)

var ErrMinChunkSize = errors.New("given value is less than minimum chunksize of 5mb")

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
