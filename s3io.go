package s3io

import (
	"errors"
	"io"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

func withS3Retries(count int) func(*s3.Options) {
	return func(o *s3.Options) {
		o.Retryer = retry.NewStandard(func(ro *retry.StandardOptions) {
			ro.MaxAttempts = count
		})
	}
}
