package s3io

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	MinChunkSize     int64 = 1024 * 1024 * 5
	DefaultChunkSize int64 = MinChunkSize

	defaultRetries     = 5
	defaultConcurrency = 5
)

var (
	ErrMinChunkSize = errors.New("given value is less than minimum chunksize of 5mb")

	noopLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
)

// DownloadAPIClient is an S3 API client that can invoke the GetObject operation.
type DownloadAPIClient interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// UploadAPIClient is an S3 API client that can invoke PutObject, UploadPart, CreateMultipartUpload,
// CompleteMultipartUpload, and AbortMultipartUpload operations.
type UploadAPIClient interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

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
