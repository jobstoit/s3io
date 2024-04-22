package s3io

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// ObjectReader is an io.Reader implementation for an S3 Object
type ObjectReader struct {
	ctx         context.Context
	cli         *s3.Client
	bucket      string
	key         string
	rd          *io.PipeReader
	logger      *slog.Logger
	chunkSize   int
	concurrency int
	s3Opts      []func(*s3.Options)
}

// NewObjectReader returns a new ObjectReader to do io.Reader opperations on your s3 object
func NewObjectReader(ctx context.Context, cli *s3.Client, bucketName, key string, opts ...ObjectReaderOption) (*ObjectReader, error) {
	rd := &ObjectReader{
		ctx:         ctx,
		cli:         cli,
		bucket:      bucketName,
		key:         key,
		chunkSize:   DefaultChunkSize,
		concurrency: defaultConcurrency,
		logger:      noopLogger,
	}

	if err := ObjectReaderOptions(opts...)(rd); err != nil {
		return nil, err
	}

	return rd, nil
}

// ObjectReaderOption is an option for the given read operation
type ObjectReaderOption func(*ObjectReader) error

// ObjectReaderOptions is a collection of ObjectReaderOption's
func ObjectReaderOptions(opts ...ObjectReaderOption) ObjectReaderOption {
	return func(r *ObjectReader) error {
		for _, op := range opts {
			if err := op(r); err != nil {
				return err
			}
		}

		return nil
	}
}

// Read is the io.Reader implementation for the ObjectReader.
//
// It returns an fs.ErrNotExists if the object doesn't exist in the given bucket.
// And returns an io.EOF when all bytes are read.
func (r *ObjectReader) Read(p []byte) (int, error) {
	if r.rd == nil {
		if err := r.preRead(); err != nil {
			return 0, err
		}
	}

	c, err := r.rd.Read(p)
	if err != nil && err == io.ErrClosedPipe {
		err = fs.ErrClosed
	}

	return c, err
}

func (r *ObjectReader) preRead() error {
	ctx := r.ctx
	rd, wr := io.Pipe()

	r.rd = rd

	res, err := r.cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &r.bucket,
		Key:    &r.key,
	}, r.s3Opts...)
	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				rd.CloseWithError(io.EOF)

				return fs.ErrNotExist
			default:
				return apiError
			}
		}
	}

	contentLen := *res.ContentLength

	r.logger.Debug("pre read", slog.Int64("content-length", contentLen))
	cl := newConcurrencyLock(r.concurrency)

	nextLock := make(chan struct{}, 1)

	go r.getChunk(ctx, wr, cl, nextLock, 0, contentLen)
	defer close(nextLock)

	return nil
}

func (r *ObjectReader) getChunk(
	ctx context.Context,
	wr *io.PipeWriter,
	cl *concurrencyLock,
	sequenceLock chan struct{},
	start, contentLen int64,
) {
	if start == contentLen+1 { // EOF
		defer cl.Close()

		select {
		case <-ctx.Done():
		case <-sequenceLock:
			wr.CloseWithError(io.EOF)
		}

		return
	}

	cl.Lock()
	defer cl.Unlock()

	end := start + int64(r.chunkSize)
	if end > contentLen {
		end = contentLen
	}

	nextLock := make(chan struct{}, 1)
	defer close(nextLock)

	go r.getChunk(ctx, wr, cl, nextLock, end+1, contentLen)

	res, err := r.getObject(ctx, start, end)
	if err != nil {
		wr.CloseWithError(err)
		return
	}

	defer res.Body.Close()

	select {
	case <-ctx.Done():
		return
	case <-sequenceLock:
		if _, err := io.Copy(wr, res.Body); err != nil && err != io.EOF {
			wr.CloseWithError(err)
		}

		r.logger.DebugContext(ctx, "chunk read",
			slog.Group("chunk", slog.Int64("start", start), slog.Int64("end", end), slog.Int64("content_lenght", contentLen)),
		)
	}
}

func (r *ObjectReader) getObject(ctx context.Context, start, end int64) (*s3.GetObjectOutput, error) {
	r.logger.DebugContext(ctx, "getting chunk", slog.Group("chunk", slog.Int64("start", start), slog.Int64("end", end)))

	byteRange := fmt.Sprintf("bytes=%d-%d", start, end)
	input := &s3.GetObjectInput{
		Bucket: &r.bucket,
		Key:    &r.key,
		Range:  &byteRange,
	}

	return r.cli.GetObject(ctx, input, r.s3Opts...)
}

/*
 * Options
 */

// WithReaderLogger sets the logger for this reader
func WithReaderLogger(logger *slog.Logger) ObjectReaderOption {
	return func(r *ObjectReader) error {
		if logger != nil {
			r.logger = logger
		}

		return nil
	}
}

// WithReaderChunkSize sets the chunksize for this reader
func WithReaderChunkSize(size uint) ObjectReaderOption {
	return func(r *ObjectReader) error {
		r.chunkSize = int(size)

		return nil
	}
}

// WithReaderConcurrency set the concurency amount for this reader
func WithReaderConcurrency(i uint8) ObjectReaderOption {
	return func(r *ObjectReader) error {
		r.concurrency = int(i)

		return nil
	}
}

// WithReaderRetries sets the retry count for this reader
func WithReaderRetries(i uint8) ObjectReaderOption {
	return func(r *ObjectReader) error {
		r.s3Opts = append(r.s3Opts, withS3Retries(int(i)))

		return nil
	}
}

// WithReaderS3Options adds s3 options to the reader opperations
func WithReaderS3Options(opts ...func(*s3.Options)) ObjectReaderOption {
	return func(r *ObjectReader) error {
		r.s3Opts = append(r.s3Opts, opts...)

		return nil
	}
}
