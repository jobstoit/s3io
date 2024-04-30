package s3io

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// ObjectReader is an io.Reader implementation for an S3 Object
type ObjectReader struct {
	ctx           context.Context
	s3            DownloadAPIClient
	rd            *io.PipeReader
	logger        *slog.Logger
	chunkSize     int64
	concurrency   int
	clientOptions []func(*s3.Options)
	input         *s3.GetObjectInput
}

// NewObjectReader returns a new ObjectReader to do io.Reader opperations on your s3 object
func NewObjectReader(ctx context.Context, s3 DownloadAPIClient, input *s3.GetObjectInput, opts ...ObjectReaderOption) io.Reader {
	rd := &ObjectReader{
		ctx:         ctx,
		s3:          s3,
		input:       input,
		chunkSize:   DefaultChunkSize,
		concurrency: defaultConcurrency,
		logger:      noopLogger,
	}

	ObjectReaderOptions(opts...)(rd)

	return rd
}

// ObjectReaderOption is an option for the given read operation
type ObjectReaderOption func(*ObjectReader)

// ObjectReaderOptions is a collection of ObjectReaderOption's
func ObjectReaderOptions(opts ...ObjectReaderOption) ObjectReaderOption {
	return func(r *ObjectReader) {
		for _, op := range opts {
			op(r)
		}
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

	input := *r.input
	input.Range = aws.String("bytes=0-0")

	res, err := r.s3.GetObject(ctx, &input, r.clientOptions...)
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
	defer res.Body.Close()

	var contentLen int64
	if res.ContentRange == nil {
		if l := aws.ToInt64(res.ContentLength); l > 0 {
			contentLen = l
		}
	} else {
		parts := strings.Split(*res.ContentRange, "/")

		total := int64(-1)
		var err error
		// Checking for whether or not a numbered total exists
		// If one does not exist, we will assume the total to be -1, undefined,
		// and sequentially download each chunk until hitting a 416 error
		totalStr := parts[len(parts)-1]
		if totalStr != "*" {
			total, err = strconv.ParseInt(totalStr, 10, 64)
			if err != nil {
				return err
			}
		}

		contentLen = total
	}

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

	input := *r.input
	input.Range = &byteRange

	return r.s3.GetObject(ctx, &input, r.clientOptions...)
}

/*
 * Options
 */

// WithReaderLogger sets the logger for this reader
func WithReaderLogger(logger *slog.Logger) ObjectReaderOption {
	return func(r *ObjectReader) {
		if logger != nil {
			r.logger = logger
		}
	}
}

// WithReaderChunkSize sets the chunksize for this reader
func WithReaderChunkSize(size int64) ObjectReaderOption {
	return func(r *ObjectReader) {
		r.chunkSize = size
	}
}

// WithReaderConcurrency set the concurency amount for this reader
func WithReaderConcurrency(i int) ObjectReaderOption {
	return func(r *ObjectReader) {
		if i < 1 {
			i = 1
		}

		r.concurrency = i
	}
}

// WithReaderRetries sets the retry count for this reader
func WithReaderRetries(i uint8) ObjectReaderOption {
	return func(r *ObjectReader) {
		r.clientOptions = append(r.clientOptions, withS3Retries(int(i)))
	}
}

// WithReaderS3Options adds s3 options to the reader opperations
func WithReaderS3Options(opts ...func(*s3.Options)) ObjectReaderOption {
	return func(r *ObjectReader) {
		r.clientOptions = append(r.clientOptions, opts...)
	}
}
