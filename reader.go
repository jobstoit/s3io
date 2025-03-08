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
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// Reader is an io.Reader implementation for an S3 Object.
//
// You can open a new reader on it's own using the *s3.Client:
//
//	client := s3.NewFromConfig(cfg)
//	rd := s3io.NewReader(ctx, client, s3io.WithReaderConcurrency(3))
//
// Or you can open it using the BucketAPI:
//
//	bucket, err := s3io.Open(ctx, "my-bucket-name", s3io.WithBucketCredentials("access-key", "access-secret"))
//	if err != nil {
//	  log.Fatalf("unable to open bucket: %w", err)
//	}
//
//	rd := bucket.Get(ctx, "path/to/object.txt")
type Reader struct {
	ctx           context.Context
	s3            DownloadAPIClient
	rd            *io.PipeReader
	logger        *slog.Logger
	chunkSize     int64
	concurrency   int
	retries       int
	clientOptions []func(*s3.Options)
	input         *s3.GetObjectInput
	closed        *atomic.Bool
}

// NewReader returns a new ObjectReader to do io.Reader opperations on your s3 object
func NewReader(ctx context.Context, s3 DownloadAPIClient, input *s3.GetObjectInput, opts ...ReaderOption) io.Reader {
	rd := &Reader{
		ctx:         ctx,
		s3:          s3,
		input:       input,
		retries:     defaultRetries,
		chunkSize:   DefaultChunkSize,
		concurrency: defaultConcurrency,
		logger:      slog.New(slog.DiscardHandler),
		closed:      &atomic.Bool{},
	}

	ObjectReaderOptions(opts...)(rd)

	return rd
}

// ReaderOption is an option for the given read operation.
type ReaderOption func(*Reader)

// ObjectReaderOptions is a collection of ObjectReaderOption's.
func ObjectReaderOptions(opts ...ReaderOption) ReaderOption {
	return func(r *Reader) {
		for _, op := range opts {
			op(r)
		}
	}
}

// Stat is the fs.File implementaion for the ObjectReader.
func (r *Reader) Stat() (fs.FileInfo, error) {
	if r.closed.Load() {
		return nil, fs.ErrClosed
	}

	ctx := r.ctx

	input := *r.input
	input.Range = aws.String("bytes=0-0")

	res, err := r.s3.GetObject(ctx, &input, r.clientOptions...)
	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				return nil, fs.ErrNotExist
			default:
				return nil, apiError
			}
		}

		return nil, err
	}

	if res.Body != nil {
		defer res.Body.Close()
	}

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
				return nil, err
			}
		}

		contentLen = total
	}

	fi := fileInfo{
		name:        aws.ToString(r.input.Key),
		size:        contentLen,
		lastUpdated: aws.ToTime(res.LastModified),
		rd:          r,
	}

	return fi, nil
}

// close is the io.Close implementation for the ObjectReader
func (r *Reader) Close() error {
	if r.closed.CompareAndSwap(false, true) && r.rd != nil {
		r.rd.CloseWithError(io.EOF)
	}

	return nil
}

// Read is the io.Reader implementation for the ObjectReader.
//
// It returns an fs.ErrNotExists if the object doesn't exist in the given bucket.
// And returns an io.EOF when all bytes are read.
func (r *Reader) Read(p []byte) (int, error) {
	if r.closed.Load() {
		return 0, fs.ErrClosed
	}

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

func (r *Reader) preRead() error {
	ctx := r.ctx
	rd, wr := io.Pipe()

	r.rd = rd

	stats, err := r.Stat()
	if err != nil {
		closeErr := err
		if errors.Is(err, fs.ErrNotExist) {
			closeErr = io.EOF
		}

		defer rd.CloseWithError(closeErr)

		return err
	}

	contentLen := stats.Size()

	r.logger.Debug("pre read", slog.Int64("content-length", contentLen))
	cl := newConcurrencyLock(r.concurrency)

	nextLock := make(chan struct{}, 1)

	go r.getChunk(ctx, wr, cl, nextLock, 0, contentLen)
	defer close(nextLock)

	return nil
}

func (r *Reader) getChunk(
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

func (r *Reader) getObject(ctx context.Context, start, end int64) (*s3.GetObjectOutput, error) {
	r.logger.DebugContext(ctx, "getting chunk", slog.Group("chunk", slog.Int64("start", start), slog.Int64("end", end)))

	byteRange := fmt.Sprintf("bytes=%d-%d", start, end)

	input := *r.input
	input.Range = &byteRange

	var res *s3.GetObjectOutput
	var err error
	for range r.retries {
		res, err = r.s3.GetObject(ctx, &input, r.clientOptions...)
		if err == nil {
			return res, err
		}
	}

	return res, err
}

/*
 * Options
 */

// WithReaderLogger sets the logger for this reader
func WithReaderLogger(logger *slog.Logger) ReaderOption {
	return func(r *Reader) {
		if logger != nil {
			r.logger = logger
		}
	}
}

// WithReaderChunkSize sets the chunksize for this reader
func WithReaderChunkSize(size int64) ReaderOption {
	return func(r *Reader) {
		r.chunkSize = size
	}
}

// WithReaderConcurrency set the concurency amount for this reader
func WithReaderConcurrency(i int) ReaderOption {
	return func(r *Reader) {
		if i < 1 {
			i = 1
		}

		r.concurrency = i
	}
}

// WithReaderRetries sets the retry count for this reader
func WithReaderRetries(i int) ReaderOption {
	return func(r *Reader) {
		if i < 1 {
			i = 1
		}

		r.retries = i
	}
}

// WithReaderS3Options adds s3 options to the reader opperations
func WithReaderS3Options(opts ...func(*s3.Options)) ReaderOption {
	return func(r *Reader) {
		r.clientOptions = append(r.clientOptions, opts...)
	}
}

// fileInfo is the fs.FileInfo implenentation for the ObjectReader
type fileInfo struct {
	name        string
	size        int64
	lastUpdated time.Time
	rd          *Reader
}

func (f fileInfo) Name() string {
	return f.name
}

func (f fileInfo) Size() int64 {
	return f.size
}

func (f fileInfo) Mode() fs.FileMode {
	return fs.ModePerm
}

func (f fileInfo) ModTime() time.Time {
	return f.lastUpdated
}

func (f fileInfo) IsDir() bool {
	return false
}

func (f fileInfo) Sys() any {
	return f.rd
}
