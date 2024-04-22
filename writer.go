package s3io

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ObjectWriter is an io.WriteCloser implementation for an s3 Object
type ObjectWriter struct {
	ctx         context.Context
	cli         *s3.Client
	bucket      string
	key         string
	wr          *io.PipeWriter
	logger      *slog.Logger
	chunkSize   int
	concurrency int
	alc         types.ObjectCannedACL
	s3Opts      []func(*s3.Options)

	mux        sync.Mutex
	wg         sync.WaitGroup
	parts      []types.CompletedPart
	closingErr chan error
}

// NewObjectWriter returns a new ObjectWriter to do io.Writer opperations on your s3 object
func NewObjectWriter(ctx context.Context, cli *s3.Client, bucketName, key string, opts ...ObjectWriterOption) (*ObjectWriter, error) {
	wr := &ObjectWriter{
		ctx:         ctx,
		cli:         cli,
		bucket:      bucketName,
		key:         key,
		chunkSize:   DefaultChunkSize,
		concurrency: defaultConcurrency,
		logger:      noopLogger,

		closingErr: make(chan error, 1),
	}

	if err := ObjectWriterOptions(opts...)(wr); err != nil {
		return nil, err
	}

	return wr, nil
}

// ObjectWriterOption is an option for the given write operation
type ObjectWriterOption func(*ObjectWriter) error

// ObjectWriterOptions is a collection of ObjectWriterOption's
func ObjectWriterOptions(opts ...ObjectWriterOption) ObjectWriterOption {
	return func(w *ObjectWriter) error {
		for _, op := range opts {
			if err := op(w); err != nil {
				return err
			}
		}

		return nil
	}
}

// Write is the io.Writer implementation of the ObjectWriter
//
// The object is stored when the Close method is called.
func (w *ObjectWriter) Write(p []byte) (int, error) {
	if w.wr == nil {
		if err := w.preWrite(); err != nil {
			return 0, err
		}
	}

	return w.wr.Write(p)
}

// Close completes the write opperation.
//
// If the byte size is less than writer's chunk size then a simply PutObject opperation is preformed.
// Otherwise a multipart upload complete opperation is preformed.
// The error returned is the error from this store opperation.
//
// If an error occured while uploading parts this error might also be a upload part error joined with
// a AbortMultipartUpload error.
func (w *ObjectWriter) Close() error {
	w.wr.CloseWithError(io.EOF)

	w.logger.DebugContext(w.ctx, "closing writer")
	err := <-w.closingErr

	w.logger.DebugContext(w.ctx, "closing writer error recieved", slog.Any("error", err))
	return err
}

func (w *ObjectWriter) preWrite() error {
	ctx := w.ctx
	rd, wr := io.Pipe()

	w.wr = wr
	cl := newConcurrencyLock(w.concurrency)

	w.wg.Add(1)
	go w.writeChunk(ctx, rd, cl, nil, 1)

	return nil
}

func (w *ObjectWriter) writeChunk(ctx context.Context, rd *io.PipeReader, cl *concurrencyLock, uploadID *string, partNr int32) {
	defer w.wg.Done()

	select {
	case <-ctx.Done():
		cl.Close()
		return
	default:
		cl.Lock()
		defer cl.Unlock()

		by, err := io.ReadAll(io.LimitReader(rd, int64(w.chunkSize)))
		if err != nil {
			w.closeWithErr(ctx, err, rd, cl, uploadID)
			return
		}

		size := len(by)
		if partNr == 1 {
			if size < w.chunkSize { // For small uploads
				err = w.putObject(ctx, by)
				w.closeWithErr(ctx, err, rd, cl, uploadID)
				return
			}

			uploadID, err = w.createMultipartUpload(ctx)
			if err != nil {
				w.closeWithErr(ctx, err, rd, cl, uploadID)
				return
			}

		}

		if len(by) < w.chunkSize { // EOF
			go w.completeUpload(ctx, uploadID)
		} else {
			w.wg.Add(1)
			go w.writeChunk(ctx, rd, cl, uploadID, partNr+1)
		}

		part, err := w.uploadPart(ctx, uploadID, partNr, by)
		if err != nil {
			w.closeWithErr(ctx, err, rd, cl, uploadID)
			return
		}

		w.mux.Lock()
		defer w.mux.Unlock()

		w.parts = append(w.parts, part)
	}
}

func (w *ObjectWriter) closeWithErr(ctx context.Context, err error, rd *io.PipeReader, cl *concurrencyLock, uploadID *string) {
	defer close(w.closingErr)
	defer cl.Close()

	if err != nil {
		w.logger.DebugContext(
			ctx,
			"error uploading",
			slog.String("upload_id", *uploadID),
			slog.Any("error", err),
		)
	}

	rd.CloseWithError(err)
	if uploadID != nil {
		err = errors.Join(err, w.abortUpload(ctx, uploadID))
	}

	w.closingErr <- err
}

func (w *ObjectWriter) putObject(ctx context.Context, by []byte) error {
	w.logger.DebugContext(ctx, "upload small file", slog.Int("size", len(by)))

	input := &s3.PutObjectInput{
		Bucket: &w.bucket,
		Key:    &w.key,
		ACL:    w.alc,
		Body:   bytes.NewReader(by),
	}

	_, err := w.cli.PutObject(ctx, input, w.s3Opts...)

	return err
}

func (w *ObjectWriter) createMultipartUpload(ctx context.Context) (*string, error) {
	w.logger.DebugContext(ctx, "starting multipart upload")

	input := &s3.CreateMultipartUploadInput{
		Bucket: &w.bucket,
		Key:    &w.key,
		ACL:    w.alc,
	}

	res, err := w.cli.CreateMultipartUpload(ctx, input, w.s3Opts...)
	if err != nil {
		return nil, err
	}

	return res.UploadId, nil
}

func (w *ObjectWriter) uploadPart(ctx context.Context, uploadID *string, partNr int32, by []byte) (types.CompletedPart, error) {
	w.logger.DebugContext(
		ctx,
		"upload part",
		slog.String("upload_id", *uploadID),
		slog.Int("part_nr", int(partNr)),
		slog.Int("size", len(by)),
	)

	input := &s3.UploadPartInput{
		Bucket:     &w.bucket,
		Key:        &w.key,
		UploadId:   uploadID,
		PartNumber: &partNr,
		Body:       bytes.NewReader(by),
	}

	res, err := w.cli.UploadPart(ctx, input, w.s3Opts...)
	if err != nil {
		return types.CompletedPart{}, err
	}

	return types.CompletedPart{
		ChecksumCRC32:  res.ChecksumCRC32,
		ChecksumCRC32C: res.ChecksumCRC32C,
		ChecksumSHA1:   res.ChecksumSHA1,
		ChecksumSHA256: res.ChecksumSHA256,
		ETag:           res.ETag,
		PartNumber:     &partNr,
	}, nil
}

func (w *ObjectWriter) abortUpload(ctx context.Context, uploadID *string) error {
	w.logger.DebugContext(ctx, "abort upload", slog.String("upload_id", *uploadID))

	input := &s3.AbortMultipartUploadInput{
		Bucket:   &w.bucket,
		Key:      &w.key,
		UploadId: uploadID,
	}

	_, err := w.cli.AbortMultipartUpload(ctx, input, w.s3Opts...)

	return err
}

func (w *ObjectWriter) completeUpload(ctx context.Context, uploadID *string) {
	defer close(w.closingErr)

	w.wg.Wait()

	w.mux.Lock()
	defer w.mux.Unlock()

	parts := make([]types.CompletedPart, len(w.parts))
	copy(parts, w.parts)

	w.logger.DebugContext(ctx, "complete upload", slog.String("upload_id", *uploadID), slog.Int("parts", len(parts)))

	sort.Slice(parts, func(i, j int) bool {
		return *parts[i].PartNumber < *parts[j].PartNumber
	})

	input := &s3.CompleteMultipartUploadInput{
		Bucket:   &w.bucket,
		Key:      &w.key,
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	}

	_, err := w.cli.CompleteMultipartUpload(ctx, input, w.s3Opts...)

	w.closingErr <- err
}

/*
 * Options
 */

// WithWriterLogger adds a logger for this writer
func WithWriterLogger(logger *slog.Logger) ObjectWriterOption {
	return func(w *ObjectWriter) error {
		w.logger = logger

		return nil
	}
}

// WithWriterChunkSize sets the chunksize for this writer
func WithWriterChunkSize(size uint) ObjectWriterOption {
	return func(w *ObjectWriter) error {
		i := int(size)
		if i < MinChunkSize {
			return ErrMinChunkSize
		}

		w.chunkSize = i

		return nil
	}
}

// WithWriterConcurrency sets the concurrency amount for this writer
func WithWriterConcurrency(i uint8) ObjectWriterOption {
	return func(w *ObjectWriter) error {
		w.concurrency = int(i)

		return nil
	}
}

// WithWriterRetries sets the retry count for this writer
func WithWriteRetries(i uint8) ObjectWriterOption {
	return func(w *ObjectWriter) error {
		w.s3Opts = append(w.s3Opts, withS3Retries(int(i)))

		return nil
	}
}

// WithWriterACL sets the ACL for the object thats written
func WithWriterACL(acl types.ObjectCannedACL) ObjectWriterOption {
	return func(w *ObjectWriter) error {
		w.alc = acl

		return nil
	}
}

// WithWriterS3Options adds s3 options to the writer opperations
func WithWriterS3Options(opts ...func(*s3.Options)) ObjectWriterOption {
	return func(w *ObjectWriter) error {
		w.s3Opts = append(w.s3Opts, opts...)

		return nil
	}
}
