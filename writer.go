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

// Writer is an io.WriteCloser implementation for an s3 Object.
//
// You can open a new writer on its own using the *s3.Client:
//
//	client := s3.NewFromConfig(cfg)
//	wr := s3io.NewWriter(ctx, client, s3io.WithWriterConcurrency(5))
//
// Or you can open it usign the BucketAPI:
//
//	bucket, err := s3io.Open(ctx, "my-bucket-name", s3io.WithBucketCredentials("access-key", "access-secret"))
//	if err != nil {
//	  log.Fatalf("unable to open bucket: %w", err)
//	}
//
//	 wr := bucket.Put(ctx, "path/to/object.txt")
type Writer struct {
	ctx           context.Context
	s3            UploadAPIClient
	wr            *io.PipeWriter
	logger        *slog.Logger
	chunkSize     int64
	concurrency   int
	clientOptions []func(*s3.Options)
	input         *s3.PutObjectInput

	mux        sync.Mutex
	wg         sync.WaitGroup
	parts      []types.CompletedPart
	closingErr chan error
}

// NewWriter returns a new ObjectWriter to do io.Writer opperations on your s3 object
func NewWriter(ctx context.Context, s3 UploadAPIClient, input *s3.PutObjectInput, opts ...WriterOption) io.WriteCloser {
	wr := &Writer{
		ctx:         ctx,
		s3:          s3,
		input:       input,
		chunkSize:   DefaultChunkSize,
		concurrency: defaultConcurrency,
		logger:      slog.New(slog.DiscardHandler),

		closingErr: make(chan error, 1),
	}

	ObjectWriterOptions(opts...)(wr)

	wr.preWrite()

	return wr
}

// WriterOption is an option for the given write operation
type WriterOption func(*Writer)

// ObjectWriterOptions is a collection of ObjectWriterOption's
func ObjectWriterOptions(opts ...WriterOption) WriterOption {
	return func(w *Writer) {
		for _, op := range opts {
			op(w)
		}
	}
}

// Write is the io.Writer implementation of the ObjectWriter
//
// The object is stored when the Close method is called.
func (w *Writer) Write(p []byte) (int, error) {
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
func (w *Writer) Close() error {
	w.wr.CloseWithError(io.EOF)

	w.logger.DebugContext(w.ctx, "closing writer")
	err := <-w.closingErr

	w.logger.DebugContext(w.ctx, "closing writer error recieved", slog.Any("error", err))
	return err
}

func (w *Writer) preWrite() {
	ctx := w.ctx
	rd, wr := io.Pipe()

	w.wr = wr
	cl := newConcurrencyLock(w.concurrency)

	w.wg.Add(1)
	go w.writeChunk(ctx, rd, cl, nil, 1)
}

func (w *Writer) writeChunk(ctx context.Context, rd *io.PipeReader, cl *concurrencyLock, uploadID *string, partNr int32) {
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

		size := int64(len(by))
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

		if size < w.chunkSize { // EOF
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

func (w *Writer) closeWithErr(ctx context.Context, err error, rd *io.PipeReader, cl *concurrencyLock, uploadID *string) {
	defer close(w.closingErr)
	defer cl.Close()

	if err != nil {
		uid := ""
		if uploadID != nil {
			uid = *uploadID
		}

		w.logger.DebugContext(
			ctx,
			"error uploading",
			slog.String("upload_id", uid),
			slog.Any("error", err),
		)
	}

	rd.CloseWithError(err)
	if uploadID != nil {
		err = errors.Join(err, w.abortUpload(ctx, uploadID))
	}

	w.closingErr <- err
}

func (w *Writer) putObject(ctx context.Context, by []byte) error {
	w.logger.DebugContext(ctx, "upload small file", slog.Int("size", len(by)))

	input := w.input
	input.Body = bytes.NewReader(by)

	_, err := w.s3.PutObject(ctx, input, w.clientOptions...)

	return err
}

func (w *Writer) createMultipartUpload(ctx context.Context) (*string, error) {
	w.logger.DebugContext(ctx, "starting multipart upload")

	input := &s3.CreateMultipartUploadInput{
		Bucket:                    w.input.Bucket,
		Key:                       w.input.Key,
		ACL:                       w.input.ACL,
		BucketKeyEnabled:          w.input.BucketKeyEnabled,
		CacheControl:              w.input.CacheControl,
		ChecksumAlgorithm:         w.input.ChecksumAlgorithm,
		ContentDisposition:        w.input.ContentDisposition,
		ContentEncoding:           w.input.ContentEncoding,
		ContentLanguage:           w.input.ContentLanguage,
		ContentType:               w.input.ContentType,
		ExpectedBucketOwner:       w.input.ExpectedBucketOwner,
		Expires:                   w.input.Expires,
		GrantFullControl:          w.input.GrantFullControl,
		GrantRead:                 w.input.GrantRead,
		GrantReadACP:              w.input.GrantReadACP,
		GrantWriteACP:             w.input.GrantWriteACP,
		Metadata:                  w.input.Metadata,
		ObjectLockLegalHoldStatus: w.input.ObjectLockLegalHoldStatus,
		ObjectLockMode:            w.input.ObjectLockMode,
		ObjectLockRetainUntilDate: w.input.ObjectLockRetainUntilDate,
		RequestPayer:              w.input.RequestPayer,
		SSECustomerAlgorithm:      w.input.SSECustomerAlgorithm,
		SSECustomerKey:            w.input.SSECustomerKey,
		SSECustomerKeyMD5:         w.input.SSECustomerKeyMD5,
		SSEKMSEncryptionContext:   w.input.SSEKMSEncryptionContext,
		SSEKMSKeyId:               w.input.SSEKMSKeyId,
		ServerSideEncryption:      w.input.ServerSideEncryption,
		StorageClass:              w.input.StorageClass,
		Tagging:                   w.input.Tagging,
		WebsiteRedirectLocation:   w.input.WebsiteRedirectLocation,
	}

	res, err := w.s3.CreateMultipartUpload(ctx, input, w.clientOptions...)
	if err != nil {
		return nil, err
	}

	return res.UploadId, nil
}

func (w *Writer) uploadPart(ctx context.Context, uploadID *string, partNr int32, by []byte) (types.CompletedPart, error) {
	w.logger.DebugContext(
		ctx,
		"upload part",
		slog.String("upload_id", *uploadID),
		slog.Int("part_nr", int(partNr)),
		slog.Int("size", len(by)),
	)

	input := &s3.UploadPartInput{
		Bucket:     w.input.Bucket,
		Key:        w.input.Key,
		UploadId:   uploadID,
		PartNumber: &partNr,
		Body:       bytes.NewReader(by),
	}

	res, err := w.s3.UploadPart(ctx, input, w.clientOptions...)
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

func (w *Writer) abortUpload(ctx context.Context, uploadID *string) error {
	w.logger.DebugContext(ctx, "abort upload", slog.String("upload_id", *uploadID))

	input := &s3.AbortMultipartUploadInput{
		Bucket:              w.input.Bucket,
		Key:                 w.input.Key,
		UploadId:            uploadID,
		ExpectedBucketOwner: w.input.ExpectedBucketOwner,
		RequestPayer:        w.input.RequestPayer,
	}

	_, err := w.s3.AbortMultipartUpload(ctx, input, w.clientOptions...)

	return err
}

func (w *Writer) completeUpload(ctx context.Context, uploadID *string) {
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
		Bucket:   w.input.Bucket,
		Key:      w.input.Key,
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
		ExpectedBucketOwner:  w.input.ExpectedBucketOwner,
		RequestPayer:         w.input.RequestPayer,
		SSECustomerAlgorithm: w.input.SSECustomerAlgorithm,
		SSECustomerKey:       w.input.SSECustomerKey,
		SSECustomerKeyMD5:    w.input.SSECustomerKeyMD5,
	}

	_, err := w.s3.CompleteMultipartUpload(ctx, input, w.clientOptions...)

	w.closingErr <- err
}

/*
 * Options
 */

// WithWriterLogger adds a logger for this writer.
func WithWriterLogger(logger *slog.Logger) WriterOption {
	return func(w *Writer) {
		if logger == nil {
			logger = slog.New(slog.DiscardHandler)
		}

		w.logger = logger
	}
}

// WithWriterChunkSize sets the chunksize for this writer.
// If set below the minimal chunk size of 5Mb then it will be set to the minimal chunksize.
func WithWriterChunkSize(size int64) WriterOption {
	return func(w *Writer) {
		if size < MinChunkSize {
			size = MinChunkSize
		}

		w.chunkSize = size
	}
}

// WithWriterConcurrency sets the concurrency amount for this writer.
func WithWriterConcurrency(i int) WriterOption {
	return func(w *Writer) {
		if i < 1 {
			i = 1
		}

		w.concurrency = i
	}
}

// WithWriterRetries sets the retry count for this writer
func WithWriteRetries(i int) WriterOption {
	return func(w *Writer) {
		w.clientOptions = append(w.clientOptions, withS3Retries(i))
	}
}

// WithWriterClientOptions adds s3 client options to the writer opperations
func WithWriterClientOptions(opts ...func(*s3.Options)) WriterOption {
	return func(w *Writer) {
		w.clientOptions = append(w.clientOptions, opts...)
	}
}

// Set ACL after giving the input
func WithWriterACL(acl types.ObjectCannedACL) WriterOption {
	return func(w *Writer) {
		if w.input == nil {
			return
		}

		w.input.ACL = acl
	}
}
