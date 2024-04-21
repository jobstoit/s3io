package s3io

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type ObjectWriter struct {
	ctx         context.Context
	cli         *s3.Client
	bucket      string
	key         string
	wr          *io.PipeWriter
	logger      *slog.Logger
	retries     int
	chunkSize   int
	concurrency int
	alc         types.ObjectCannedACL

	mux        sync.Mutex
	wg         sync.WaitGroup
	parts      []types.CompletedPart
	closingErr chan error
}

type WriterOption func(*ObjectWriter) error

func WriterOptions(ops ...WriterOption) WriterOption {
	return func(w *ObjectWriter) error {
		for _, op := range ops {
			if err := op(w); err != nil {
				return err
			}
		}

		return nil
	}
}

// Write is the io.Writer implementation of the ObjectWriter
func (w *ObjectWriter) Write(p []byte) (int, error) {
	if w.wr == nil {
		if err := w.preWrite(); err != nil {
			return 0, err
		}
	}

	return w.wr.Write(p)
}

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
		err = w.abortUpload(ctx, uploadID)
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

	var err error
	for range w.retries {
		_, err = w.cli.PutObject(ctx, input)
		if err == nil {
			return err
		}
	}

	return err
}

func (w *ObjectWriter) createMultipartUpload(ctx context.Context) (*string, error) {
	w.logger.DebugContext(ctx, "starting multipart upload")

	input := &s3.CreateMultipartUploadInput{
		Bucket: &w.bucket,
		Key:    &w.key,
		ACL:    w.alc,
	}

	var res *s3.CreateMultipartUploadOutput
	var err error
	for range w.retries {
		res, err = w.cli.CreateMultipartUpload(ctx, input)
		if err == nil {
			return res.UploadId, nil
		}
	}

	return nil, err
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

	var res *s3.UploadPartOutput
	var err error
	for range w.retries {
		res, err = w.cli.UploadPart(ctx, input)
		if err == nil {
			return types.CompletedPart{
				ChecksumCRC32:  res.ChecksumCRC32,
				ChecksumCRC32C: res.ChecksumCRC32C,
				ChecksumSHA1:   res.ChecksumSHA1,
				ChecksumSHA256: res.ChecksumSHA256,
				ETag:           res.ETag,
				PartNumber:     &partNr,
			}, nil
		}
	}

	return types.CompletedPart{}, err
}

func (w *ObjectWriter) abortUpload(ctx context.Context, uploadID *string) error {
	w.logger.DebugContext(ctx, "abort upload", slog.String("upload_id", *uploadID))

	input := &s3.AbortMultipartUploadInput{
		Bucket:   &w.bucket,
		Key:      &w.key,
		UploadId: uploadID,
	}

	var err error
	for range w.retries {
		_, err := w.cli.AbortMultipartUpload(ctx, input)
		if err == nil {
			break
		}
	}

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

	// TODO: check if you need to sort
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

	var err error
	for range w.retries {
		_, err = w.cli.CompleteMultipartUpload(ctx, input)
		if err == nil {
			break
		}
	}

	w.closingErr <- err
}

/*
 * Options
 */

// WithWriterLogger adds a logger for this writer
func WithWriterLogger(logger *slog.Logger) WriterOption {
	return func(w *ObjectWriter) error {
		w.logger = logger

		return nil
	}
}

// WithWriterChunkSize sets the chunksize for this writer
func WithWriterChunkSize(size uint) WriterOption {
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
func WithWriterConcurrency(i uint8) WriterOption {
	return func(w *ObjectWriter) error {
		w.concurrency = int(i)

		return nil
	}
}

// WithWriterRetries sets the retry count for this writer
func WithWriteRetries(i uint8) WriterOption {
	return func(w *ObjectWriter) error {
		w.retries = int(i)

		return nil
	}
}

// WithWriterACL sets the ACL for the object thats written
func WithWriterACL(acl types.ObjectCannedACL) WriterOption {
	return func(w *ObjectWriter) error {
		w.alc = acl

		return nil
	}
}
