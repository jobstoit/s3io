package s3io

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// Bucket is an abstraction to interact with objects in your S3 bucket
type Bucket struct {
	name           string
	readChunkSize  int64
	writeChunkSize int64
	concurrency    int
	logger         *slog.Logger
	cli            *s3.Client
}

// OpenBucket returns a bucket to interact with.
func OpenBucket(ctx context.Context, name string, opts ...BucketOption) (*Bucket, error) {
	builder := newBucketBuilder()
	BucketOptions(opts...)(builder)

	return builder.Build(ctx, name)
}

// OpenBucketkwithClient returns a bucket to interact with.
func OpenBucketkwithClient(ctx context.Context, cli *s3.Client, name string, opts ...BucketOption) (*Bucket, error) {
	builder := newBucketBuilder()
	BucketOptions(append(opts, WithBucketCli(cli))...)(builder)

	return builder.Build(ctx, name)
}

// Open is the fs.FS implenentation for the Bucket.
//
// Open returns an fs.ErrInvalid argument if there was an error in checking if the file exists
// and an fs.ErrNotExists if the object doesn't exist
func (b *Bucket) Open(name string) (fs.File, error) {
	ctx := context.Background()

	exists, err := b.Exists(ctx, name)
	if err != nil {
		return nil, fs.ErrInvalid
	}

	if !exists {
		return nil, fs.ErrNotExist
	}

	input := &s3.GetObjectInput{
		Bucket: &b.name,
		Key:    &name,
	}

	rd := &ObjectReader{
		ctx:         ctx,
		s3:          b.cli,
		input:       input,
		retries:     defaultRetries,
		chunkSize:   b.readChunkSize,
		concurrency: b.concurrency,
		logger:      b.logger,
	}

	return rd, nil
}

// Exists returns a a boolean indicating whether the requested object exists.
func (b *Bucket) Exists(ctx context.Context, key string) (bool, error) {
	input := &s3.HeadObjectInput{
		Bucket: &b.name,
		Key:    &key,
	}

	if _, err := b.cli.HeadObject(ctx, input); err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				return false, nil
			default:
				return false, err
			}
		}
	}

	return true, nil
}

// Delete deletes the given object keys
func (b *Bucket) Delete(ctx context.Context, keys ...string) error {
	var err error

	length := len(keys)
	if length == 0 {
		return nil
	}

	if length == 1 {
		_, err = b.cli.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &b.name,
			Key:    &keys[0],
		})

		return err
	}

	if length > deleteLimit {
		err = errors.Join(err, b.Delete(ctx, keys[deleteLimit:]...))

		length = deleteLimit
	}

	objs := make([]types.ObjectIdentifier, length)
	for i, key := range keys {
		objs[i] = types.ObjectIdentifier{
			Key: &key,
		}
	}

	_, derr := b.cli.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: &b.name,
		Delete: &types.Delete{
			Objects: objs,
		},
	})

	return errors.Join(err, derr)
}

// List returns a list of objects details.
//
// Use the prefix "/" to list all the objects in the bucket.
func (b *Bucket) List(ctx context.Context, prefix string) ([]types.Object, error) {
	res, err := b.cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &b.name,
		Prefix: &prefix,
	})
	if err != nil {
		return nil, err
	}

	return res.Contents, nil
}

// NewReader returns a new ObjectReader to do io.Reader opperations with your s3 object
func (b *Bucket) NewReader(ctx context.Context, key string, opts ...ObjectReaderOption) io.Reader {
	input := &s3.GetObjectInput{
		Bucket: &b.name,
		Key:    &key,
	}

	rd := &ObjectReader{
		ctx:         ctx,
		s3:          b.cli,
		input:       input,
		retries:     defaultRetries,
		chunkSize:   b.readChunkSize,
		concurrency: b.concurrency,
		logger:      b.logger,
	}

	ObjectReaderOptions(opts...)(rd)

	return rd
}

// ReadAll reads all the bytes of the given object
func (b *Bucket) ReadAll(ctx context.Context, key string, opts ...ObjectReaderOption) ([]byte, error) {
	rd := b.NewReader(ctx, key, opts...)

	return io.ReadAll(rd)
}

// NewWriter returns a new ObjectWriter to do io.Write opparations with your s3 object
func (b *Bucket) NewWriter(ctx context.Context, key string, opts ...ObjectWriterOption) io.WriteCloser {
	input := &s3.PutObjectInput{
		Bucket: &b.name,
		Key:    &key,
	}

	wr := &ObjectWriter{
		ctx:         ctx,
		s3:          b.cli,
		input:       input,
		chunkSize:   b.writeChunkSize,
		concurrency: b.concurrency,
		logger:      b.logger,

		closingErr: make(chan error, 1),
	}

	ObjectWriterOptions(opts...)(wr)

	return wr
}

// WriteFrom writes all the bytes from the reader into the given object
func (b *Bucket) WriteFrom(ctx context.Context, key string, from io.Reader, opts ...ObjectWriterOption) (int64, error) {
	wr := b.NewWriter(ctx, key, opts...)
	defer wr.Close()

	n, err := io.Copy(wr, from)
	if err != nil {
		return n, err
	}

	return n, wr.Close()
}

// WriteAll writes all the given bytes into the given object
func (b *Bucket) WriteAll(ctx context.Context, key string, p []byte, opts ...ObjectWriterOption) (int, error) {
	wr := b.NewWriter(ctx, key, opts...)
	defer wr.Close()

	n, err := wr.Write(p)
	if err != nil {
		return n, err
	}

	return n, wr.Close()
}

// Client returns the s3 client the Bucket uses
func (b *Bucket) Client() *s3.Client {
	return b.cli
}

// Name returns the specified bucket's name
func (b *Bucket) Name() string {
	return b.name
}
