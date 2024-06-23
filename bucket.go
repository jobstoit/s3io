package s3io

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/url"
	"path"
	"strings"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

var (
	ErrInvalidScheme BucketURIError = "url doesn't start with s3 scheme"
	ErrNoBucketName  BucketURIError = "no bucketname"
	ErrNoCredentials BucketURIError = "missing credentials"
)

// BucketURIError
type BucketURIError string

func (b BucketURIError) Error() string {
	return fmt.Sprintf("invalid bucket uri: '%s'", string(b))
}

// Bucket is an abstraction to interact with objects in your S3 bucket
type Bucket struct {
	name           string
	readChunkSize  int64
	writeChunkSize int64
	concurrency    int
	logger         *slog.Logger
	cli            *s3.Client
}

// OpenURL opens the bucket with all the connection options in the url.
// The url is written as: s3://access-key:access-secret@host/bucketname?region=us-east.
// the url assumes the host has a https protocol unless the "insecure" query param is set to "true".
// to crete the bucket if it doesn't exist set the "create" query param to "true".
func OpenURL(ctx context.Context, u string, opts ...BucketOption) (*Bucket, error) {
	pu, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	if pu.Scheme != "s3" {
		return nil, ErrInvalidScheme
	}

	bucketName := ""
	if pathChunks := strings.Split(pu.Path, "/"); len(pathChunks) > 1 {
		bucketName = pathChunks[1]
	}

	if bucketName == "" {
		return nil, ErrNoBucketName
	}

	accessSecret, ok := pu.User.Password()
	if !ok {
		return nil, ErrNoCredentials
	}

	protocol := "https"
	if pu.Query().Get("insecure") == "true" {
		protocol = "http"
	}

	urlOpts := []BucketOption{
		WithBucketHost(fmt.Sprintf("%s://%s", protocol, pu.Host), pu.Query().Get("region"), true),
		WithBucketCredentials(pu.User.Username(), accessSecret),
	}

	if pu.Query().Get("create") == "true" {
		urlOpts = append(urlOpts, WithBucketCreateIfNotExists())
	}

	return OpenBucket(ctx, bucketName, append(urlOpts, opts...)...)
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
		closed:      &atomic.Bool{},
	}

	return rd, nil
}

// Glob is an implementation of fs.GlobFS
func (b *Bucket) Glob(pattern string) ([]string, error) {
	prefix := strings.Split(pattern, "*")[0]
	prefix = strings.Split(prefix, "[")[0]

	objs, err := b.List(context.Background(), prefix)
	if err != nil {
		return nil, err
	}

	matches := make([]string, 0)
	for _, obj := range objs {
		if obj.Key == nil {
			continue
		}
		key := *obj.Key
		ok, err := path.Match(pattern, *obj.Key)
		if err != nil {
			return matches, err
		}

		if ok && key != "" {
			matches = append(matches, key)
		}
	}

	return matches, nil
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
	pre := &prefix
	if prefix == "" {
		pre = nil
	}

	objs := make([]types.Object, 0)
	continuationToken := (*string)(nil)
	for {
		res, err := b.cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &b.name,
			Prefix:            pre,
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return objs, err
		}

		objs = append(objs, res.Contents...)
		if res.NextContinuationToken == nil {
			return objs, nil
		}

		continuationToken = res.NextContinuationToken
	}
}

// NewReader returns a new ObjectReader to do io.Reader opperations with your s3 object
func (b *Bucket) NewReader(ctx context.Context, key string, opts ...ObjectReaderOption) io.Reader {
	input := &s3.GetObjectInput{
		Bucket: &b.name,
		Key:    &key,
	}

	bucketOpts := []ObjectReaderOption{
		WithReaderChunkSize(b.readChunkSize),
		WithReaderConcurrency(b.concurrency),
		WithReaderLogger(b.logger),
	}

	return NewObjectReader(ctx, b.cli, input, append(bucketOpts, opts...)...)
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

	bucketOpts := []ObjectWriterOption{
		WithWriterChunkSize(b.writeChunkSize),
		WithWriterConcurrency(b.concurrency),
		WithWriterLogger(b.logger),
	}

	return NewObjectWriter(ctx, b.cli, input, append(bucketOpts, opts...)...)
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
