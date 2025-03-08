package s3io

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
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

type Bucket interface {
	ExistsBucketAPI
	DeleteBucketBucketAPI
	DeleteBucketAPI
	ListBucketAPI
	GetBucketAPI
	PutBucketAPI
	fmt.Stringer
}

type ExistsBucketAPI interface {
	Exists(ctx context.Context, key string) (bool, error)
}

type DeleteBucketBucketAPI interface {
	DeleteBucket(ctx context.Context) error
}

type DeleteBucketAPI interface {
	Delete(ctx context.Context, keys ...string) error
}

type ListBucketAPI interface {
	List(ctx context.Context, prefix string) ([]types.Object, error)
}

type GetBucketAPI interface {
	Get(ctx context.Context, key string, opts ...ReaderOption) io.Reader
}

type PutBucketAPI interface {
	Put(ctx context.Context, key string, opts ...WriterOption) io.WriteCloser
}

// bucket is an abstraction to interact with objects in your S3 bucket
type bucket struct {
	name           string
	readChunkSize  int64
	writeChunkSize int64
	concurrency    int
	logger         *slog.Logger
	cli            BucketApiClient
}

// New returns a new bucket instance.
// For normal operations use Open instead as this will connect and verify the bucket.
func New(
	name string,
	readChunkSize, writeChunkSize int64,
	concurrency int,
	logger *slog.Logger,
	cli BucketApiClient,
) Bucket {
	return &bucket{
		name,
		readChunkSize,
		writeChunkSize,
		concurrency,
		logger,
		cli,
	}
}

// OpenURL opens the bucket with all the connection options in the url.
// The url is written as: s3://access-key:access-secret@host/bucketname?region=us-east&pathstyle=true
//
// The url assumes the host has a https protocol unless the "insecure" query param is set to "true".
// To create the bucket if it doesn't exist set the "create" query param to "true".
// To use the pathstyle url set "pathstyle" to "true".
func OpenURL(ctx context.Context, u string, opts ...BucketOption) (Bucket, error) {
	pu, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	if pu.Scheme != "s3" {
		return nil, ErrInvalidScheme
	}

	query := pu.Query()

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

	urlOpts := []BucketOption{
		WithBucketCredentials(pu.User.Username(), accessSecret),
	}

	if pu.Host != "" {
		pathStyle := query.Get("pathstyle") == "true"

		protocol := "https"
		if query.Get("insecure") == "true" {
			protocol = "http"
		}

		urlOpts = append(urlOpts, WithBucketHost(fmt.Sprintf("%s://%s", protocol, pu.Host), query.Get("region"), pathStyle))
	}

	if query.Get("create") == "true" {
		urlOpts = append(urlOpts, WithBucketCreateIfNotExists())
	}

	return Open(ctx, bucketName, append(urlOpts, opts...)...)
}

// Open returns a bucket to interact with.
func Open(ctx context.Context, name string, opts ...BucketOption) (Bucket, error) {
	builder := newBucketBuilder()
	BucketOptions(opts...)(builder)

	return builder.Build(ctx, name)
}

// Exists returns a a boolean indicating whether the requested object exists.
func (b *bucket) Exists(ctx context.Context, key string) (bool, error) {
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

// DeleteBucket deletes the whole bucket
func (b *bucket) DeleteBucket(ctx context.Context) error {
	_, err := b.cli.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(b.name),
	})

	return err
}

// Delete deletes the given object keys
func (b *bucket) Delete(ctx context.Context, keys ...string) error {
	var err error

	length := len(keys)
	if length == 0 {
		return nil
	}

	count := 0
	errCh := make(chan error, b.concurrency)
	for _, key := range keys {
		go b.deleteAsync(ctx, errCh, key)
	}

	for res := range errCh {
		err = errors.Join(err, res)

		count++
		if count == length {
			break
		}
	}

	return err
}

func (b *bucket) deleteAsync(ctx context.Context, errCh chan error, key string) {
	_, err := b.cli.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &b.name,
		Key:    &key,
	})

	errCh <- err
}

// List returns a list of objects details.
//
// Use the prefix "/" to list all the objects in the bucket.
func (b *bucket) List(ctx context.Context, prefix string) ([]types.Object, error) {
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

// Get returns a new ObjectReader to do io.Reader opperations with your s3 object
func (b *bucket) Get(ctx context.Context, key string, opts ...ReaderOption) io.Reader {
	input := &s3.GetObjectInput{
		Bucket: &b.name,
		Key:    &key,
	}

	bucketOpts := []ReaderOption{
		WithReaderChunkSize(b.readChunkSize),
		WithReaderConcurrency(b.concurrency),
		WithReaderLogger(b.logger),
	}

	return NewReader(ctx, b.cli, input, append(bucketOpts, opts...)...)
}

// Put returns a new ObjectWriter to do io.Write opparations with your s3 object
func (b *bucket) Put(ctx context.Context, key string, opts ...WriterOption) io.WriteCloser {
	input := &s3.PutObjectInput{
		Bucket: &b.name,
		Key:    &key,
	}

	bucketOpts := []WriterOption{
		WithWriterChunkSize(b.writeChunkSize),
		WithWriterConcurrency(b.concurrency),
		WithWriterLogger(b.logger),
	}

	return NewWriter(ctx, b.cli, input, append(bucketOpts, opts...)...)
}

// Name returns the specified bucket's name
func (b *bucket) String() string {
	return b.name
}
