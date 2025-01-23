package s3io_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jobstoit/s3io/v2"
)

func TestBucketInterface(t *testing.T) {
	var _ s3io.BucketAPI = (*s3io.Bucket)(nil)
}

func TestOpenUrl(t *testing.T) {
	ctx := context.Background()

	_, err := s3io.OpenURL(ctx, "http://eliot@example.com/test")
	if !errors.Is(err, s3io.ErrInvalidScheme) {
		t.Errorf("should have invalid scheme err but got %v", err)
	}

	_, err = s3io.OpenURL(ctx, "s3://access-key:access-secret@example.com")
	if !errors.Is(err, s3io.ErrNoBucketName) {
		t.Errorf("should have no bucketname err but got %v", err)
	}

	_, err = s3io.OpenURL(ctx, "s3://eliot@example.com/test")
	if !errors.Is(err, s3io.ErrNoCredentials) {
		t.Errorf("should have no credentials err but got %v", err)
	}

	region := envOrDefault("AWS_REGION", "local")
	bucketName := envOrDefault("AWS_BUCKET_NAME", "jobbitz-testing")
	accessKey := envOrDefault("AWS_ACCESS_KEY_ID", "access_key")
	secretKey := envOrDefault("AWS_SECRET_ACCESS_KEY", "secret_key")
	endpoint := envOrDefault("AWS_S3_ENDPOINT", "http://localhost:9000")
	endpointURL, _ := url.Parse(endpoint)

	insecure := "false"
	if endpointURL.Scheme == "http" {
		insecure = "true"
	}

	u := fmt.Sprintf("s3://%s:%s@%s/%s?region=%s&insecure=%s&create=true", accessKey, secretKey, endpointURL.Host, bucketName, region, insecure)

	_, err = s3io.OpenURL(ctx, u)
	if err != nil {
		t.Errorf("unexpected error with url '%s': %v", u, err)
	}
}

func TestReadFrom(t *testing.T) {
	ctx := context.Background()
	s, ops, args := NewUploadLoggingClient(nil)

	cli := &BucketLoggingClient{
		uploadClient: s,
	}

	bucket := s3io.NewRawBucket(
		"testing",
		s3io.DefaultChunkSize,
		s3io.DefaultChunkSize,
		1, noopLogger, cli)

	var amount int64 = 1024 * 1024 * 12

	c, err := bucket.ReadFrom(
		ctx,
		"path/to/item",
		io.LimitReader(rand.Reader, amount),
		s3io.WithWriterConcurrency(1),
		s3io.WithWriterChunkSize(1024*1024*7),
	)
	if err != nil {
		t.Fatalf("error reading from: %v", err)
	}

	if e, a := amount, c; e != a {
		t.Errorf("expected %d not equal to actual %d", e, a)
	}

	vals := []string{"CreateMultipartUpload", "UploadPart", "UploadPart", "CompleteMultipartUpload"}
	if !reflect.DeepEqual(vals, *ops) {
		t.Errorf("expect %v, got %v", vals, *ops)
	}

	// Part lengths
	if e, a := int64(1024*1024*7), getReaderLength((*args)[1].(*s3.UploadPartInput).Body); e != a {
		t.Errorf("expect %d, got %d", e, a)
	}

	if e, a := int64(1024*1024*5), getReaderLength((*args)[2].(*s3.UploadPartInput).Body); e != a {
		t.Errorf("expect %d, got %d", e, a)
	}
}

func TestWriteTo(t *testing.T) {
	ctx := context.Background()
	dlcli, invocations, ranges := newDownloadRangeClient(buf12MB)

	cli := &BucketLoggingClient{
		downloadCaptureClient: dlcli,
	}

	bucket := s3io.NewRawBucket("testing", s3io.DefaultChunkSize, s3io.DefaultChunkSize, 1, noopLogger, cli)

	buf := &bytes.Buffer{}
	_, err := bucket.WriteTo(
		ctx,
		"path/to/item",
		buf,
		s3io.WithReaderConcurrency(1),
	)
	if err != nil {
		t.Fatalf("unable to write to: %v", err)
	}

	if e, a := len(buf12MB), buf.Len(); e != a {
		t.Errorf("expected %d but got %d", e, a)
	}

	if e, a := 4, *invocations; e != a {
		t.Errorf("expect %v API calls, got %v", e, a)
	}

	expectRngs := []string{"bytes=0-0", "bytes=0-5242880", "bytes=5242881-10485761", "bytes=10485762-12582912"}
	if e, a := expectRngs, *ranges; !reflect.DeepEqual(e, a) {
		t.Errorf("expect %v ranges, got %v", e, a)
	}
}

// BucketLoggingClient is a test client
type BucketLoggingClient struct {
	Invocations           []string
	paths                 []string
	uploadClient          *UploadLoggingClient
	downloadCaptureClient *downloadCaptureClient
}

// HeadObject is an implementation of s3.HeadObjectAPIClient
func (b *BucketLoggingClient) HeadObject(
	_ context.Context,
	input *s3.HeadObjectInput,
	optFns ...func(*s3.Options),
) (*s3.HeadObjectOutput, error) {
	b.Invocations = append(b.Invocations, "HeadObject")

	var err error
	if !strings.Contains(strings.Join(b.paths, " "), aws.ToString(input.Key)) {
		err = &types.NotFound{
			Message: aws.String("object not found"),
		}
	}

	return &s3.HeadObjectOutput{
		ContentType: aws.String("text/plain"),
	}, err
}

// ListObjectsV2 is an implementation of s3.ListObjectsV2APIClient
func (b *BucketLoggingClient) ListObjectsV2(
	_ context.Context,
	input *s3.ListObjectsV2Input,
	optFns ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	b.Invocations = append(b.Invocations, "ListObjectsV2")

	objs := make([]types.Object, len(b.paths))
	for i, p := range b.paths {
		objs[i] = types.Object{
			Key: aws.String(p),
		}
	}

	out := &s3.ListObjectsV2Output{
		Contents: objs,
	}

	return out, nil
}

func (b *BucketLoggingClient) DeleteBucket(_ context.Context, _ *s3.DeleteBucketInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketOutput, error) {
	panic("not implemented")
}

func (b *BucketLoggingClient) DeleteObjects(_ context.Context, input *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	panic("not implemented")
}

func (b *BucketLoggingClient) DeleteObject(_ context.Context, input *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	panic("not implemented")
}

func (b *BucketLoggingClient) GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return b.downloadCaptureClient.GetObject(ctx, input, optFns...)
}

func (b *BucketLoggingClient) PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return b.uploadClient.PutObject(ctx, input, optFns...)
}

func (b *BucketLoggingClient) UploadPart(ctx context.Context, input *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return b.uploadClient.UploadPart(ctx, input, optFns...)
}

func (b *BucketLoggingClient) CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return b.uploadClient.CreateMultipartUpload(ctx, input, optFns...)
}

func (b *BucketLoggingClient) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return b.uploadClient.CompleteMultipartUpload(ctx, input, optFns...)
}

func (b *BucketLoggingClient) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return b.uploadClient.AbortMultipartUpload(ctx, input, optFns...)
}
