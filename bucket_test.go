package s3io_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jobstoit/s3io/v3"
)

func TestOpenUrl(t *testing.T) {
	ctx := t.Context()

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

func TestDelete(t *testing.T) {
	cli := &BucketLoggingClient{}

	bucket := s3io.New("testing", s3io.DefaultChunkSize, s3io.DefaultChunkSize, 1, slog.New(slog.DiscardHandler), cli)

	itemsToDelete := []string{"test1.txt", "test2.txt", "test3.txt"}

	err := bucket.Delete(t.Context(), itemsToDelete...)
	if err != nil {
		t.Errorf("unable to delete items: %s", err.Error())
	}

	if e, a := len(itemsToDelete), len(cli.Invocations); e != a {
		t.Errorf("expected %d but got %d", e, a)
	}
}

// BucketLoggingClient is a test client
type BucketLoggingClient struct {
	mx                    sync.Mutex
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
	b.mx.Lock()
	defer b.mx.Unlock()

	b.Invocations = append(b.Invocations, "DeleteObject")

	return &s3.DeleteObjectOutput{}, nil
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
