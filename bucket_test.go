package s3io_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/jobstoit/s3io/v2"
)

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

	u := fmt.Sprintf("s3://%s:%s@%s/%s?region=%s&insecure=%s", accessKey, secretKey, endpointURL.Host, bucketName, region, insecure)

	_, err = s3io.OpenURL(ctx, u)
	if err != nil {
		t.Errorf("unexpected error with url '%s': %v", u, err)
	}
}
