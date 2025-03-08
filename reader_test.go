package s3io_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"reflect"
	"regexp"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jobstoit/s3io/v3"
)

func TestImplementFSFile(t *testing.T) {
	t.Parallel()

	var _ fs.File = &s3io.Reader{}
}

func TestImplementIOReader(t *testing.T) {
	t.Parallel()

	var _ io.Reader = &s3io.Reader{}
}

func TestReaderSinglePart(t *testing.T) {
	c, invocations, ranges := newDownloadRangeClient(buf2MB)

	rd := s3io.NewReader(t.Context(), c, &s3.GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
	}, s3io.WithReaderConcurrency(1))

	n, err := io.Copy(io.Discard, rd)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if e, a := int64(len(buf2MB)), n; e != a {
		t.Errorf("expected %d buffer length, got %d", e, a)
	}

	if e, a := 2, *invocations; e != a {
		t.Errorf("expect %v API calls, got %v", e, a)
	}

	expectRngs := []string{"bytes=0-0", "bytes=0-2097152"}
	if e, a := expectRngs, *ranges; !reflect.DeepEqual(e, a) {
		t.Errorf("expect %v ranges, got %v", e, a)
	}
}

func TestReaderMultiPart(t *testing.T) {
	c, invocations, ranges := newDownloadRangeClient(buf12MB)

	rd := s3io.NewReader(t.Context(), c, &s3.GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
	})

	n, err := io.Copy(io.Discard, rd)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if e, a := int64(len(buf12MB)), n; e != a {
		t.Errorf("expected %d buffer length, got %d", e, a)
	}

	if e, a := 4, *invocations; e != a {
		t.Errorf("expect %v API calls, got %v", e, a)
	}

	expectRngs := []string{"bytes=0-0", "bytes=0-5242880", "bytes=5242881-10485761", "bytes=10485762-12582912"}
	if e, a := expectRngs, *ranges; !reflect.DeepEqual(e, a) {
		t.Errorf("expect %v ranges, got %v", e, a)
	}
}

type downloadCaptureClient struct {
	GetObjectFn          func(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	GetObjectInvocations int

	RetrievedRanges []string

	lock sync.Mutex
}

func (c *downloadCaptureClient) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.GetObjectInvocations++

	if params.Range != nil {
		c.RetrievedRanges = append(c.RetrievedRanges, aws.ToString(params.Range))
	}

	return c.GetObjectFn(ctx, params, optFns...)
}

var rangeValueRegex = regexp.MustCompile(`bytes=(\d+)-(\d+)`)

func parseRange(rangeValue string) (start, fin int64) {
	rng := rangeValueRegex.FindStringSubmatch(rangeValue)
	start, _ = strconv.ParseInt(rng[1], 10, 64)
	fin, _ = strconv.ParseInt(rng[2], 10, 64)
	return start, fin
}

func newDownloadRangeClient(data []byte) (*downloadCaptureClient, *int, *[]string) {
	capture := &downloadCaptureClient{}

	capture.GetObjectFn = func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
		start, fin := parseRange(aws.ToString(params.Range))
		fin++

		if fin >= int64(len(data)) {
			fin = int64(len(data))
		}

		bodyBytes := data[start:fin]

		return &s3.GetObjectOutput{
			Body:          io.NopCloser(bytes.NewReader(bodyBytes)),
			ContentRange:  aws.String(fmt.Sprintf("bytes %d-%d/%d", start, fin-1, len(data))),
			ContentLength: aws.Int64(int64(len(bodyBytes))),
		}, nil
	}

	return capture, &capture.GetObjectInvocations, &capture.RetrievedRanges
}
