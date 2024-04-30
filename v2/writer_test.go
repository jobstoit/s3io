package s3io_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jobstoit/s3io"
)

func TestObjectWriterSingePartUpload(t *testing.T) {
	s, ops, args := NewUploadLoggingClient(nil)

	wr := s3io.NewObjectWriter(context.Background(), s, &s3.PutObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
	},
		s3io.WithWriterChunkSize(1024*1024*7),
		s3io.WithWriterConcurrency(1),
	)

	if _, err := wr.Write(buf2MB); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := wr.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	vals := []string{"PutObject"}
	if !reflect.DeepEqual(vals, *ops) {
		t.Errorf("expect %v, got %v", vals, *ops)
	}

	// Part lengths
	if e, a := int64(1024*1024*2), getReaderLength((*args)[0].(*s3.PutObjectInput).Body); e != a {
		t.Errorf("expect %d, got %d", e, a)
	}
}

func TestObjectWriterMultipartUpload(t *testing.T) {
	s, ops, args := NewUploadLoggingClient(nil)

	wr := s3io.NewObjectWriter(context.Background(), s, &s3.PutObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
	},
		s3io.WithWriterChunkSize(1024*1024*7),
		s3io.WithWriterConcurrency(1),
	)

	if _, err := wr.Write(buf12MB); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := wr.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
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

// UploadLoggingClient is a mock client that can be used to record and stub responses for testing the manager.Uploader.
type UploadLoggingClient struct {
	Invocations []string
	Params      []interface{}

	ConsumeBody bool

	PutObjectFn               func(*UploadLoggingClient, *s3.PutObjectInput) (*s3.PutObjectOutput, error)
	UploadPartFn              func(*UploadLoggingClient, *s3.UploadPartInput) (*s3.UploadPartOutput, error)
	CreateMultipartUploadFn   func(*UploadLoggingClient, *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUploadFn func(*UploadLoggingClient, *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUploadFn    func(*UploadLoggingClient, *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error)

	ignoredOperations []string

	PartNum int
	m       sync.Mutex
}

func (u *UploadLoggingClient) simulateHTTPClientOption(optFns ...func(*s3.Options)) error {
	o := s3.Options{
		HTTPClient: httpDoFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				Request: request,
			}, nil
		}),
	}

	for _, fn := range optFns {
		fn(&o)
	}

	_, err := o.HTTPClient.Do(&http.Request{URL: &url.URL{
		Scheme:   "https",
		Host:     "mock.amazonaws.com",
		Path:     "/key",
		RawQuery: "foo=bar",
	}})
	if err != nil {
		return err
	}

	return nil
}

type httpDoFunc func(*http.Request) (*http.Response, error)

func (f httpDoFunc) Do(r *http.Request) (*http.Response, error) {
	return f(r)
}

func (u *UploadLoggingClient) traceOperation(name string, params interface{}) {
	if contains(u.ignoredOperations, name) {
		return
	}

	u.Invocations = append(u.Invocations, name)
	u.Params = append(u.Params, params)
}

// PutObject is the S3 PutObject API.
func (u *UploadLoggingClient) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	u.m.Lock()
	defer u.m.Unlock()

	if u.ConsumeBody {
		io.Copy(io.Discard, params.Body)
	}

	u.traceOperation("PutObject", params)
	if err := u.simulateHTTPClientOption(optFns...); err != nil {
		return nil, err
	}

	if u.PutObjectFn != nil {
		return u.PutObjectFn(u, params)
	}

	return &s3.PutObjectOutput{
		VersionId: aws.String("VERSION-ID"),
	}, nil
}

// UploadPart is the S3 UploadPart API.
func (u *UploadLoggingClient) UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	u.m.Lock()
	defer u.m.Unlock()

	if u.ConsumeBody {
		io.Copy(io.Discard, params.Body)
	}

	u.traceOperation("UploadPart", params)
	if err := u.simulateHTTPClientOption(optFns...); err != nil {
		return nil, err
	}

	u.PartNum++

	if u.UploadPartFn != nil {
		return u.UploadPartFn(u, params)
	}

	return &s3.UploadPartOutput{
		ETag: aws.String(fmt.Sprintf("ETAG%d", u.PartNum)),
	}, nil
}

// CreateMultipartUpload is the S3 CreateMultipartUpload API.
func (u *UploadLoggingClient) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	u.m.Lock()
	defer u.m.Unlock()

	u.traceOperation("CreateMultipartUpload", params)
	if err := u.simulateHTTPClientOption(optFns...); err != nil {
		return nil, err
	}

	if u.CreateMultipartUploadFn != nil {
		return u.CreateMultipartUploadFn(u, params)
	}

	return &s3.CreateMultipartUploadOutput{
		UploadId: aws.String("UPLOAD-ID"),
	}, nil
}

// CompleteMultipartUpload is the S3 CompleteMultipartUpload API.
func (u *UploadLoggingClient) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	u.m.Lock()
	defer u.m.Unlock()

	u.traceOperation("CompleteMultipartUpload", params)
	if err := u.simulateHTTPClientOption(optFns...); err != nil {
		return nil, err
	}

	if u.CompleteMultipartUploadFn != nil {
		return u.CompleteMultipartUploadFn(u, params)
	}

	return &s3.CompleteMultipartUploadOutput{
		Location:  aws.String("http://location"),
		VersionId: aws.String("VERSION-ID"),
	}, nil
}

// AbortMultipartUpload is the S3 AbortMultipartUpload API.
func (u *UploadLoggingClient) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	u.m.Lock()
	defer u.m.Unlock()

	u.traceOperation("AbortMultipartUpload", params)
	if err := u.simulateHTTPClientOption(optFns...); err != nil {
		return nil, err
	}

	if u.AbortMultipartUploadFn != nil {
		return u.AbortMultipartUploadFn(u, params)
	}

	return &s3.AbortMultipartUploadOutput{}, nil
}

// NewUploadLoggingClient returns a new UploadLoggingClient.
func NewUploadLoggingClient(ignoreOps []string) (*UploadLoggingClient, *[]string, *[]interface{}) {
	client := &UploadLoggingClient{
		ignoredOperations: ignoreOps,
	}

	return client, &client.Invocations, &client.Params
}

func contains(src []string, s string) bool {
	for _, v := range src {
		if s == v {
			return true
		}
	}
	return false
}

// getReaderLength discards the bytes from reader and returns the length
func getReaderLength(r io.Reader) int64 {
	n, _ := io.Copy(io.Discard, r)
	return n
}
