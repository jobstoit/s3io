package s3io_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jobstoit/s3io/v3"
)

var (
	buf12MB = make([]byte, 1024*1024*12)
	buf2MB  = make([]byte, 1024*1024*2)

	createBucketMux = &sync.Mutex{}
)

func TestReadWrite(t *testing.T) {
	t.Parallel()

	s3Cli, err := getTestS3Client()
	if err != nil {
		t.Fatalf("error getting test manager")
	}

	bucket, err := getTestBucket(s3Cli)
	if err != nil {
		t.Fatalf("error getting test bucket: %v", err)
	}

	testFile(t, bucket, "exact smallish size file", "data/test-exact-smallish.txt", io.LimitReader(rand.Reader, 1024*1024*10))
	testFile(t, bucket, "smallish size file", "data/test-smallish.txt", io.LimitReader(rand.Reader, 1024*1024*18+291))
	testFile(t, bucket, "small size file", "data/test-small.txt", io.LimitReader(rand.Reader, 1024*512+342))
	testFile(t, bucket, "medium size file", "data/test-medium.txt", io.LimitReader(rand.Reader, 1024*1024*358+572))
	testFile(t, bucket, "large size file", "data/test-large.txt", io.LimitReader(rand.Reader, 1024*1024*1024*2+812))
}

func BenchmarkAgainstManager(b *testing.B) {
	ctx := b.Context()

	s3Cli, err := getTestS3Client()
	if err != nil {
		b.Fatalf("error getting test manager")
	}

	bucket, err := getTestBucket(s3Cli)
	if err != nil {
		b.Fatalf("error getting test bucket: %v", err)
	}

	fileName := "benchmark.txt"
	bucketName := bucket.String()
	var fileSize int64 = 1024 * 1024 * 120 // 120Mb

	b.Run("fs upload", func(b *testing.B) {
		wr := bucket.Put(ctx, fileName, s3io.WithWriterLogger(slog.New(slog.DiscardHandler)))
		defer wr.Close()

		_, err = io.Copy(wr, io.LimitReader(rand.Reader, fileSize))
		if err != nil {
			b.Fatalf("error writing file: %v", err)
		}

		if err = wr.Close(); err != nil {
			b.Errorf("error closing file: %v", err)
		}
	})

	b.Run("manager upload", func(b *testing.B) {
		uploader := manager.NewUploader(s3Cli)

		_, err := uploader.Upload(b.Context(), &s3.PutObjectInput{
			Bucket: &bucketName,
			Key:    aws.String(fileName),
			Body:   io.LimitReader(rand.Reader, fileSize),
		})
		if err != nil {
			b.Errorf("error uploading file: %v", err)
		}
	})

	b.Run("fs download", func(b *testing.B) {
		rd := bucket.Get(ctx, fileName, s3io.WithReaderLogger(slog.New(slog.DiscardHandler)))

		buf := io.Discard
		_, err = io.Copy(buf, rd)
		if err != nil {
			b.Errorf("error reading file: %v", err)
		}
	})

	b.Run("manager download", func(b *testing.B) {
		downloader := manager.NewDownloader(s3Cli)

		buf := manager.NewWriteAtBuffer(make([]byte, fileSize))
		_, err := downloader.Download(b.Context(), buf, &s3.GetObjectInput{
			Bucket: &bucketName,
			Key:    aws.String(fileName),
		})
		if err != nil {
			b.Errorf("error reading object: %v", err)
		}
	})
}

func TestLocalReadWrite(t *testing.T) {
	t.Parallel()

	localFile := os.Getenv("S3IO_TESTFILE")
	if localFile == "" {
		t.Skip()
	}

	ctx := t.Context()

	s3Cli, err := getTestS3Client()
	if err != nil {
		t.Fatalf("error getting test manager")
	}

	bucket, err := getTestBucket(s3Cli)
	if err != nil {
		t.Errorf("error getting test bucket: %v", err)
		return
	}

	srcFile, err := os.Open(localFile)
	if err != nil {
		t.Errorf("error opening localfile '%s': %v", localFile, err)
		return
	}
	defer srcFile.Close()

	fileName := path.Base(localFile)
	dest := path.Join("data", fileName)

	wr := bucket.Put(ctx, dest)

	if _, err := io.Copy(wr, srcFile); err != nil {
		t.Fatalf("error writing: %v", err)
	}

	if err := wr.Close(); err != nil {
		t.Fatalf("error writing on close: %v", err)
	}

	localDest := os.Getenv("S3IO_DEST")
	if localDest == "" {
		return
	}

	rd := bucket.Get(ctx, dest)

	destFile, err := os.OpenFile(localDest, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777)
	if err != nil {
		t.Fatalf("error opening local dest file: %v", err)
	}

	if _, err := io.Copy(destFile, rd); err != nil {
		t.Fatalf("error writing: %v", err)
	}
}

func testFile(t *testing.T, bucket s3io.Bucket, testName, filename string, src io.Reader) {
	t.Run(testName, func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()

		writeHash := sha256.New()
		success := t.Run("writing file", func(t *testing.T) {
			wr := bucket.Put(ctx, filename)

			_, err := io.Copy(wr, io.TeeReader(src, writeHash))
			if err != nil {
				t.Fatalf("error writing object: %v", err)
			}

			if err := wr.Close(); err != nil {
				t.Fatalf("error writing object on close: %v", err)
			}
		})

		if !success {
			t.FailNow()
		}

		readHash := sha256.New()
		success = t.Run("reading file", func(t *testing.T) {
			rd := bucket.Get(ctx, filename)

			_, err := io.Copy(readHash, rd)
			if err != nil && err != io.EOF {
				t.Fatalf("error reading file: %v", err)
			}
		})

		if !success {
			t.FailNow()
		}

		writeSum := fmt.Sprintf("%x", writeHash.Sum(nil))
		readSum := fmt.Sprintf("%x", readHash.Sum(nil))
		if writeSum != readSum {
			t.Errorf("read and write are not equal. write: '%s', read: '%s'", writeSum, readSum)
		}
	})
}

func getTestBucket(cli *s3.Client) (s3io.Bucket, error) {
	bucketName := envOrDefault("AWS_BUCKET_NAME", "jobbitz-testing")

	logger := slog.New(slog.DiscardHandler)
	if withDebug := os.Getenv("DEBUG_LOG"); withDebug != "" {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelDebug,
		}))
	}

	createBucketMux.Lock()
	defer createBucketMux.Unlock()

	bucket, err := s3io.Open(context.Background(),
		bucketName,
		s3io.WithBucketCli(cli),
		s3io.WithBucketRetries(3),
		s3io.WithBucketCreateIfNotExists(),
		s3io.WithBucketLogger(logger),
	)

	return bucket, err
}

func getTestS3Client() (*s3.Client, error) {
	region := envOrDefault("AWS_REGION", "local")
	accessKey := envOrDefault("AWS_ACCESS_KEY_ID", "access_key")
	secretKey := envOrDefault("AWS_SECRET_ACCESS_KEY", "secret_key")
	endpoint := envOrDefault("AWS_S3_ENDPOINT", "http://localhost:9000")

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithBaseEndpoint(endpoint),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, err
	}

	cli := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})

	return cli, nil
}

func envOrDefault(varname, defaultVal string) string {
	if val := os.Getenv(varname); val != "" {
		defaultVal = val
	}

	return defaultVal
}
