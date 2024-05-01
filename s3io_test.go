package s3io

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestReadWrite(t *testing.T) {
	t.Parallel()

	bucket, err := getTestBucket()
	if err != nil {
		t.Errorf("error getting test bucket: %v", err)
		return
	}

	testFile(t, bucket, "exact smallish size file", "data/test-exact-smallish.txt", io.LimitReader(rand.Reader, 1024*1024*10))
	testFile(t, bucket, "smallish size file", "data/test-smallish.txt", io.LimitReader(rand.Reader, 1024*1024*18+291))
	testFile(t, bucket, "small size file", "data/test-small.txt", io.LimitReader(rand.Reader, 1024*512+342))
	testFile(t, bucket, "medium size file", "data/test-medium.txt", io.LimitReader(rand.Reader, 1024*1024*358+572))
	// testFile(t, bucket, "large size file", "data/test-large.txt", io.LimitReader(rand.Reader, 1024*1024*1024*2+812))
}

func BenchmarkAgainstManager(b *testing.B) {
	ctx := context.Background()

	bucket, err := getTestBucket()
	if err != nil {
		b.Errorf("error getting test bucket: %v", err)
		return
	}

	fileName := "benchmark.txt"
	// var fileSize int64 = 1024 * 1024 * 1024 * 1 // 1Gb
	var fileSize int64 = 1024 * 1024 * 120 // 120Mb

	b.Run("fs upload", func(b *testing.B) {
		wr, err := bucket.NewWriter(ctx, fileName, WithWriterLogger(noopLogger))
		if err != nil {
			b.Errorf("error opening writer: %v", err)
			return
		}
		defer wr.Close()

		_, err = io.Copy(wr, io.LimitReader(rand.Reader, fileSize))
		if err != nil {
			b.Errorf("error writing file: %v", err)
			return
		}

		if err = wr.Close(); err != nil {
			b.Errorf("error closing file: %v", err)
		}
	})

	b.Run("manager upload", func(b *testing.B) {
		uploader := manager.NewUploader(bucket.Client())

		_, err := uploader.Upload(context.Background(), &s3.PutObjectInput{
			Bucket: &bucket.name,
			Key:    aws.String(fileName),
			Body:   io.LimitReader(rand.Reader, fileSize),
		})
		if err != nil {
			b.Errorf("error uploading file: %v", err)
		}
	})

	b.Run("fs download", func(b *testing.B) {
		rd, err := bucket.NewReader(ctx, fileName, WithReaderLogger(noopLogger))
		if err != nil {
			b.Errorf("error opening reader: %v", err)
			return
		}

		// buf := bytes.NewBuffer(make([]byte, fileSize))
		buf := io.Discard
		_, err = io.Copy(buf, rd)
		if err != nil {
			b.Errorf("error reading file: %v", err)
		}
	})

	b.Run("manager download", func(b *testing.B) {
		downloader := manager.NewDownloader(bucket.Client())

		// buf := discardWriterAt{}
		buf := manager.NewWriteAtBuffer(make([]byte, fileSize))
		_, err := downloader.Download(context.Background(), buf, &s3.GetObjectInput{
			Bucket: &bucket.name,
			Key:    aws.String(fileName),
		})
		if err != nil {
			b.Errorf("error reading object: %v", err)
		}
	})
}

func TestLocalReadWrite(t *testing.T) {
	t.Skip()
	t.Parallel()

	localFile := os.Getenv("S3IO_TESTFILE")
	if localFile == "" {
		t.Skip()
	}

	ctx := context.Background()

	bucket, err := getTestBucket()
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

	wr, err := bucket.NewWriter(ctx, dest)
	if err != nil {
		t.Errorf("error getting new writer: %v", err)
		return
	}

	if _, err := io.Copy(wr, srcFile); err != nil {
		t.Errorf("error writing: %v", err)
		return
	}

	if err := wr.Close(); err != nil {
		t.Errorf("error writing on close: %v", err)
		return
	}

	localDest := os.Getenv("S3IO_DEST")
	if localDest == "" {
		return
	}

	rd, err := bucket.NewReader(ctx, dest)
	if err != nil {
		t.Errorf("error opening new reader: %v", err)
		return
	}

	destFile, err := os.OpenFile(localDest, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777)
	if err != nil {
		t.Errorf("error opening local dest file: %v", err)
		return
	}

	if _, err := io.Copy(destFile, rd); err != nil {
		t.Errorf("error writing: %v", err)
		return
	}
}

func testFile(t *testing.T, bucket *Bucket, testName, filename string, src io.Reader) {
	t.Run(testName, func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()

		writeHash := sha256.New()
		success := t.Run("writing file", func(t *testing.T) {
			wr, err := bucket.NewWriter(ctx, filename)
			if err != nil {
				t.Errorf("error creating new writer: %v", err)
				t.FailNow()
			}

			_, err = io.Copy(wr, io.TeeReader(src, writeHash))
			if err != nil {
				t.Errorf("error writing object: %v", err)
				return
			}

			if err := wr.Close(); err != nil {
				t.Errorf("error writing object on close: %v", err)
				return
			}
		})

		if !success {
			t.Fail()
			return
		}

		readHash := sha256.New()
		success = t.Run("reading file", func(t *testing.T) {
			rd, err := bucket.NewReader(ctx, filename)
			if err != nil {
				t.Errorf("error creating new reader: %v", err)
				return
			}

			_, err = io.Copy(readHash, rd)
			if err != nil && err != io.EOF {
				t.Errorf("error reading file: %v", err)
				return
			}
		})

		if !success {
			t.Fail()
			return
		}

		writeSum := fmt.Sprintf("%x", writeHash.Sum(nil))
		readSum := fmt.Sprintf("%x", readHash.Sum(nil))
		if writeSum != readSum {
			t.Errorf("read and write are not equal. write: '%s', read: '%s'", writeSum, readSum)
		}
	})
}

func ExampleObjectReader_Read() {
	ctx := context.Background()

	bucket, err := OpenBucket(ctx, "bucket-name",
		WithBucketCredentials("access-key", "access-secret"),
		WithBucketCreateIfNotExists(),
	)
	if err != nil {
		log.Fatal(err)
	}

	rd, err := bucket.NewReader(ctx, "path/to/object.txt")
	if err != nil {
		log.Fatal(err)
	}

	if _, err := io.Copy(os.Stdout, rd); err != nil {
		log.Fatal(err)
	}
}

func ExampleObjectWriter_Write() {
	ctx := context.Background()

	bucket, err := OpenBucket(ctx, "bucket-name",
		WithBucketCredentials("access-key", "access-secret"),
		WithBucketCreateIfNotExists(),
	)
	if err != nil {
		log.Fatal(err)
	}

	wr, err := bucket.NewWriter(ctx, "path/to/object.txt")
	if err != nil {
		log.Fatal(err)
	}

	if _, err := io.WriteString(wr, "hello world"); err != nil {
		log.Fatal(err)
	}

	if err := wr.Close(); err != nil {
		log.Fatal(err)
	}
}

func getTestBucket() (*Bucket, error) {
	region := envOrDefault("AWS_REGION", "local")
	bucketName := envOrDefault("AWS_BUCKET_NAME", "jobbitz-testing")
	accessKey := envOrDefault("AWS_ACCESS_KEY_ID", "access_key")
	secretKey := envOrDefault("AWS_SECRET_ACCESS_KEY", "secret_key")
	endpoint := envOrDefault("AWS_S3_ENDPOINT", "http://localhost:9000")

	logger := noopLogger
	if withDebug := os.Getenv("DEBUG_LOG"); withDebug != "" {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelDebug,
		}))
	}

	bucket, err := OpenBucket(context.Background(),
		bucketName,
		WithBucketHost(endpoint, region, true),
		WithBucketCredentials(accessKey, secretKey),
		WithBucketRetries(3),
		WithBucketCreateIfNotExists(),
		WithBucketLogger(logger),
	)

	return bucket, err
}

func envOrDefault(varname, defaultVal string) string {
	if val := os.Getenv(varname); val != "" {
		defaultVal = val
	}

	return defaultVal
}
