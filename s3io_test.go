package s3io_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jobstoit/s3io/v2"
)

var (
	buf12MB    = make([]byte, 1024*1024*12)
	buf2MB     = make([]byte, 1024*1024*2)
	noopLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

	createBucketMux = &sync.Mutex{}
)

func TestBucketFS(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	bucket, err := getTestBucket()
	if err != nil {
		t.Fatalf("unable to get test bucket: %v", err)
	}

	t.Run("bucket implements glob and fs", func(t *testing.T) {
		var _ fs.GlobFS = bucket
	})

	t.Run("get fs template", func(t *testing.T) {
		const templ = "templates/index.html.tmpl"

		wr := bucket.NewWriter(ctx, templ)
		if _, err := wr.Write([]byte("<p>{{.Message}}</p>")); err != nil {
			t.Fatalf("unable to write template: %v", err)
		}

		if err := wr.Close(); err != nil {
			t.Fatalf("unable to store template: %v", err)
		}

		engine, err := template.ParseFS(bucket, "templates/*.html.tmpl")
		if err != nil {
			t.Fatalf("unable to initialize template engine: %v", err)
		}

		hash := sha256.New()

		err = engine.ExecuteTemplate(
			hash,
			"index.html.tmpl",
			map[string]string{"Message": "hello world"},
		)
		if err != nil {
			t.Errorf("unable to execute template: %v", err)
		}

		expectedHash := sha256.New()
		_, _ = io.WriteString(expectedHash, "<p>hello world</p>")

		if a, e := fmt.Sprintf("%x", hash.Sum(nil)), fmt.Sprintf("%x", expectedHash.Sum(nil)); e != a {
			t.Errorf("error unexpected message")
		}
	})
}

func TestReadWrite(t *testing.T) {
	t.Parallel()

	bucket, err := getTestBucket()
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
	ctx := context.Background()

	bucket, err := getTestBucket()
	if err != nil {
		b.Fatalf("error getting test bucket: %v", err)
		return
	}

	fileName := "benchmark.txt"
	bucketName := bucket.Name()
	var fileSize int64 = 1024 * 1024 * 120 // 120Mb

	b.Run("fs upload", func(b *testing.B) {
		wr := bucket.NewWriter(ctx, fileName, s3io.WithWriterLogger(noopLogger))
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
		uploader := manager.NewUploader(bucket.Client())

		_, err := uploader.Upload(context.Background(), &s3.PutObjectInput{
			Bucket: &bucketName,
			Key:    aws.String(fileName),
			Body:   io.LimitReader(rand.Reader, fileSize),
		})
		if err != nil {
			b.Errorf("error uploading file: %v", err)
		}
	})

	b.Run("fs download", func(b *testing.B) {
		rd := bucket.NewReader(ctx, fileName, s3io.WithReaderLogger(noopLogger))

		buf := io.Discard
		_, err = io.Copy(buf, rd)
		if err != nil {
			b.Errorf("error reading file: %v", err)
		}
	})

	b.Run("manager download", func(b *testing.B) {
		downloader := manager.NewDownloader(bucket.Client())

		buf := manager.NewWriteAtBuffer(make([]byte, fileSize))
		_, err := downloader.Download(context.Background(), buf, &s3.GetObjectInput{
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

	wr := bucket.NewWriter(ctx, dest)

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

	rd := bucket.NewReader(ctx, dest)

	destFile, err := os.OpenFile(localDest, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777)
	if err != nil {
		t.Fatalf("error opening local dest file: %v", err)
	}

	if _, err := io.Copy(destFile, rd); err != nil {
		t.Fatalf("error writing: %v", err)
	}
}

func testFile(t *testing.T, bucket *s3io.Bucket, testName, filename string, src io.Reader) {
	t.Run(testName, func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()

		writeHash := sha256.New()
		success := t.Run("writing file", func(t *testing.T) {
			wr := bucket.NewWriter(ctx, filename)

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
			rd := bucket.NewReader(ctx, filename)

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

func getTestBucket() (*s3io.Bucket, error) {
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

	createBucketMux.Lock()
	defer createBucketMux.Unlock()

	bucket, err := s3io.OpenBucket(context.Background(),
		bucketName,
		s3io.WithBucketHost(endpoint, region, true),
		s3io.WithBucketCredentials(accessKey, secretKey),
		s3io.WithBucketRetries(3),
		s3io.WithBucketCreateIfNotExists(),
		s3io.WithBucketLogger(logger),
	)

	return bucket, err
}

func envOrDefault(varname, defaultVal string) string {
	if val := os.Getenv(varname); val != "" {
		defaultVal = val
	}

	return defaultVal
}
