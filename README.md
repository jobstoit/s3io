# S3IO

[![Go Reference](https://pkg.go.dev/badge/github.com/jobstoit/s3io/v2.svg)](https://pkg.go.dev/github.com/jobstoit/s3io/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/jobstoit/s3io/v2)](https://goreportcard.com/report/github.com/jobstoit/s3io/v2)

An abstraction layer on top of the s3 sdk to do io read/write opperations on s3 objects.
The s3io reader and writer stream the objects from and to your s3 instance while being memory efficient.

```go
// Note the "WithBucket..." are options
bucket, err := s3io.OpenBucket(ctx, "my-bucket-name", s3io.WithBucketCredentials(accessKey, secretKey))
if err != nil {
  return err
}

// Note the "WithBucket..." are options specifically for this writer session
writer := bucket.NewWriter(ctx, "path/to/object.txt", s3io.WithWriterRetries(3))
defer writer.Close() // makes sure your upload won't keep hanging

if _, err := io.WriteString(writer, "Hello world!"); err != nil {
  return err
}

if err := writer.Close(); err != nil {
  return err 
}

reader := bucket.NewReader(ctx, "path/to/object.txt")

_, err := io.Copy(os.Stdout, reader)


// Use the bucket as fs.FS
subSys, err := fs.Sub(bucket, "path/to/subdir")
...
```
