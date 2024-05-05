# S3IO

[![Go Reference](https://pkg.go.dev/badge/github.com/jobstoit/s3io/v2.svg)](https://pkg.go.dev/github.com/jobstoit/s3io/v2)

An abstraction layer on top of the s3 sdk to do io read/write opperations on s3 objects.
The s3io reader and writer stream the objects from and to your s3 instance while being memory efficient.

```go
// Note the "WithBucket..." are options
bucket, err := s3io.OpenBucket(ctx, "my-bucket-name", s3io.WithBucketCredentials(accessKey, secretKey))
if err != nil {
  return err
}

// Note the "WithBucket..." are options specifically for this writer session
writer, err := bucket.NewWriter(ctx, "path/to/object.txt", s3io.WithWriterRetries(3))
if err != nil {
  return err
}

if _, err := io.WriteString(writer, "Hello world!"); err != nil {
  return err
}

if err := writer.Close(); err != nil {
  return err 
}

reader, err := bucket.NewReader(ctx, "path/to/object.txt")
if err != nil {
  return err
}

_, err := io.Copy(os.Stdout, reader)
...
```
