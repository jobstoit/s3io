# S3IO

An abstraction layer on top of the s3 sdk to do io read/write opperations on s3 objects

```go
// Note the "WithBucket..." are options
bucket, err := s3io.OpenBucket(ctx, "my-bucket-name", WithBucketCredentials(accessKey, secretKey))
if err != nil {
  ...
}

// Note the "WithBucket..." are options specifically for this writer session
writer, err := bucket.NewWriter(ctx, "path/to/object.txt", WithWriterRetries(3))
if err != nil {
  ...
}

_, err := io.Copy(writer, file)
if err != nil {
  ....
}

reader, err := bucket.NewReader(ctx, "path/to/object.txt")
if err != nil {

}

_, err := io.Copy(os.Stdout, reader)
...
```
