// Package s3io is an abstraction layer on the s3 sdk.
//
// The s3io package provides a bucket that can interact with all elements within the bucket.
// And can be configured with the "WithBucket..." options
//
//	bucket, err := s3io.OpenBucket(ctx, "my-bucket-name", s3io.WithBucketCredentials("access-key", "secret-key"))
//
// There is an ObjectReader to preform read opperations on an s3 object.
//
//	rd, err := bucket.NewReader(ctx, "path/to/object.txt", s3io.WithReaderConcurrency(10))
//	if err != nil {
//	  return err
//	}
//
//	_, err := io.Copy(os.Stdout, rd)
//
// And there is an ObjectWriter to preform write opperations on an s3 object.
// Note The writer MUST close to safe the object.
//
//	wr, err := bucket.NewWriter(ctx, "path/to/object.txt")
//	if err != nil
//	  return err
//	}
//
//	_, err := io.WriteString(wr, "Hello world!")
//	if err != nil {
//	  return err
//	}
//
//	if err := wr.Close(); err != nil {
//	  return err
//	}
//
// The s3io reader and writer stream the objects from and to your s3 instance while being memory efficient.
package s3io
