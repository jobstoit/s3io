package s3io

import (
	"context"
	"io"
	"log"
	"os"
)

func ExampleReader_Read() {
	ctx := context.Background()

	bucket, err := Open(ctx, "bucket-name",
		WithBucketCredentials("access-key", "access-secret"),
		WithBucketCreateIfNotExists(),
	)
	if err != nil {
		log.Fatal(err)
	}

	rd := bucket.Get(ctx, "path/to/object.txt")

	if _, err := io.Copy(os.Stdout, rd); err != nil {
		log.Fatal(err)
	}
}

func ExampleWriter_Write() {
	ctx := context.Background()

	bucket, err := Open(ctx, "bucket-name",
		WithBucketCredentials("access-key", "access-secret"),
		WithBucketCreateIfNotExists(),
	)
	if err != nil {
		log.Fatal(err)
	}

	wr := bucket.Put(ctx, "path/to/object.txt")

	if _, err := io.WriteString(wr, "hello world"); err != nil {
		log.Fatal(err)
	}

	// Note: you must close to finilize the upload
	if err := wr.Close(); err != nil {
		log.Fatal(err)
	}
}
