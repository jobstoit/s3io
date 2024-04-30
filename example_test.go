package s3io

import (
	"context"
	"io"
	"log"
	"os"
)

func ExampleObjectReader_Read() {
	ctx := context.Background()

	bucket, err := OpenBucket(ctx, "bucket-name",
		WithBucketCredentials("access-key", "access-secret"),
		WithBucketCreateIfNotExists(),
	)
	if err != nil {
		log.Fatal(err)
	}

	rd := bucket.NewReader(ctx, "path/to/object.txt")
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

	wr := bucket.NewWriter(ctx, "path/to/object.txt")
	if err != nil {
		log.Fatal(err)
	}

	if _, err := io.WriteString(wr, "hello world"); err != nil {
		log.Fatal(err)
	}

	// Note: you must close to finilize the upload
	if err := wr.Close(); err != nil {
		log.Fatal(err)
	}
}
