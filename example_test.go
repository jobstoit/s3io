package s3io

import (
	"context"
	"io"
	"io/fs"
	"log"
	"os"
	"text/template"
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

func ExampleBucket_Open() {
	// you're able to use the bucket as fs.FS.
	// here's an example how to use it with your html/template's.

	ctx := context.Background()

	bucket, err := OpenBucket(ctx, "bucket-name",
		WithBucketCredentials("access-key", "access-secret"),
		WithBucketCreateIfNotExists(),
	)
	if err != nil {
		log.Fatal(err)
	}

	templateEngine, err := template.ParseFS(bucket, "path/to/templates/*.html.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	err = templateEngine.ExecuteTemplate(os.Stdout, "index.html.tmpl", map[string]any{
		"arg1": "foo",
		"arg2": "bar",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Or use it as simple subFS
	subSys, err := fs.Sub(bucket, "path/to/subdir")
	if err != nil {
		log.Fatal(err)
	}

	file, err := subSys.Open("somefile")
	if err != nil {
		log.Fatal(err)
	}

	if _, err := io.Copy(os.Stdout, file); err != nil {
		log.Fatal(err)
	}
}
