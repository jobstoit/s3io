package s3io

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type bucketBuilder struct {
	createIfNotExist bool
	cli              *s3.Client
	retries          int
	concurrency      int
	readChunkSize    int64
	writeChunkSize   int64
	logger           *slog.Logger

	// client options
	cliOpts []func(*config.LoadOptions) error
	s3Opts  []func(*s3.Options)
	acl     types.BucketCannedACL
}

func newBucketBuilder() *bucketBuilder {
	return &bucketBuilder{
		retries:        defaultConcurrency,
		concurrency:    defaultConcurrency,
		readChunkSize:  DefaultChunkSize,
		writeChunkSize: DefaultChunkSize,
		logger:         slog.New(slog.DiscardHandler),
	}
}

type BucketOption func(*bucketBuilder)

// BucketOptions bundles bucket options
func BucketOptions(opts ...BucketOption) BucketOption {
	return func(b *bucketBuilder) {
		for _, op := range opts {
			op(b)
		}
	}
}

func (b *bucketBuilder) Build(ctx context.Context, name string) (Bucket, error) {
	cli := b.cli
	if cli == nil {
		cfg, err := config.LoadDefaultConfig(ctx, b.cliOpts...)
		if err != nil {
			return nil, err
		}

		cli = s3.NewFromConfig(cfg, append(b.s3Opts, withS3Retries(b.retries))...)
	}

	exists, err := bucketExists(ctx, cli, name)
	if err != nil {
		return nil, err
	}

	if !exists {
		if !b.createIfNotExist {
			return nil, fmt.Errorf("bucket does not exist: '%s'", name)
		}

		_, err := cli.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: &name,
		})
		if err != nil {
			return nil, fmt.Errorf("creating missing bucket '%s' %w", name, err)
		}
	}

	if b.acl != "" {
		_, err := cli.PutBucketAcl(ctx, &s3.PutBucketAclInput{
			Bucket: &name,
			ACL:    b.acl,
		})
		if err != nil {
			return nil, fmt.Errorf("setting bucket '%s' acl: %w", name, err)
		}
	}

	return &bucket{
		name:           name,
		cli:            cli,
		readChunkSize:  b.readChunkSize,
		writeChunkSize: b.writeChunkSize,
		concurrency:    b.concurrency,
		logger:         b.logger,
	}, nil
}

func bucketExists(ctx context.Context, cli *s3.Client, name string) (bool, error) {
	_, err := cli.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: &name,
	})
	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				return false, nil
			default:
				return false, apiError
			}
		}
	}

	return true, err
}

// WithCli directly sets the s3 client for the bucket.
func WithBucketCli(cli *s3.Client) BucketOption {
	return func(b *bucketBuilder) {
		b.cli = cli
	}
}

// WithBucketCliLoaderOptions sets the config.LoaderOptions for the aws config.
// Only works if the cli is not already provided.
func WithBucketCliLoaderOptions(opts ...func(*config.LoadOptions) error) BucketOption {
	return func(b *bucketBuilder) {
		b.cliOpts = append(b.cliOpts, opts...)
	}
}

// WithBucketS3Options sets the s3 options for t.he s3 client.
// Only works if the cli is not already provided.
func WithBucketS3Options(opts ...func(*s3.Options)) BucketOption {
	return func(b *bucketBuilder) {
		b.s3Opts = append(b.s3Opts, opts...)
	}
}

// WithBucketHost sets the endpoint, region and if it uses a pathstyle for the cli.
// Only works if the cli is not already provided.
func WithBucketHost(url, region string, usePathStyle bool) BucketOption {
	return func(b *bucketBuilder) {
		b.cliOpts = append(b.cliOpts,
			config.WithRegion(region),
			config.WithBaseEndpoint(url),
		)

		b.s3Opts = append(b.s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}
}

// WithBucketCredentials sets the access key and secret key for the cli.
// Only works if the cli is not already provided.
func WithBucketCredentials(accessKey, secretKey string) BucketOption {
	return func(b *bucketBuilder) {
		b.cliOpts = append(b.cliOpts,
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		)
	}
}

// WithBucketCreateIfNotExitsts will create the bucket if it doesn't already exist.
func WithBucketCreateIfNotExists() BucketOption {
	return func(b *bucketBuilder) {
		b.createIfNotExist = true
	}
}

// WithBucketReadChunkSize sets the default read chunksize
func WithBucketReadChunkSize(size int64) BucketOption {
	return func(b *bucketBuilder) {
		b.readChunkSize = size
	}
}

// WithBucketWriteChunkSize sets the default write chunksize
func WithBucketWriteChunkSize(size int64) BucketOption {
	return func(b *bucketBuilder) {
		s := size
		if s < MinChunkSize {
			s = MinChunkSize
		}

		b.readChunkSize = s
	}
}

// WithBucketConcurrency sets the default read/write concurrency
func WithBucketConcurrency(size int) BucketOption {
	return func(b *bucketBuilder) {
		if size < 1 {
			size = 1
		}

		b.concurrency = size
	}
}

// WithBucketRetries sets the default amount of retries for any given opperation
func WithBucketRetries(i int) BucketOption {
	return func(b *bucketBuilder) {
		if i < 1 {
			i = 1
		}

		b.retries = i
	}
}

// WithBucketLogger sets the default logger for any opperation.
// Setting the logger provides debug logs.
func WithBucketLogger(logger *slog.Logger) BucketOption {
	return func(b *bucketBuilder) {
		if logger == nil {
			logger = slog.New(slog.DiscardHandler)
		}

		b.logger = logger
	}
}
