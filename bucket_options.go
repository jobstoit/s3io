package s3io

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
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
	readChunkSize    int
	writeChunkSize   int
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
		logger:         noopLogger,
	}
}

type BucketOption func(*bucketBuilder) error

// BucketOptions bundles bucket options
func BucketOptions(opts ...BucketOption) BucketOption {
	return func(b *bucketBuilder) error {
		for _, op := range opts {
			if err := op(b); err != nil {
				return err
			}
		}

		return nil
	}
}

func (b *bucketBuilder) Build(ctx context.Context, name string) (*Bucket, error) {
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

	return &Bucket{
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
	return func(b *bucketBuilder) error {
		b.cli = cli

		return nil
	}
}

// WithBucketCliLoaderOptions sets the config.LoaderOptions for the aws config.
// Only works if the cli is not already provided.
func WithBucketCliLoaderOptions(opts ...func(*config.LoadOptions) error) BucketOption {
	return func(b *bucketBuilder) error {
		b.cliOpts = append(b.cliOpts, opts...)

		return nil
	}
}

// WithBucketS3Options sets the s3 options for t.he s3 client.
// Only works if the cli is not already provided.
func WithBucketS3Options(opts ...func(*s3.Options)) BucketOption {
	return func(b *bucketBuilder) error {
		b.s3Opts = append(b.s3Opts, opts...)

		return nil
	}
}

// WithBucketHost sets the endpoint, region and if it uses a pathstyle for the cli.
// Only works if the cli is not already provided.
func WithBucketHost(url, region string, usePathStyle bool) BucketOption {
	return func(b *bucketBuilder) error {
		b.cliOpts = append(b.cliOpts,
			config.WithRegion(region),
			config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, opt ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: url, SigningRegion: region}, nil
			})),
		)

		b.s3Opts = append(b.s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})

		return nil
	}
}

// WithBucketCredentials sets the access key and secret key for the cli.
// Only works if the cli is not already provided.
func WithBucketCredentials(accessKey, secretKey string) BucketOption {
	return func(b *bucketBuilder) error {
		b.cliOpts = append(b.cliOpts,
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		)

		return nil
	}
}

// WithBucketCreateIfNotExitsts will create the bucket if it doesn't already exist.
func WithBucketCreateIfNotExists() BucketOption {
	return func(b *bucketBuilder) error {
		b.createIfNotExist = true

		return nil
	}
}

// WithBucketReadChunkSize sets the default read chunksize
func WithBucketReadChunkSize(size uint) BucketOption {
	return func(b *bucketBuilder) error {
		b.readChunkSize = int(size)

		return nil
	}
}

// WithBucketWriteChunkSize sets the default write chunksize
func WithBucketWriteChunkSize(size uint) BucketOption {
	return func(b *bucketBuilder) error {
		s := int(size)
		if s < DefaultChunkSize {
			return ErrMinChunkSize
		}

		b.readChunkSize = s

		return nil
	}
}

// WithBucketConcurrency sets the default read/write concurrency
func WithBucketConcurrency(size uint8) BucketOption {
	return func(b *bucketBuilder) error {
		b.concurrency = int(size)

		return nil
	}
}

// WithBucketRetries sets the default amount of retries for any given opperation
func WithBucketRetries(i uint8) BucketOption {
	return func(b *bucketBuilder) error {
		b.retries = int(i)

		return nil
	}
}

// WithBucketLogger sets the default logger for any opperation.
// Setting the logger provides debug logs.
func WithBucketLogger(logger *slog.Logger) BucketOption {
	return func(b *bucketBuilder) error {
		if logger != nil {
			b.logger = logger
		}

		return nil
	}
}
