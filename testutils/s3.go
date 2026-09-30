package testutils

import (
	"context"
	"fmt"

	"github.com/Clarilab/s3-client/v4"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	miniocontainer "github.com/testcontainers/testcontainers-go/modules/minio"
)

const (
	// DefaultImage is the default MinIO container image.
	//
	// Deprecated: MinIO no longer publishes this image publicly. Use SeaweedFSImage instead.
	DefaultImage = "quay.io/minio/minio:latest"
)

// NewClient starts a container with a running MinIO instance and returns a new s3.Client, the container and an error.
//
// Notes: Only meant to be used for testing purposes. Host MUST have a docker engine running.
//
// Deprecated: MinIO no longer publishes its image publicly. Use NewSeaweedFSClient instead.
func NewClient(ctx context.Context, bucketName string, options ...Option) (s3.Client, testcontainers.Container, error) {
	const errMessage = "failed to create new client: %w"

	opts := containerOptions{
		image: DefaultImage,
	}

	for i := range options {
		options[i](&opts)
	}

	var customizers []testcontainers.ContainerCustomizer

	if opts.username != "" || opts.password != "" {
		customizers = append(customizers, miniocontainer.WithUsername(opts.username), miniocontainer.WithPassword(opts.password))
	}

	container, err := miniocontainer.Run(ctx, opts.image, customizers...)
	if err != nil {
		return nil, nil, fmt.Errorf(errMessage, err)
	}

	url, err := container.ConnectionString(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf(errMessage, err)
	}

	conn, err := newBucketAndClient(ctx, url, container.Username, container.Password, bucketName, opts.s3Options)
	if err != nil {
		return nil, nil, fmt.Errorf(errMessage, err)
	}

	return conn, container, nil
}

// NewSeaweedFSClient starts a container with a running SeaweedFS instance and returns a new s3.Client,
// the container and an error.
//
// Notes: Only meant to be used for testing purposes. Host MUST have a docker engine running.
func NewSeaweedFSClient(ctx context.Context, bucketName string, options ...Option) (s3.Client, testcontainers.Container, error) {
	const errMessage = "failed to create new client: %w"

	opts := containerOptions{
		image: SeaweedFSImage,
	}

	for i := range options {
		options[i](&opts)
	}

	var customizers []testcontainers.ContainerCustomizer

	if opts.username != "" || opts.password != "" {
		customizers = append(customizers, WithSeaweedFSCredentials(opts.username, opts.password))
	}

	container, err := NewSeaweedFSContainer(ctx, opts.image, customizers...)
	if err != nil {
		return nil, nil, fmt.Errorf(errMessage, err)
	}

	url, err := container.ConnectionString(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf(errMessage, err)
	}

	conn, err := newBucketAndClient(ctx, url, container.Username, container.Password, bucketName, opts.s3Options)
	if err != nil {
		return nil, nil, fmt.Errorf(errMessage, err)
	}

	return conn, container, nil
}

// NewContainer runs a container with a running MinIO instance.
//
// Notes: Only meant to be used for testing purposes. Host MUST have a docker engine running.
//
// Deprecated: MinIO no longer publishes its image publicly. Use NewSeaweedFSContainer instead.
func NewContainer(ctx context.Context, image string, customizers ...testcontainers.ContainerCustomizer) (*miniocontainer.MinioContainer, error) {
	return miniocontainer.Run(ctx, image, customizers...)
}

func newBucketAndClient(
	ctx context.Context,
	url, username, password, bucketName string,
	s3Options []s3.ClientOption,
) (s3.Client, error) {
	minioClient, err := minio.New(url, &minio.Options{
		Secure: false,
		Creds:  credentials.NewStaticV4(username, password, ""),
	})
	if err != nil {
		return nil, err
	}

	if err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{}); err != nil {
		return nil, err
	}

	return s3.NewClient(
		&s3.ClientDetails{
			Host:         url,
			AccessKey:    username,
			AccessSecret: password,
			BucketName:   bucketName,
			Secure:       false,
		},
		s3Options...,
	)
}
