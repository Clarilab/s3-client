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
	DefaultImage = "quay.io/minio/minio:latest"
)

// NewClient starts a container with a running MinIO instance and returns a new s3.Client, the container and an error.
//
// Notes: Only meant to be used for testing purposes. Host MUST have a docker engine running.
func NewClient(ctx context.Context, bucketName string, options ...Option) (s3.Client, testcontainers.Container, error) {
	const errMessage = "failed to create new client: %w"

	opts := containerOptions{
		image: DefaultImage,
	}

	for i := range options {
		options[i](&opts)
	}

	container, err := miniocontainer.Run(ctx, opts.image, opts.customizers...)
	if err != nil {
		return nil, nil, fmt.Errorf(errMessage, err)
	}

	url, err := container.ConnectionString(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf(errMessage, err)
	}

	minioClient, err := minio.New(url, &minio.Options{
		Secure: false,
		Creds:  credentials.NewStaticV4(container.Username, container.Password, ""),
	})
	if err != nil {
		return nil, nil, fmt.Errorf(errMessage, err)
	}

	if err = minioClient.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{}); err != nil {
		return nil, nil, fmt.Errorf(errMessage, err)
	}

	conn, err := s3.NewClient(
		&s3.ClientDetails{
			Host:         url,
			AccessKey:    container.Username,
			AccessSecret: container.Password,
			BucketName:   bucketName,
			Secure:       false,
		},
		opts.s3Options...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(errMessage, err)
	}

	return conn, container, nil
}

// NewContainer runs a container with a running MinIO instance.
//
// Notes: Only meant to be used for testing purposes. Host MUST have a docker engine running.
func NewContainer(ctx context.Context, image string, customizers ...testcontainers.ContainerCustomizer) (*miniocontainer.MinioContainer, error) {
	return miniocontainer.Run(ctx, image, customizers...)
}
