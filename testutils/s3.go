package testutils

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/Clarilab/s3-client/v4"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/orlangure/gnomock"
)

const (
	user      = "admin"
	passwd    = "password"
	imageTag  = "quay.io/minio/minio:latest"
	minioPort = 9000

	userEnv    = "MINIO_ROOT_USER=" + user
	userPasswd = "MINIO_ROOT_PASSWORD=" + passwd
	command    = "server"
	arg        = "/data"
)

// StopFunc is a function to stop the created container.
type StopFunc func() error

// NewClient starts a container with a running MinIO instance
// and returns a new s3.Client, a function to stop the container on purpose and an error.
//
// Notes:
// Only meant to be used for testing purposes.
// Host MUST have a docker engine running.
func NewClient(bucketName string, options ...s3.ClientOption) (s3.Client, StopFunc, error) {
	const errMessage = "failed to create new client: %w"

	container, err := gnomock.StartCustom(
		imageTag,
		gnomock.DefaultTCP(minioPort),
		gnomock.WithUseLocalImagesFirst(),
		gnomock.WithEnv(userEnv),
		gnomock.WithEnv(userPasswd),
		gnomock.WithCommand(command, arg),
	)
	if err != nil {
		return nil, nil, fmt.Errorf(errMessage, err)
	}

	url := net.JoinHostPort(container.Host, strconv.Itoa(container.DefaultPort()))

	minioClient, err := minio.New(url, &minio.Options{
		Secure: false,
		Creds:  credentials.NewStaticV4(user, passwd, ""),
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
			AccessKey:    user,
			AccessSecret: passwd,
			BucketName:   bucketName,
			Secure:       false,
		},
		options...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(errMessage, err)
	}

	return conn, func() error { return gnomock.Stop(container) }, nil
}
