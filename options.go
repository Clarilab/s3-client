package s3

import (
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
)

// ClientOption is an option for the s3 client.
type ClientOption func(*client) error

// WithHealthCheck enables the health check for the s3 client.
func WithHealthCheck(interval time.Duration) ClientOption {
	const errMessage = "failed to enable health check: %w"

	return func(c *client) (err error) { //nolint:nonamedreturns // intended
		c.cancelFunc, err = c.minioClient.HealthCheck(interval)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		return nil
	}
}

// ClientUploadOptions is an alias for minio.PutObjectOptions.
type ClientUploadOptions minio.PutObjectOptions

type uploadOptions struct {
	clientOptions ClientUploadOptions
}

// UploadOption is an option for uploading a file.
type UploadOption func(*uploadOptions)

// WithClientUploadOptions sets client options for the upload request.
func WithClientUploadOptions(options ClientUploadOptions) UploadOption {
	return func(o *uploadOptions) {
		o.clientOptions = options
	}
}

type getOptions struct {
	clientOptions ClientGetOptions
}

// GetOption is an option for getting a file.
type GetOption func(*getOptions)

// ClientGetOptions is an alias for minio.GetObjectOptions.
type ClientGetOptions minio.GetObjectOptions

// WithClientGetOptions sets client options for the get request.
func WithClientGetOptions(options ClientGetOptions) GetOption {
	return func(o *getOptions) {
		o.clientOptions = options
	}
}

type downloadOptions struct {
	clientOptions ClientGetOptions
}

// DownloadOption is an option for downloading a file.
type DownloadOption func(*downloadOptions)

// WithClientGetOptions sets client options for the get request.
func WithClientDownloadOptions(options ClientGetOptions) DownloadOption {
	return func(o *downloadOptions) {
		o.clientOptions = options
	}
}

// ClientRemoveOptions is an alias for minio.RemoveObjectOptions.
type ClientRemoveOptions minio.RemoveObjectOptions

type removeOptions struct {
	clientOptions ClientRemoveOptions
}

// UploadOption is an option for uploading a file.
type RemoveOption func(*removeOptions)

// WithClientUploadOptions sets client options for the upload request.
func WithClientRemoveOptions(options ClientRemoveOptions) RemoveOption {
	return func(o *removeOptions) {
		o.clientOptions = options
	}
}
