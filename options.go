package s3

import (
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
)

// Option is an option for the s3 client.
type Option func(*client) error

// WithHealthCheck enables the health check for the s3 client.
func WithHealthCheck(interval time.Duration) Option {
	const errMessage = "failed to enable health check: %w"

	return func(c *client) (err error) { //nolint:nonamedreturns // intended
		c.cancelFunc, err = c.minioClient.HealthCheck(interval)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}

		return nil
	}
}

type getOption struct {
	noContentDownload bool
	clientGetOptions  minio.GetObjectOptions
}

// GetOption is an option for getting a file.
type GetOption func(*getOption)

// WithGetOptionDownloadContent enables or disables downloading the content of the file.
//
// Default is enabled.
//
// When disabled, the Content field of the returned File struct will be nil.
func WithGetOptionDownloadContent(downloadContent bool) GetOption {
	return func(o *getOption) {
		o.noContentDownload = !downloadContent
	}
}

// WithGetOptionClientGetOptions sets client options for the get request.
func WithGetOptionClientGetOptions(options minio.GetObjectOptions) GetOption {
	return func(o *getOption) {
		o.clientGetOptions = options
	}
}
