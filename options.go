package s3

import (
	"fmt"
	"time"
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
