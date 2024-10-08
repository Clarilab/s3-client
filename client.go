package s3

import (
	"context"
	"fmt"
	"net/url"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const name = "s3"

type client struct {
	minioClient *minio.Client
	bucketName  string
	urlValues   url.Values
	cancelFunc  context.CancelFunc
	integritySettings
}

// NewClient instantiates a s3.
func NewClient(details *ClientDetails, options ...ClientOption) (Client, error) {
	const errMessage = "failed to create s3 client: %w"

	if err := details.validate(); err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	client := &client{
		bucketName: details.BucketName,
		urlValues:  make(url.Values),
		integritySettings: integritySettings{
			useIntegrityCRC32C: true,
			useIntegrityMD5:    false,
		},
	}

	var err error

	client.minioClient, err = minio.New(details.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(details.AccessKey, details.AccessSecret, ""),
		Secure: details.Secure,
	})
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	for i := range options {
		if err := options[i](client); err != nil {
			return nil, fmt.Errorf(errMessage, err)
		}
	}

	exists, err := client.minioClient.BucketExists(context.Background(), details.BucketName)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	if !exists {
		return nil, fmt.Errorf(errMessage, &BucketDoesNotExistError{details.BucketName})
	}

	client.urlValues.Set("response-content-disposition", "inline")

	return client, nil
}

func (c *client) Close() {
	if c.cancelFunc != nil {
		c.cancelFunc()
	}
}

func (c *client) IsOnline() bool {
	return c.minioClient.IsOnline()
}

func (c *client) IsHealthy() bool {
	return c.IsOnline()
}

func (c *client) GetName() string {
	return name
}
