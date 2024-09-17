package s3

import (
	"context"
	"fmt"
	"net/url"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type client struct {
	minioClient *minio.Client
	bucketName  string
	urlValues   url.Values
	cancelFunc  context.CancelFunc
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exists, err := client.minioClient.BucketExists(ctx, details.BucketName)
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
