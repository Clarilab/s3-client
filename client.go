package s3

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
)

type client struct {
	minioClient *minio.Client
	bucketName  string
	urlValues   url.Values
	cancelFunc  context.CancelFunc
}

// NewClient instantiates a s3.
func NewClient(ctx context.Context, s3URL, accessKey, accessSecret, bucketName string, secure bool, options ...Option) (Client, error) {
	const errMessage = "failed to create s3 client: %w"
	client := &client{
		bucketName: bucketName,
		urlValues:  make(url.Values),
	}

	var err error

	client.minioClient, err = minio.New(s3URL, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, accessSecret, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	for i := range options {
		if err := options[i](client); err != nil {
			return nil, fmt.Errorf(errMessage, err)
		}
	}

	exists, err := client.minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	if !exists {
		return nil, fmt.Errorf(errMessage, &BucketDoesNotExistError{bucketName})
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

func (c *client) AddLifeCycleRule(ctx context.Context, ruleID, folderPath string, daysToExpiry int) error {
	const errMessage = "failed to add lifecycle rule: %w"

	if !strings.HasSuffix(folderPath, "/") {
		folderPath += "/"
	}

	err := c.minioClient.SetBucketLifecycle(ctx, c.bucketName, &lifecycle.Configuration{
		XMLName: xml.Name{},
		Rules: []lifecycle.Rule{
			{
				ID: ruleID,
				Expiration: lifecycle.Expiration{
					Days: lifecycle.ExpirationDays(daysToExpiry),
				},
				Prefix: folderPath,
				Status: "Enabled",
			},
		},
	})
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}
