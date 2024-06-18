package s3

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
)

func (c *client) UploadFile(ctx context.Context, path, contentType string, data io.Reader, objectSize *int64) (*minio.UploadInfo, error) {
	return c.UploadFileWithOptions(ctx, path, data, objectSize, minio.PutObjectOptions{
		ContentType:          contentType,
		DisableContentSha256: true,
	})
}

func (c *client) UploadFileWithOptions(ctx context.Context, path string, data io.Reader, objectSize *int64, options minio.PutObjectOptions) (*minio.UploadInfo, error) {
	const errMessage = "failed to upload file: %w"

	size := int64(-1)

	if objectSize != nil {
		size = *objectSize
	}

	info, err := c.minioClient.PutObject(
		ctx,
		c.bucketName,
		path,
		data,
		size,
		options,
	)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	return &info, nil
}

func (c *client) UploadJSONFileWithLink(
	ctx context.Context,
	path string,
	data io.Reader,
	linkExpiration time.Duration,
) (*url.URL, error) {
	const errMessage = "failed to upload json file: %w"

	_, err := c.minioClient.PutObject(
		ctx,
		c.bucketName,
		path,
		data,
		-1,
		minio.PutObjectOptions{ContentType: "application/json"},
	)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	u, err := c.minioClient.PresignedGetObject(
		ctx,
		c.bucketName,
		path,
		linkExpiration,
		c.urlValues,
	)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	return u, nil
}
