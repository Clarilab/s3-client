package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	pathpkg "path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
)

// File holds the file content and some meta data.
type File struct {
	Content      []byte            `json:"content,omitempty"`
	Length       int64             `json:"length,omitempty"`
	ModifiedDate time.Time         `json:"modifiedDate,omitempty"`
	ContentType  string            `json:"contentType,omitempty"`
	Name         string            `json:"name,omitempty"`
	MetaData     map[string]string `json:"metadata,omitempty"`
	Checksum     string            `json:"checksum,omitempty"` // empty if not requested via options
}

func (c *client) GetFileURL(ctx context.Context, path string, expiration time.Duration) (*url.URL, error) {
	return c.minioClient.PresignedGetObject( //nolint:wrapcheck
		ctx,
		c.bucketName,
		path,
		expiration,
		c.urlValues,
	)
}

func (c *client) GetFileNamesInPath(ctx context.Context, path string, recursive bool) ([]string, error) {
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	objectCh := c.minioClient.ListObjects(ctx, c.bucketName, minio.ListObjectsOptions{
		Prefix:    path,
		Recursive: recursive,
	})
	result := make([]string, 0, len(objectCh))

	for obj := range objectCh {
		if obj.Err != nil {
			return nil, obj.Err
		}

		fileName := strings.TrimPrefix(obj.Key, path)
		result = append(result, fileName)
	}

	return result, nil
}

func (c *client) DownloadFile(ctx context.Context, path string) (*File, error) {
	return c.DownloadFileWithOptions(ctx, path, minio.GetObjectOptions{})
}

func (c *client) DownloadFileWithOptions(ctx context.Context, path string, options minio.GetObjectOptions) (*File, error) {
	const errMessage = "failed to download file from s3: %w"

	object, err := c.minioClient.GetObject(ctx, c.bucketName, path, options)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	defer object.Close()

	fileInfo, err := object.Stat()
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	content := make([]byte, fileInfo.Size)

	_, err = object.Read(content)
	if err != nil && !errors.Is(err, io.EOF) {
		var minioResponse minio.ErrorResponse

		if errors.As(err, &minioResponse) {
			return nil, ErrNotFound
		}

		return nil, fmt.Errorf(errMessage, err)
	}

	return &File{
		Content:      content,
		ModifiedDate: fileInfo.LastModified,
		ContentType:  fileInfo.ContentType,
		Name:         pathpkg.Base(path),
		MetaData:     fileInfo.UserMetadata,
		Checksum:     fileInfo.ChecksumCRC32C,
		Length:       fileInfo.Size,
	}, nil
}

func (c *client) DownloadFileToLocalPath(ctx context.Context, path, localPath string) error {
	return c.DownloadFileToLocalPathWithOptions(ctx, path, localPath, minio.GetObjectOptions{})
}

func (c *client) DownloadFileToLocalPathWithOptions(ctx context.Context, path, localPath string, options minio.GetObjectOptions) error {
	return c.minioClient.FGetObject( //nolint:wrapcheck
		ctx,
		c.bucketName,
		path,
		localPath,
		options,
	)
}

func (c *client) DownloadDirectory(ctx context.Context, path string) ([]*File, error) {
	return c.DownloadDirectoryWithOptions(ctx, path, minio.GetObjectOptions{})
}

func (c *client) DownloadDirectoryWithOptions(ctx context.Context, path string, options minio.GetObjectOptions) ([]*File, error) {
	const errMessage = "failed to download directory: %w"

	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := c.minioClient.ListObjects(ctx, c.bucketName, minio.ListObjectsOptions{
		Prefix:    path,
		Recursive: true,
	})

	wg := new(sync.WaitGroup)
	errCh := make(chan error)
	result := make([]*File, 0, len(objectCh))

	for obj := range objectCh {
		if obj.Err != nil {
			return nil, fmt.Errorf(errMessage, obj.Err)
		}

		wg.Add(1)

		go func(obj minio.ObjectInfo) {
			doc, err := c.DownloadFileWithOptions(ctx, obj.Key, options)
			if err != nil {
				errCh <- err

				return
			}

			result = append(result, doc)

			wg.Done()
		}(obj)
	}

	wg.Wait()
	close(errCh)

	errs := make([]error, 0, len(errCh))
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf(errMessage, &DownloadingFilesFailedError{errs})
	}

	return result, nil
}

func (c *client) DownloadDirectoryToLocalPath(ctx context.Context, path, localPath string, recursive bool) error {
	return c.DownloadDirectoryToLocalPathWithOptions(ctx, path, localPath, recursive, minio.GetObjectOptions{})
}

func (c *client) DownloadDirectoryToLocalPathWithOptions(ctx context.Context, path, localPath string, recursive bool, options minio.GetObjectOptions) error {
	const errMessage = "failed to download files from s3: %w"

	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := c.minioClient.ListObjects(ctx, c.bucketName, minio.ListObjectsOptions{
		Prefix:    path,
		Recursive: recursive,
	})

	wg := new(sync.WaitGroup)
	errCh := make(chan error)

	for obj := range objectCh {
		if obj.Err != nil {
			return fmt.Errorf(errMessage, obj.Err)
		}

		wg.Add(1)

		go func(obj minio.ObjectInfo) {
			fileName := strings.TrimPrefix(obj.Key, path+"/")

			err := c.DownloadFileToLocalPathWithOptions(ctx, obj.Key, localPath+"/"+fileName, options)
			if err != nil {
				errCh <- err
			}

			wg.Done()
		}(obj)
	}

	wg.Wait()
	close(errCh)

	errs := make([]error, 0, len(errCh))
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf(errMessage, &DownloadingFilesFailedError{errs})
	}

	return nil
}

func (c *client) GetObject(ctx context.Context, path string) (*minio.Object, error) {
	return c.GetObjectWithOptions(ctx, path, minio.GetObjectOptions{})
}

func (c *client) GetObjectWithOptions(ctx context.Context, path string, options minio.GetObjectOptions) (*minio.Object, error) {
	return c.minioClient.GetObject( //nolint:wrapcheck
		ctx,
		c.bucketName,
		path,
		options,
	)
}

func (c *client) RemoveFile(ctx context.Context, path string) error {
	return c.RemoveFileWithOptions(ctx, path, minio.RemoveObjectOptions{})
}

func (c *client) RemoveFileWithOptions(ctx context.Context, path string, options minio.RemoveObjectOptions) error {
	return c.minioClient.RemoveObject(ctx, c.bucketName, path, options) //nolint:wrapcheck
}
