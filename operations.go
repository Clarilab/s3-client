package s3

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	pathpkg "path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
)

const (
	defaultUploadSize int64 = -1
)

func (c *client) UploadFile(ctx context.Context, upload *Upload, options ...UploadOption) (*UploadInfo, error) {
	const errMessage = "failed to upload file: %w"

	opts := new(uploadOptions)

	for i := range options {
		options[i](opts)
	}

	size := defaultUploadSize
	uploadSize := upload.Size

	if uploadSize != nil {
		size = *uploadSize
	}

	if opts.clientOptions.UserMetadata == nil {
		opts.clientOptions.UserMetadata = make(map[string]string)
	}

	var (
		crc32c string
		md5    string
	)

	if c.useIntegrityCRC32C {
		checksum, err := getCheckSumCRC32C(upload)
		if err != nil {
			return nil, fmt.Errorf(errMessage, err)
		}

		crc32c = checksum.hex()

		opts.clientOptions.UserMetadata[keyCR32CChecksum] = crc32c
	}

	if c.useIntegrityMD5 {
		checksum, err := getCheckSumMD5(upload)
		if err != nil {
			return nil, fmt.Errorf(errMessage, err)
		}

		md5 = checksum.hex()

		opts.clientOptions.UserMetadata[keyMD5Checksum] = md5
	}

	for k, v := range upload.MetaData {
		opts.clientOptions.UserMetadata[k] = v
	}

	contentType := upload.ContentType

	if contentType != "" {
		opts.clientOptions.ContentType = contentType
	}

	objInfo, err := c.minioClient.PutObject(
		ctx,
		c.bucketName,
		upload.Path,
		upload,
		size,
		minio.PutObjectOptions(opts.clientOptions),
	)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	info := &UploadInfo{
		Size: objInfo.Size,
		Integrity: Integrity{
			ChecksumCRC32C: crc32c,
			ChecksumMD5:    md5,
		},
	}

	return info, nil
}

func (c *client) GetFile(ctx context.Context, path string, options ...GetOption) (File, error) {
	const errMessage = "failed to get file from s3: %w"

	opts := new(getOptions)

	for i := range options {
		options[i](opts)
	}

	object, err := c.minioClient.GetObject(ctx, c.bucketName, path, minio.GetObjectOptions(opts.clientOptions))
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	objInfo, err := object.Stat()
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	if objInfo.Err != nil {
		return nil, fmt.Errorf(errMessage, objInfo.Err)
	}

	info := &FileInfo{
		Name:         pathpkg.Base(path),
		Path:         objInfo.Key,
		Size:         objInfo.Size,
		ContentType:  objInfo.ContentType,
		MetaData:     objInfo.UserMetadata,
		ModifiedDate: objInfo.LastModified,
	}

	if err = c.handleIntegrity(object, info, opts); err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	return &file{ReadCloser: object, info: info}, nil
}

func (c *client) GetFileInfo(ctx context.Context, path string) (*FileInfo, error) {
	const errMessage = "failed to get file info: %w"

	objInfo, err := c.minioClient.GetObjectACL(ctx, c.bucketName, path)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	if objInfo.Err != nil {
		return nil, fmt.Errorf(errMessage, objInfo.Err)
	}

	info := &FileInfo{
		Name:         pathpkg.Base(path),
		Path:         objInfo.Key,
		Size:         objInfo.Size,
		ContentType:  objInfo.ContentType,
		MetaData:     objInfo.UserMetadata,
		ModifiedDate: objInfo.LastModified,
	}

	c.handleGetFileInfoIntegrity(info)

	return info, nil
}

func (c *client) DownloadFile(ctx context.Context, path, localPath string, options ...DownloadOption) error {
	const errMessage = "failed to download file: %w"

	opts := new(downloadOptions)

	for i := range options {
		options[i](opts)
	}

	err := c.minioClient.FGetObject(
		ctx,
		c.bucketName,
		path,
		localPath,
		minio.GetObjectOptions(opts.clientOptions),
	)
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (c *client) GetDirectory(ctx context.Context, path string, options ...GetDirectoryOption) ([]File, error) {
	const errMessage = "failed to get directory: %w"

	getDirectoryOptions := new(getDirectoryOptions)

	for i := range options {
		options[i](getDirectoryOptions)
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := c.minioClient.ListObjects(ctx, c.bucketName, minio.ListObjectsOptions{
		Prefix:    path,
		Recursive: true,
	})

	wg := new(sync.WaitGroup)
	errCh := make(chan error)
	mtx := new(sync.Mutex)

	result := make([]File, 0, len(objectCh))

	for objInfo := range objectCh {
		if objInfo.Err != nil {
			return nil, fmt.Errorf(errMessage, objInfo.Err)
		}

		wg.Add(1)

		go func(info minio.ObjectInfo) {
			defer wg.Done()

			doc, err := c.GetFile(
				ctx,
				info.Key,
				[]GetOption{WithClientGetOptions(getDirectoryOptions.clientOptions)}...,
			)
			if err != nil {
				errCh <- err

				return
			}

			mtx.Lock()
			result = append(result, doc)
			mtx.Unlock()
		}(objInfo)
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

func (c *client) GetDirectoryInfos(ctx context.Context, path string) ([]*FileInfo, error) {
	const errMessage = "failed to get directory: %w"

	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := c.minioClient.ListObjects(ctx, c.bucketName, minio.ListObjectsOptions{
		Prefix:    path,
		Recursive: true,
	})

	wg := new(sync.WaitGroup)
	errCh := make(chan error)
	mtx := new(sync.Mutex)

	result := make([]*FileInfo, 0, len(objectCh))

	for objInfo := range objectCh {
		if objInfo.Err != nil {
			return nil, fmt.Errorf(errMessage, objInfo.Err)
		}

		wg.Add(1)

		go func(info minio.ObjectInfo) {
			defer wg.Done()

			fileInfo, err := c.GetFileInfo(ctx, info.Key)
			if err != nil {
				errCh <- err

				return
			}

			mtx.Lock()
			result = append(result, fileInfo)
			mtx.Unlock()
		}(objInfo)
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

func (c *client) DownloadDirectory(ctx context.Context, path, localPath string, recursive bool, options ...DownloadOption) error {
	const errMessage = "failed to download files from s3: %w"

	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := c.minioClient.ListObjects(ctx, c.bucketName, minio.ListObjectsOptions{
		Prefix:    path,
		Recursive: recursive,
	})

	wg := new(sync.WaitGroup)
	errCh := make(chan error)

	for objInfo := range objectCh {
		if objInfo.Err != nil {
			return fmt.Errorf(errMessage, objInfo.Err)
		}

		wg.Add(1)

		go func(info minio.ObjectInfo) {
			defer wg.Done()

			fileName := strings.TrimPrefix(info.Key, path+"/")

			err := c.DownloadFile(ctx, info.Key, localPath+"/"+fileName, options...)
			if err != nil {
				errCh <- err
			}
		}(objInfo)
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

func (c *client) RemoveFile(ctx context.Context, path string, options ...RemoveOption) error {
	const errMessage = "failed to remove file: %w"

	opts := new(removeOptions)

	for i := range options {
		options[i](opts)
	}

	if err := c.minioClient.RemoveObject(ctx, c.bucketName, path, minio.RemoveObjectOptions(opts.clientOptions)); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func (c *client) CreateFileLink(ctx context.Context, path string, expiration time.Duration) (*url.URL, error) {
	return c.minioClient.PresignedGetObject( //nolint:wrapcheck
		ctx,
		c.bucketName,
		path,
		expiration,
		c.urlValues,
	)
}

func (c *client) AddLifeCycleRule(ctx context.Context, ruleID, folderPath string, daysToExpiry int) error {
	const (
		errMessage    = "failed to add lifecycle rule: %w"
		statusEnabled = "Enabled"
	)

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
				Status: statusEnabled,
			},
		},
	})
	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}
