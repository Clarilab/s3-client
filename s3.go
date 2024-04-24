package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
)

// Document holds document content and some meta data.
type Document struct {
	Content      []byte            `json:"content,omitempty"`
	ModifiedDate time.Time         `json:"modifiedDate,omitempty"`
	ContentType  string            `json:"contentType,omitempty"`
	Name         string            `json:"name,omitempty"`
	MetaData     map[string]string `json:"metadata,omitempty"`
}

// ErrNotFound indicates that the requested document does not exist.
var ErrNotFound = errors.New("document under specified filepath does not exist")

type s3 struct {
	client         *minio.Client
	lifeCycleRules string
	bucketName     string
	urlValues      url.Values
}

// NewClient instantiates a s3.
func NewClient(ctx context.Context, s3URL, accessKey, accessSecret, bucketName string, secure bool) (Client, error) {
	client, err := minio.New(s3URL, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, accessSecret, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, err
	}

	exists, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("s3 bucket required for s3 (%s) doesn't exist", bucketName)
	}

	urlValues := make(url.Values)
	urlValues.Set("response-content-disposition", "inline")

	return &s3{
		client:         client,
		lifeCycleRules: "",
		bucketName:     bucketName,
		urlValues:      urlValues,
	}, nil
}

func (s *s3) AddLifeCycleRule(ctx context.Context, ruleID, folderPath string, daysToExpiry int) error {
	if !strings.HasSuffix(folderPath, "/") {
		folderPath += "/"
	}

	return s.client.SetBucketLifecycle(ctx, s.bucketName, &lifecycle.Configuration{
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
}

func (s *s3) UploadFile(ctx context.Context, path, contentType string, data io.Reader, objectSize *int64, options ...func(*minio.PutObjectOptions)) error {
	size := int64(-1)

	if objectSize != nil {
		size = *objectSize
	}

	minioOptions := minio.PutObjectOptions{
		ContentType: contentType,
	}

	for _, option := range options {
		option(&minioOptions)
	}

	_, err := s.client.PutObject(
		ctx,
		s.bucketName,
		path,
		data,
		size,
		minioOptions,
	)

	return err
}

func (s *s3) GetFileURL(ctx context.Context, path string, expiration time.Duration) (*url.URL, error) {
	return s.client.PresignedGetObject(
		ctx,
		s.bucketName,
		path,
		expiration,
		s.urlValues,
	)
}

func (s *s3) GetDocumentsInPath(ctx context.Context, path string, recursive bool) ([]string, error) {
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	objectCh := s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
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

func (s *s3) UploadJSONFileWithLink(
	ctx context.Context,
	path string,
	data io.Reader,
	linkExpiration time.Duration,
) (*url.URL, error) {
	_, err := s.client.PutObject(
		ctx,
		s.bucketName,
		path,
		data,
		-1,
		minio.PutObjectOptions{ContentType: "application/json"},
	)
	if err != nil {
		return nil, err
	}

	return s.client.PresignedGetObject(
		ctx,
		s.bucketName,
		path,
		linkExpiration,
		s.urlValues,
	)
}

func (s *s3) DownloadDirectory(ctx context.Context, path string) ([]*Document, error) {
	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    path,
		Recursive: true,
	})
	wg := sync.WaitGroup{}
	errCh := make(chan error)

	result := make([]*Document, 0, len(objectCh))

	for obj := range objectCh {
		if obj.Err != nil {
			return nil, obj.Err
		}

		wg.Add(1)

		go func(obj minio.ObjectInfo, errChan chan<- error) {
			doc, err := s.DownloadFile(ctx, obj.Key)
			if err != nil {
				errCh <- err

				return
			}

			result = append(result, doc)

			wg.Done()
		}(obj, errCh)
	}

	wg.Wait()
	close(errCh)

	errs := make([]error, len(errCh))
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("failed to download files from s3: %v", errs)
	}

	return result, nil
}

func (s *s3) DownloadDirectoryToPath(ctx context.Context, path, localPath string, recursive bool) error {
	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    path,
		Recursive: recursive,
	})
	wg := sync.WaitGroup{}
	errCh := make(chan error)

	for obj := range objectCh {
		if obj.Err != nil {
			return obj.Err
		}

		wg.Add(1)

		go func(obj minio.ObjectInfo) {
			fileName := strings.TrimPrefix(obj.Key, path+"/")

			err := s.DownloadFileToPath(ctx, obj.Key, localPath+"/"+fileName)
			if err != nil {
				errCh <- err
			}

			wg.Done()
		}(obj)
	}

	wg.Wait()
	close(errCh)

	errs := make([]error, len(errCh))
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to download files from s3: %v", errs)
	}

	return nil
}

func (s *s3) DownloadFileToPath(ctx context.Context, path, localPath string) error {
	return s.client.FGetObject(
		ctx,
		s.bucketName,
		path,
		localPath,
		minio.GetObjectOptions{},
	)
}

func (s *s3) GetObject(ctx context.Context, path string) (*minio.Object, error) {
	return s.client.GetObject(
		ctx,
		s.bucketName,
		path,
		minio.GetObjectOptions{},
	)
}

func (s *s3) DownloadFile(ctx context.Context, path string) (*Document, error) {
	object, err := s.client.GetObject(ctx, s.bucketName, path, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	defer object.Close()

	fileInfo, err := object.Stat()
	if err != nil {
		return nil, err
	}

	content := make([]byte, fileInfo.Size)

	_, err = object.Read(content)
	if err != nil && err != io.EOF {
		var minioResponse minio.ErrorResponse

		if errors.As(err, &minioResponse) {
			return nil, ErrNotFound
		}

		return nil, err
	}

	docName := extractFilenameFromPath(path)

	document := &Document{
		Content:      content,
		ModifiedDate: fileInfo.LastModified,
		ContentType:  fileInfo.ContentType,
		Name:         docName,
		MetaData:     fileInfo.UserMetadata,
	}

	return document, nil
}

func (s *s3) RemoveFile(ctx context.Context, path string) error {
	return s.client.RemoveObject(ctx, s.bucketName, path, minio.RemoveObjectOptions{})
}

func extractFilenameFromPath(path string) string {
	var docName string

	splittedPath := strings.Split(path, "/")
	if len(splittedPath) > 0 {
		docName = splittedPath[len(splittedPath)-1]
	}

	return docName
}
