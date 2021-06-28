package s3

import (
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v6"
	"github.com/pkg/errors"
)

// Document holds document content and some meta data.
type Document struct {
	Content      []byte    `json:"content,omitempty"`
	ModifiedDate time.Time `json:"modifiedDate,omitempty"`
	ContentType  string    `json:"contentType,omitempty"`
	Name         string    `json:"name,omitempty"`
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
func NewClient(s3URL, accessKey, accessSecret, bucketName string, secure bool) (Client, error) {
	client, err := minio.New(s3URL, accessKey, accessSecret, secure)
	if err != nil {
		return nil, err
	}

	exists, err := client.BucketExists(bucketName)
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

func (s *s3) AddLifeCycleRule(ruleID, folderPath string, daysToExpiry int) error {
	if !strings.HasSuffix(folderPath, "/") {
		folderPath += "/"
	}

	lifeCycleString := fmt.Sprintf(
		`<LifecycleConfiguration><Rule><ID>%s</ID><Prefix>%s</Prefix><Status>Enabled</Status><Expiration><Days>%d</Days></Expiration></Rule></LifecycleConfiguration>`,
		ruleID, folderPath, daysToExpiry)

	return s.client.SetBucketLifecycle(s.bucketName, lifeCycleString)
}

func (s *s3) UploadFile(path, contentType string, data io.Reader, objectSize *int64) error {
	size := int64(-1)

	if objectSize != nil {
		size = *objectSize
	}

	_, err := s.client.PutObject(s.bucketName, path, data, size, minio.PutObjectOptions{ContentType: contentType})

	return err
}

func (s *s3) GetFileURL(path string, expiration time.Duration) (*url.URL, error) {
	return s.client.PresignedGetObject(s.bucketName, path, expiration, s.urlValues)
}

func (s *s3) GetDocumentsInPath(path string, recursive bool) ([]string, error) {
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := s.client.ListObjectsV2(s.bucketName, path, recursive, doneCh)
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

func (s *s3) UploadJSONFileWithLink(path string, data io.Reader, linkExpiration time.Duration) (*url.URL, error) {
	_, err := s.client.PutObject(s.bucketName, path, data, -1, minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return nil, err
	}

	return s.client.PresignedGetObject(s.bucketName, path, linkExpiration, s.urlValues)
}

func (s *s3) DownloadDirectory(path string) ([]*Document, error) {
	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := s.client.ListObjectsV2(s.bucketName, path, true, doneCh)
	wg := sync.WaitGroup{}
	errCh := make(chan error)

	result := make([]*Document, 0, len(objectCh))

	for obj := range objectCh {
		if obj.Err != nil {
			return nil, obj.Err
		}

		wg.Add(1)

		go func(obj minio.ObjectInfo, errChan chan<- error) {
			doc, err := s.DownloadFile(obj.Key)
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

func (s *s3) DownloadDirectoryToPath(path, localPath string, recursive bool) error {
	doneCh := make(chan struct{})
	defer close(doneCh)

	objectCh := s.client.ListObjectsV2(s.bucketName, path, recursive, doneCh)
	wg := sync.WaitGroup{}
	errCh := make(chan error)

	for obj := range objectCh {
		if obj.Err != nil {
			return obj.Err
		}

		wg.Add(1)

		go func(obj minio.ObjectInfo) {
			fileName := strings.TrimPrefix(obj.Key, path+"/")

			err := s.DownloadFileToPath(obj.Key, localPath+"/"+fileName)
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

func (s *s3) DownloadFileToPath(path, localPath string) error {
	return s.client.FGetObject(s.bucketName, path, localPath, minio.GetObjectOptions{})
}

func (s *s3) DownloadFile(path string) (*Document, error) {
	object, err := s.client.GetObject(s.bucketName, path, minio.GetObjectOptions{})
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
	}

	return document, nil
}

func (s *s3) RemoveFile(path string) error {
	return s.client.RemoveObject(s.bucketName, path)
}

func extractFilenameFromPath(path string) string {
	var docName string

	splittedPath := strings.Split(path, "/")
	if len(splittedPath) > 0 {
		docName = splittedPath[len(splittedPath)-1]
	}

	return docName
}
