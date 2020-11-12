package s3

import (
	"io"
	"net/url"
	"time"
)

// Client holds all callable methods.
type Client interface {
	AddLifeCycleRule(ruleID, folderPath string, daysToExpiry int) error
	UploadFile(path, contentType string, data io.Reader, objectSize *int64) error
	UploadJSONFileWithLink(path string, data io.Reader, linkExpiration time.Duration) (*url.URL, error)

	GetFileURL(path string, expiration time.Duration) (*url.URL, error)
	GetDocumentsInPath(path string, recursive bool) ([]string, error)

	DownloadFile(path string) (*Document, error)
	DownloadDirectory(path string) ([]*Document, error)
	DownloadFileToPath(path, localPath string) error
	DownloadDirectoryToPath(path, localPath string) error

	RemoveFile(path string) error
}
