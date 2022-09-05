package s3

import (
	"context"
	"io"
	"net/url"
	"time"
)

// Client holds all callable methods.
type Client interface {
	// AddLifeCycleRule adds a lifecycle rule to the given folder.
	AddLifeCycleRule(ctx context.Context, ruleID, folderPath string, daysToExpiry int) error

	// UploadFile uploads data under a given s3 path.
	UploadFile(ctx context.Context, path, contentType string, data io.Reader, objectSize *int64) error

	// UploadJSONFileWithLink uploads a file with content type "application/json" to the given s3 path.
	UploadJSONFileWithLink(ctx context.Context, path string, data io.Reader, linkExpiration time.Duration) (*url.URL, error)

	// GetFileURL creates a link with expiration for a document under the given path.
	GetFileURL(ctx context.Context, path string, expiration time.Duration) (*url.URL, error)

	// GetDocumentsInPath returns a list of names of all files under the given path.
	// The recursive option also lists all files from sub folders.
	GetDocumentsInPath(ctx context.Context, path string, recursive bool) ([]string, error)

	// DownloadFile returns the Document from given s3 path.
	DownloadFile(ctx context.Context, path string) (*Document, error)

	// DownloadDirectory returns a list of Documents from given s3 folder.
	DownloadDirectory(ctx context.Context, path string) ([]*Document, error)

	// DownloadFileToPath downloads the requested file to the file system under given localPath.
	DownloadFileToPath(ctx context.Context, path, localPath string) error

	// DownloadDirectoryToPath downloads the requested folder to the file system.
	// The recursive option also downloads all sub folders.
	DownloadDirectoryToPath(ctx context.Context, path, localPath string, recursive bool) error

	// RemoveFile deletes the file under given s3 path.
	RemoveFile(ctx context.Context, path string) error
}
