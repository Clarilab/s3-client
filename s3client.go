package s3

import (
	"context"
	"net/url"
	"time"
)

// Client holds all callable methods.
type Client interface {
	// UploadFile uploads data under a given s3 path.
	UploadFile(ctx context.Context, upload *Upload, options ...UploadOption) (*UploadInfo, error)

	// GetFile returns the file from given s3 path.
	GetFile(ctx context.Context, path string, options ...GetOption) (File, error)

	// GetObjectInfo returns an minio.ObjectInfo for the given s3 path.
	GetFileInfo(ctx context.Context, path string) (*FileInfo, error)

	// GetDirectory returns a list of files from given s3 folder.
	GetDirectory(ctx context.Context, path string, options ...GetDirectoryOption) ([]File, error)

	// GetDirectoryInfos returns a list of file infos for all files from given s3 folder.
	GetDirectoryInfos(ctx context.Context, path string) ([]*FileInfo, error)

	// DownloadFile downloads the requested file to the file system under given localPath.
	DownloadFile(ctx context.Context, path, localPath string, options ...DownloadOption) error

	// DownloadDirectory downloads the requested folder to the file system.
	// The recursive option also downloads all sub folders.
	DownloadDirectory(ctx context.Context, path, localPath string, recursive bool, options ...DownloadOption) error

	// RemoveFile deletes the file under given s3 path.
	RemoveFile(ctx context.Context, path string, options ...RemoveOption) error

	// AddLifeCycleRule adds a lifecycle rule to the given folder.
	AddLifeCycleRule(ctx context.Context, ruleID, folderPath string, daysToExpiry int) error

	// CreateFileLink creates a link with expiration for a file under the given path.
	CreateFileLink(ctx context.Context, path string, expiration time.Duration) (*url.URL, error)

	// Close closes the s3 client.
	Close()

	// IsOnline reports true if the client is online. If the health-check has not been enabled this will always return true.
	IsOnline() bool
}
