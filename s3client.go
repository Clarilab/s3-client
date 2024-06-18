package s3

import (
	"context"
	"io"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
)

// Client holds all callable methods.
type Client interface {
	// Close closes the s3 client.
	Close()

	// IsOnline reports true if the client is online. If the health-check has not been enabled this will always return true.
	IsOnline() bool

	// AddLifeCycleRule adds a lifecycle rule to the given folder.
	AddLifeCycleRule(ctx context.Context, ruleID, folderPath string, daysToExpiry int) error

	// UploadFile uploads data under a given s3 path.
	UploadFile(ctx context.Context, path, contentType string, data io.Reader, objectSize *int64) (*minio.UploadInfo, error)

	// UploadFileWithOptions uploads data under a given s3 path with options.
	UploadFileWithOptions(ctx context.Context, path string, data io.Reader, objectSize *int64, options minio.PutObjectOptions) (*minio.UploadInfo, error)

	// GetFileURL creates a link with expiration for a file under the given path.
	GetFileURL(ctx context.Context, path string, expiration time.Duration) (*url.URL, error)

	// GetFileNamesInPath returns a list of names of all files under the given path.
	// The recursive option also lists all files from sub folders.
	GetFileNamesInPath(ctx context.Context, path string, recursive bool) ([]string, error)

	// GetFile returns the file from given s3 path.
	GetFile(ctx context.Context, path string) (*File, error)

	// GetFileWithOptions returns the file from given s3 path with options
	GetFileWithOptions(ctx context.Context, path string, options minio.GetObjectOptions) (*File, error)

	// GetDirectory returns a list of files from given s3 folder.
	GetDirectory(ctx context.Context, path string) ([]*File, error)

	// GetDirectoryWithOptions returns a list of files from given s3 folder with options.
	GetDirectoryWithOptions(ctx context.Context, path string, options minio.GetObjectOptions) ([]*File, error)

	// DownloadFile downloads the requested file to the file system under given localPath.
	DownloadFile(ctx context.Context, path, localPath string) error

	// DownloadFileWithOptions downloads the requested file to the file system under given localPath with minio options.
	DownloadFileWithOptions(ctx context.Context, path, localPath string, options minio.GetObjectOptions) error

	// DownloadDirectory downloads the requested folder to the file system.
	// The recursive option also downloads all sub folders.
	DownloadDirectory(ctx context.Context, path, localPath string, recursive bool) error

	// DownloadDirectoryWithOptions downloads the requested folder to the file system with options.
	// The recursive option also downloads all sub folders.
	DownloadDirectoryWithOptions(ctx context.Context, path, localPath string, recursive bool, options minio.GetObjectOptions) error

	// RemoveFile deletes the file under given s3 path.
	RemoveFile(ctx context.Context, path string) error

	// RemoveFileWithOptions deletes the file under given s3 path with minio options.
	RemoveFileWithOptions(ctx context.Context, path string, options minio.RemoveObjectOptions) error

	// GetObject returns an minio.Object for the given s3 path.
	// Don't forget to close the Object.
	GetObject(ctx context.Context, path string) (*minio.Object, error)

	// GetObjectWithOptions returns an minio.Object for the given s3 path with minio options.
	// Don't forget to close the Object.
	GetObjectWithOptions(ctx context.Context, path string, options minio.GetObjectOptions) (*minio.Object, error)
}
