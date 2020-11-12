# s3-client

This client is a wrapper around [minio-go](https://github.com/minio/minio-go).

## Installation
```shell
go get github.com/Clarilab/s3-client
```

## Importing
```go
import "github.com/Clarilab/s3-client"
```

## Features
```go
// Client holds all callable methods.
type Client interface {
	// AddLifeCycleRule adds a lifecycle rule to the given folder.
	AddLifeCycleRule(ruleID, folderPath string, daysToExpiry int) error

	// UploadFile uploads data under a given s3 path.
	UploadFile(path, contentType string, data io.Reader, objectSize *int64) error

	// UploadJSONFileWithLink uploads a file with content type "application/json" to the given s3 path.
	UploadJSONFileWithLink(path string, data io.Reader, linkExpiration time.Duration) (*url.URL, error)

	// GetFileURL creates a link with expiration for a document under the given path.
	GetFileURL(path string, expiration time.Duration) (*url.URL, error)

	// GetDocumentsInPath returns a list of names of all files under the given path.
	// The recursive option also lists all files from sub folders.
	GetDocumentsInPath(path string, recursive bool) ([]string, error)

	// DownloadFile returns the Document from given s3 path.
	DownloadFile(path string) (*Document, error)

	// DownloadDirectory returns a list of Documents from given s3 folder.
	DownloadDirectory(path string) ([]*Document, error)

	// DownloadFileToPath downloads the requested file to the file system under given localPath.
	DownloadFileToPath(path, localPath string) error

	// DownloadDirectoryToPath downloads the requested folder to the file system.
	// The recursive option also downloads all sub folders.
	DownloadDirectoryToPath(path, localPath string, recursive bool) error

	// RemoveFile deletes the file under given s3 path.
	RemoveFile(path string) error
}
```

## TODO
- [ ] add examples to documentation
