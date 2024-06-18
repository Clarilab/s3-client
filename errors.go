package s3

import (
	"errors"
	"fmt"
)

// ErrNotFound indicates that the requested file does not exist.
var ErrNotFound = errors.New("file under specified filepath does not exist")

// BucketDoesNotExistError occurs when the given bucket does not exist.
type BucketDoesNotExistError struct {
	bucketName string
}

// Error implements the error interface.
func (e *BucketDoesNotExistError) Error() string {
	return fmt.Sprintf("bucket '%s' does not exist", e.bucketName)
}

// DownloadingFilesFailedError occurs when downloading files from s3 failed.
type DownloadingFilesFailedError struct {
	errs []error
}

// DownloadingFilesFailedError implements the error interface.
func (e *DownloadingFilesFailedError) Error() string {
	return fmt.Sprintf("failed to download files from s3: %v", e.errs)
}
