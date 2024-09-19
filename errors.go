package s3

import (
	"errors"
	"fmt"

	"github.com/minio/minio-go/v7"
)

var (
	// ErrEmptyHost occurs when the host is not specified.
	ErrEmptyHost = errors.New("host not specified")
	// ErrEmptyAccessKey occurs when the access key is not specified.
	ErrEmptyAccessKey = errors.New("access key not specified")
	// ErrEmptyAccessSecret occurs when the access secret is not specified.
	ErrEmptyAccessSecret = errors.New("access secret not specified")
	// ErrEmptyBucketName occurs when the bucket name is not specified.
	ErrEmptyBucketName = errors.New("bucket name not specified")
	// ErrNotFound indicates that the requested file does not exist.
	ErrNotFound = errors.New("file under specified filepath does not exist")
	// ErrChecksumMismatch occurs when the checksum of the downloaded file
	// does not match the expected checksum.
	ErrChecksumMismatch = errors.New("checksum mismatch")
)

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

func handleClientError(err error) error {
	const notFound = "NoSuchKey"

	var minioResponse minio.ErrorResponse

	if errors.As(err, &minioResponse) {
		switch minioResponse.Code {
		case notFound:
			return ErrNotFound
		default:
			return err
		}
	}

	return err
}
