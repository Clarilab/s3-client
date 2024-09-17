package s3

import "io"

// Upload represents a file that can be uploaded to the s3.
type Upload struct {
	io.ReadSeeker
	Path        string
	ContentType string
	MetaData    map[string]string
	Size        *int64
}

// NewUpload creates a new Upload instance.
func NewUpload(data io.ReadSeeker, size *int64, path, contentType string, metaData map[string]string) *Upload {
	return &Upload{
		ReadSeeker:  data,
		Path:        path,
		ContentType: contentType,
		MetaData:    metaData,
		Size:        size,
	}
}

// UploadInfo contains information about the uploaded file.
type UploadInfo struct {
	Size int64
	Integrity
}
