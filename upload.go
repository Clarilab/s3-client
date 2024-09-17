package s3

import "io"

// Upload represents a file that can be uploaded to the s3.
type Upload interface {
	io.ReadSeeker
	Path() string
	ContentType() string
	MetaData() map[string]string
	Size() *int64
}

// NewUpload creates a new Upload instance.
func NewUpload(data io.ReadSeeker, size *int64, path, contentType string, metaData map[string]string) Upload {
	return &upload{
		ReadSeeker:  data,
		path:        path,
		contentType: contentType,
		metaData:    metaData,
		size:        size,
	}
}

type upload struct {
	io.ReadSeeker
	path        string
	contentType string
	metaData    map[string]string
	size        *int64
}

// Path implements the Upload interface.
func (u *upload) Path() string {
	return u.path
}

// ContentType implements the Upload interface.
func (u *upload) ContentType() string {
	return u.contentType
}

// MetaData implements the Upload interface.
func (u *upload) MetaData() map[string]string {
	return u.metaData
}

// Size implements the Upload interface.
func (u *upload) Size() *int64 {
	return u.size
}

// UploadInfo contains information about the uploaded file.
type UploadInfo struct {
	Size int64
	Integrity
}
