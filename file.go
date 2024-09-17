package s3

import (
	"errors"
	"fmt"
	"io"
	"time"
)

// File is a file downloaded from s3.
type File interface {
	io.ReadCloser
	// Info returns the file information.
	Info() *FileInfo
	// Bytes reads the entire file and returns its content as a byte slice.
	//
	// Note: The file is closed and must not be read after using this function!
	Bytes() ([]byte, error)
}

type file struct {
	io.ReadCloser
	info *FileInfo
}

// Info implements the File interface.
func (f *file) Info() *FileInfo {
	return f.info
}

// Bytes implements the File interface.
func (f *file) Bytes() ([]byte, error) {
	const errMessage = "failed to read file: %w"

	defer f.Close()

	buf := make([]byte, f.info.Size)

	_, err := f.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf(errMessage, err)
	}

	return buf, nil
}

// FileInfo contains information about a file.
type FileInfo struct {
	Name         string
	Path         string
	Size         int64
	ContentType  string
	MetaData     map[string]string
	ModifiedDate time.Time
	Integrity
}
