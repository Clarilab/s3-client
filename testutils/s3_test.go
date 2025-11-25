package testutils_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/Clarilab/s3-client/v4"
	"github.com/Clarilab/s3-client/v4/testutils"
)

func Test_NewClient(t *testing.T) {
	client, container, err := testutils.NewClient(t.Context(), "my-bucket")
	if err != nil {
		t.Fatalf("failed to create new client: %v", err)
	}

	defer func() {
		client.Close()

		if err := container.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %v", err)
		}
	}()

	content := []byte("Hello, World!")
	fileSize := int64(len(content))

	upload := s3.NewUpload(bytes.NewReader(content), &fileSize, "path/to/object.txt", "text/plain", nil)

	info, err := client.UploadFile(t.Context(), upload)
	if err != nil {
		t.Fatalf("failed to upload file: %v", err)
	}

	expectedCRC32CChecksum := "4d551068"

	if info == nil {
		t.Fatal("expected upload info, got nil")
	}

	if info.ChecksumCRC32C != expectedCRC32CChecksum {
		t.Fatalf("expected CRC32C checksum %s, got %s", expectedCRC32CChecksum, info.ChecksumCRC32C)
	}
}
