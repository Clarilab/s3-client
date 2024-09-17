package s3_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/Clarilab/s3-client/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func Test_GetChecksumCRC32C(t *testing.T) {
	t.Parallel()

	expectedChecksum := "4748d6bb"

	data := strings.NewReader("asdfqweryxcv")

	checksum, err := s3.GenerateCheckSumCRC32C(data)
	if err != nil {
		t.Fatal(err)
	}

	if checksum != expectedChecksum {
		t.Errorf("unexpected checksum: %v", checksum)
	}
}

func Test_GetChecksumMD5(t *testing.T) {
	t.Parallel()

	expectedChecksum := "443217297805b7b46584cea3c26980f0"

	data := strings.NewReader("asdfqweryxcv")

	checksum, err := s3.GenerateCheckSumMD5(data)
	if err != nil {
		t.Fatal(err)
	}

	if checksum != expectedChecksum {
		t.Errorf("unexpected checksum: %v", checksum)
	}
}

func Test_Integrity_UploadFile(t *testing.T) {
	t.Parallel()

	const folder = "test-integrity-upload-file"

	t.Run("upload file with crc32c integrity support", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t)

		content, err := testData.ReadFile(testDataFolder + "/" + testFile1Name)
		require.NoError(t, err)

		lenTestFile := int64(len(content))

		fileName := uuid.NewString()

		filePath := folder + "/" + fileName

		metaData := map[string]string{headerFileName: testFile1Name}

		upload := s3.NewUpload(bytes.NewReader(content), &lenTestFile, filePath, contentType, metaData)

		expectedChecksum, err := s3.GenerateCheckSumCRC32C(bytes.NewReader(content))
		require.NoError(t, err)

		info, err := s3Client.UploadFile(context.Background(), upload)
		require.NoError(t, err)

		require.Equal(t, lenTestFile, info.Size)
		require.Equal(t, expectedChecksum, info.ChecksumCRC32C)
	})

	t.Run("upload file with md5 integrity support", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false), s3.WithMD5IntegritySupport(true))

		content, err := testData.ReadFile(testDataFolder + "/" + testFile1Name)
		require.NoError(t, err)

		lenTestFile := int64(len(content))

		fileName := uuid.NewString()

		filePath := folder + "/" + fileName

		metaData := map[string]string{headerFileName: testFile1Name}

		upload := s3.NewUpload(bytes.NewReader(content), &lenTestFile, filePath, contentType, metaData)

		expectedChecksum, err := s3.GenerateCheckSumMD5(bytes.NewReader(content))
		require.NoError(t, err)

		info, err := s3Client.UploadFile(context.Background(), upload)
		require.NoError(t, err)

		require.Equal(t, lenTestFile, info.Size)
		require.Equal(t, expectedChecksum, info.ChecksumMD5)
	})

	t.Run("upload file without crc32c integrity support", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false), s3.WithMD5IntegritySupport(false))

		content, err := testData.ReadFile(testDataFolder + "/" + testFile1Name)
		require.NoError(t, err)

		lenTestFile := int64(len(content))

		fileName := uuid.NewString()

		filePath := folder + "/" + fileName

		metaData := map[string]string{headerFileName: testFile1Name}

		upload := s3.NewUpload(bytes.NewReader(content), &lenTestFile, filePath, contentType, metaData)

		info, err := s3Client.UploadFile(context.Background(), upload)
		require.NoError(t, err)

		require.Equal(t, lenTestFile, info.Size)
		require.Empty(t, info.ChecksumCRC32C)
	})

	t.Run("upload file without md5 integrity support", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false), s3.WithMD5IntegritySupport(false))

		content, err := testData.ReadFile(testDataFolder + "/" + testFile1Name)
		require.NoError(t, err)

		lenTestFile := int64(len(content))

		fileName := uuid.NewString()

		filePath := folder + "/" + fileName

		metaData := map[string]string{headerFileName: testFile1Name}

		upload := s3.NewUpload(bytes.NewReader(content), &lenTestFile, filePath, contentType, metaData)

		info, err := s3Client.UploadFile(context.Background(), upload)
		require.NoError(t, err)

		require.Equal(t, lenTestFile, info.Size)
		require.Empty(t, info.ChecksumMD5)
	})
}

func Test_Integrity_GetFile(t *testing.T) {
	t.Parallel()

	const folder = "test-integrity-get-file"

	t.Run("get file crc32c", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t)

		uploaded := uploadTestFileWithClient(t, s3Client, folder, testFile1Name)

		file, err := s3Client.GetFile(context.Background(), uploaded.filePath)
		require.NoError(t, err)

		fileContent, err := file.Bytes()
		require.NoError(t, err)

		fileInfo := file.Info()

		expectedChecksum, err := s3.GenerateCheckSumCRC32C(bytes.NewReader(uploaded.content))
		require.NoError(t, err)

		require.Equal(t, uploaded.content, fileContent)
		require.Equal(t, uploaded.lenTestFile, fileInfo.Size)
		require.Equal(t, uploaded.contentType, fileInfo.ContentType)
		require.Equal(t, uploaded.fileName, fileInfo.Name)
		require.Equal(t, uploaded.metaData, fileInfo.MetaData)
		require.Equal(t, expectedChecksum, fileInfo.ChecksumCRC32C)
		require.False(t, fileInfo.ModifiedDate.IsZero())
	})

	t.Run("get file md5", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false), s3.WithMD5IntegritySupport(true))

		uploaded := uploadTestFileWithClient(t, s3Client, folder, testFile1Name)

		file, err := s3Client.GetFile(context.Background(), uploaded.filePath)
		require.NoError(t, err)

		fileContent, err := file.Bytes()
		require.NoError(t, err)

		fileInfo := file.Info()

		expectedChecksum, err := s3.GenerateCheckSumMD5(bytes.NewReader(uploaded.content))
		require.NoError(t, err)

		require.Equal(t, uploaded.content, fileContent)
		require.Equal(t, uploaded.lenTestFile, fileInfo.Size)
		require.Equal(t, uploaded.contentType, fileInfo.ContentType)
		require.Equal(t, uploaded.fileName, fileInfo.Name)
		require.Equal(t, uploaded.metaData, fileInfo.MetaData)
		require.Equal(t, expectedChecksum, fileInfo.ChecksumMD5)
		require.False(t, fileInfo.ModifiedDate.IsZero())
	})

	t.Run("get file with crc32c check", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t)

		uploaded := uploadTestFileWithClient(t, s3Client, folder, testFile2Name)

		expectedChecksum, err := s3.GenerateCheckSumCRC32C(bytes.NewReader(uploaded.content))
		require.NoError(t, err)

		file, err := s3Client.GetFile(context.Background(), uploaded.filePath, s3.WithIntegrityCheckCRC32C(expectedChecksum))
		require.NoError(t, err)

		fileContent, err := file.Bytes()
		require.NoError(t, err)

		fileInfo := file.Info()

		require.Equal(t, uploaded.content, fileContent)
		require.Equal(t, uploaded.lenTestFile, fileInfo.Size)
		require.Equal(t, uploaded.contentType, fileInfo.ContentType)
		require.Equal(t, uploaded.fileName, fileInfo.Name)
		require.Equal(t, uploaded.metaData, fileInfo.MetaData)
		require.Equal(t, expectedChecksum, fileInfo.ChecksumCRC32C)
		require.False(t, fileInfo.ModifiedDate.IsZero())
	})

	t.Run("get file with md5 check", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false), s3.WithMD5IntegritySupport(true))

		uploaded := uploadTestFileWithClient(t, s3Client, folder, testFile2Name)

		expectedChecksum, err := s3.GenerateCheckSumMD5(bytes.NewReader(uploaded.content))
		require.NoError(t, err)

		file, err := s3Client.GetFile(context.Background(), uploaded.filePath, s3.WithIntegrityCheckMD5(expectedChecksum))
		require.NoError(t, err)

		fileContent, err := file.Bytes()
		require.NoError(t, err)

		fileInfo := file.Info()

		require.Equal(t, uploaded.content, fileContent)
		require.Equal(t, uploaded.lenTestFile, fileInfo.Size)
		require.Equal(t, uploaded.contentType, fileInfo.ContentType)
		require.Equal(t, uploaded.fileName, fileInfo.Name)
		require.Equal(t, uploaded.metaData, fileInfo.MetaData)
		require.Equal(t, expectedChecksum, fileInfo.ChecksumMD5)
		require.False(t, fileInfo.ModifiedDate.IsZero())
	})

	t.Run("get file with crc32c check without cr3c integrity support", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false), s3.WithMD5IntegritySupport(false))

		uploaded := uploadTestFileWithClient(t, s3Client, folder, testFile1Name)

		expectedChecksum, err := s3.GenerateCheckSumCRC32C(bytes.NewReader(uploaded.content))
		require.NoError(t, err)

		file, err := s3Client.GetFile(context.Background(), uploaded.filePath, s3.WithIntegrityCheckCRC32C(expectedChecksum))
		require.NoError(t, err)

		fileContent, err := file.Bytes()
		require.NoError(t, err)

		fileInfo := file.Info()

		require.Equal(t, uploaded.content, fileContent)
		require.Equal(t, uploaded.lenTestFile, fileInfo.Size)
		require.Equal(t, uploaded.contentType, fileInfo.ContentType)
		require.Equal(t, uploaded.fileName, fileInfo.Name)
		require.Equal(t, uploaded.metaData, fileInfo.MetaData)
		require.Equal(t, expectedChecksum, fileInfo.ChecksumCRC32C)
		require.False(t, fileInfo.ModifiedDate.IsZero())
	})

	t.Run("get file with md5 check without md5 integrity support", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false), s3.WithMD5IntegritySupport(false))

		uploaded := uploadTestFileWithClient(t, s3Client, folder, testFile1Name)

		expectedChecksum, err := s3.GenerateCheckSumMD5(bytes.NewReader(uploaded.content))
		require.NoError(t, err)

		file, err := s3Client.GetFile(context.Background(), uploaded.filePath, s3.WithIntegrityCheckMD5(expectedChecksum))
		require.NoError(t, err)

		fileContent, err := file.Bytes()
		require.NoError(t, err)

		fileInfo := file.Info()

		require.Equal(t, uploaded.content, fileContent)
		require.Equal(t, uploaded.lenTestFile, fileInfo.Size)
		require.Equal(t, uploaded.contentType, fileInfo.ContentType)
		require.Equal(t, uploaded.fileName, fileInfo.Name)
		require.Equal(t, uploaded.metaData, fileInfo.MetaData)
		require.Equal(t, expectedChecksum, fileInfo.ChecksumMD5)
		require.False(t, fileInfo.ModifiedDate.IsZero())
	})

	t.Run("invalid crc32c checksum", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t)

		uploaded := uploadTestFileWithClient(t, s3Client, folder, testFile1Name)

		_, err := s3Client.GetFile(context.Background(), uploaded.filePath, s3.WithIntegrityCheckCRC32C("invalid-checksum"))
		require.ErrorIs(t, err, s3.ErrChecksumMismatch)
	})

	t.Run("invalid md5 checksum", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t)

		uploaded := uploadTestFileWithClient(t, s3Client, folder, testFile1Name)

		_, err := s3Client.GetFile(context.Background(), uploaded.filePath, s3.WithIntegrityCheckMD5("invalid-checksum"))
		require.ErrorIs(t, err, s3.ErrChecksumMismatch)
	})
}

func Test_Integrity_GetFileInfo(t *testing.T) {
	t.Parallel()

	const folder = "test-integrity-get-file-info"

	t.Run("get file uploaded with crc32c integrity support", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t)

		uploaded := uploadTestFileWithClient(t, s3Client, folder, testFile1Name)

		expectedChecksum, err := s3.GenerateCheckSumCRC32C(bytes.NewReader(uploaded.content))
		require.NoError(t, err)

		fileInfo, err := s3Client.GetFileInfo(context.Background(), uploaded.filePath)
		require.NoError(t, err)

		require.Equal(t, uploaded.lenTestFile, fileInfo.Size)
		require.Equal(t, uploaded.contentType, fileInfo.ContentType)
		require.Equal(t, uploaded.fileName, fileInfo.Name)
		require.Equal(t, uploaded.metaData, fileInfo.MetaData)
		require.Equal(t, expectedChecksum, fileInfo.ChecksumCRC32C)
		require.False(t, fileInfo.ModifiedDate.IsZero())
	})

	t.Run("get file uploaded with md5 integrity support", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false), s3.WithMD5IntegritySupport(true))

		uploaded := uploadTestFileWithClient(t, s3Client, folder, testFile1Name)

		expectedChecksum, err := s3.GenerateCheckSumMD5(bytes.NewReader(uploaded.content))
		require.NoError(t, err)

		fileInfo, err := s3Client.GetFileInfo(context.Background(), uploaded.filePath)
		require.NoError(t, err)

		require.Equal(t, uploaded.lenTestFile, fileInfo.Size)
		require.Equal(t, uploaded.contentType, fileInfo.ContentType)
		require.Equal(t, uploaded.fileName, fileInfo.Name)
		require.Equal(t, uploaded.metaData, fileInfo.MetaData)
		require.Equal(t, expectedChecksum, fileInfo.ChecksumMD5)
		require.False(t, fileInfo.ModifiedDate.IsZero())
	})

	t.Run("get file uploaded without crc32c integrity support", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false), s3.WithMD5IntegritySupport(false))

		uploaded := uploadTestFileWithClient(t, s3Client, folder, testFile1Name)

		fileInfo, err := s3Client.GetFileInfo(context.Background(), uploaded.filePath)
		require.NoError(t, err)

		require.Equal(t, uploaded.lenTestFile, fileInfo.Size)
		require.Equal(t, uploaded.contentType, fileInfo.ContentType)
		require.Equal(t, uploaded.fileName, fileInfo.Name)
		require.Equal(t, uploaded.metaData, fileInfo.MetaData)
		require.False(t, fileInfo.ModifiedDate.IsZero())
		require.Empty(t, fileInfo.ChecksumCRC32C)
	})

	t.Run("get file uploaded without md5 integrity support", func(t *testing.T) {
		t.Parallel()

		s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false), s3.WithMD5IntegritySupport(false))

		uploaded := uploadTestFileWithClient(t, s3Client, folder, testFile1Name)

		fileInfo, err := s3Client.GetFileInfo(context.Background(), uploaded.filePath)
		require.NoError(t, err)

		require.Equal(t, uploaded.lenTestFile, fileInfo.Size)
		require.Equal(t, uploaded.contentType, fileInfo.ContentType)
		require.Equal(t, uploaded.fileName, fileInfo.Name)
		require.Equal(t, uploaded.metaData, fileInfo.MetaData)
		require.False(t, fileInfo.ModifiedDate.IsZero())
		require.Empty(t, fileInfo.ChecksumMD5)
	})
}

func uploadTestFileWithClient(t *testing.T, client s3.Client, s3Folder, testFileName string) *uploaded {
	t.Helper()

	content, err := testData.ReadFile(testDataFolder + "/" + testFileName)
	require.NoError(t, err)

	lenTestFile := int64(len(content))

	fileName := uuid.NewString()

	filePath := s3Folder + "/" + fileName

	metaData := map[string]string{headerFileName: testFileName}

	upload := s3.NewUpload(bytes.NewReader(content), &lenTestFile, filePath, contentType, metaData)

	_, err = client.UploadFile(context.Background(), upload)
	require.NoError(t, err)

	uploaded := &uploaded{
		content:     content,
		lenTestFile: lenTestFile,
		fileName:    fileName,
		filePath:    filePath,
		contentType: contentType,
		metaData:    metaData,
	}

	return uploaded
}
