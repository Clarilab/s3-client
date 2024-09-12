package s3_test

import (
	"bytes"
	"context"
	"embed"
	"io"
	"net"
	"strconv"
	"testing"

	"github.com/Clarilab/s3-client/v3"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/orlangure/gnomock"
	"github.com/stretchr/testify/require"
)

const (
	s3User     = "admin"
	s3Pwd      = "password"
	bucketName = "test-bucket"

	testFile1Name  = "test-file-1.txt"
	headerFileName = "Filename"
)

//go:embed testdata
var testData embed.FS

func Test_GetFile(t *testing.T) {
	s3URL, minioClient := setupTestClients(t, bucketName)

	testFile1, err := testData.ReadFile("testdata" + "/" + testFile1Name)
	require.NoError(t, err)

	lenTestFile1 := int64(len(testFile1))

	fileName := uuid.NewString()
	filePath := "/test/" + fileName

	contentType := "text/plain"

	metaData := map[string]string{headerFileName: testFile1Name}

	uploadTestFile(
		t,
		minioClient,
		filePath,
		bytes.NewReader(testFile1),
		lenTestFile1,
		minio.PutObjectOptions{
			ContentType:  contentType,
			UserMetadata: metaData,
		},
	)

	s3Client, err := s3.NewClient(
		context.Background(),
		s3URL,
		s3User,
		s3Pwd,
		bucketName,
		false,
	)
	require.NoError(t, err)

	t.Run("get file", func(t *testing.T) {
		file, err := s3Client.GetFile(context.Background(), filePath)
		require.NoError(t, err)

		require.Equal(t, testFile1, file.Content)
		require.Equal(t, lenTestFile1, file.Length)
		require.Equal(t, contentType, file.ContentType)
		require.Equal(t, fileName, file.Name)
		require.Equal(t, metaData, file.MetaData)
		require.False(t, file.ModifiedDate.IsZero())
	})

	t.Run("get file with options without options", func(t *testing.T) {
		file, err := s3Client.GetFileWithOptions(context.Background(), filePath)
		require.NoError(t, err)

		require.Equal(t, testFile1, file.Content)
		require.Equal(t, lenTestFile1, file.Length)
		require.Equal(t, contentType, file.ContentType)
		require.Equal(t, fileName, file.Name)
		require.Equal(t, metaData, file.MetaData)
		require.False(t, file.ModifiedDate.IsZero())
	})

	t.Run("get file with content with option", func(t *testing.T) {
		file, err := s3Client.GetFileWithOptions(context.Background(), filePath, s3.WithGetOptionDownloadContent(true))
		require.NoError(t, err)

		require.Equal(t, testFile1, file.Content)
		require.Equal(t, lenTestFile1, file.Length)
		require.Equal(t, contentType, file.ContentType)
		require.Equal(t, fileName, file.Name)
		require.Equal(t, metaData, file.MetaData)
		require.False(t, file.ModifiedDate.IsZero())
	})

	t.Run("get file without content", func(t *testing.T) {
		file, err := s3Client.GetFileWithOptions(context.Background(), filePath, s3.WithGetOptionDownloadContent(false))
		require.NoError(t, err)

		require.Nil(t, file.Content)
		require.Equal(t, lenTestFile1, file.Length)
		require.Equal(t, contentType, file.ContentType)
		require.Equal(t, fileName, file.Name)
		require.Equal(t, metaData, file.MetaData)
		require.False(t, file.ModifiedDate.IsZero())
	})
}

func uploadTestFile(t *testing.T, client *minio.Client, path string, content io.Reader, size int64, opt minio.PutObjectOptions) {
	t.Helper()

	_, err := client.PutObject(context.Background(), bucketName, path, content, size, opt)
	require.NoError(t, err)
}

func setupTestClients(t *testing.T, bucketName string) (string, *minio.Client) {
	t.Helper()

	const (
		imageTag          = "quay.io/minio/minio:latest"
		minioPort         = 9000
		envKeyRootUser    = "MINIO_ROOT_USER="
		envKeyRootUserPwd = "MINIO_ROOT_PASSWORD="
		command           = "server"
		commandArg        = "/data"
	)

	container, err := gnomock.StartCustom(
		imageTag,
		gnomock.DefaultTCP(minioPort),
		gnomock.WithUseLocalImagesFirst(),
		gnomock.WithEnv(envKeyRootUser+s3User),
		gnomock.WithEnv(envKeyRootUserPwd+s3Pwd),
		gnomock.WithCommand(command, commandArg),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := gnomock.Stop(container)
		require.NoError(t, err)
	})

	minioClient, err := minio.New(net.JoinHostPort(container.Host, strconv.Itoa(container.DefaultPort())), &minio.Options{
		Secure: false,
		Creds:  credentials.NewStaticV4(s3User, s3Pwd, ""),
	})
	require.NoError(t, err)

	err = minioClient.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{})
	require.NoError(t, err)

	return net.JoinHostPort(container.Host, strconv.Itoa(container.DefaultPort())), minioClient
}
