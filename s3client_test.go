package s3_test //nolint:revive // package name matches folder name

import (
	"bytes"
	"context"
	"embed"
	"io"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Clarilab/s3-client/v4"
	"github.com/Clarilab/s3-client/v4/testutils"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

const (
	bucketName = "test-bucket"

	testFile1Name = "test-file-1.txt"
	testFile2Name = "test-file-2.txt"

	headerFileName = "Filename"

	contentType    = "text/plain"
	testDataFolder = "testdata"
)

//go:embed testdata
var testData embed.FS

var (
	s3URL       string
	s3User      string
	s3Pwd       string
	minioClient *minio.Client
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	env, err := setupTestEnvironment(ctx, bucketName)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := env.container.Terminate(ctx); err != nil {
			panic(err)
		}
	}()

	s3URL = env.s3URL
	s3User = env.s3User
	s3Pwd = env.s3Pwd
	minioClient = env.minioClient

	m.Run()
}

func Test_Client(t *testing.T) {
	t.Parallel()

	t.Run("new client", func(t *testing.T) {
		t.Parallel()

		details := &s3.ClientDetails{
			Host:         s3URL,
			AccessKey:    s3User,
			AccessSecret: s3Pwd,
			BucketName:   bucketName,
			Secure:       false,
		}

		client, err := s3.NewClient(details)
		require.NoError(t, err)

		client.Close()
	})

	t.Run("empty host", func(t *testing.T) {
		t.Parallel()

		details := &s3.ClientDetails{
			Host:         "",
			AccessKey:    s3User,
			AccessSecret: s3Pwd,
			BucketName:   bucketName,
			Secure:       false,
		}

		_, err := s3.NewClient(details)
		require.ErrorIs(t, err, s3.ErrEmptyHost)
	})

	t.Run("empty access key", func(t *testing.T) {
		t.Parallel()

		details := &s3.ClientDetails{
			Host:         s3URL,
			AccessKey:    "",
			AccessSecret: s3Pwd,
			BucketName:   bucketName,
			Secure:       false,
		}

		_, err := s3.NewClient(details)
		require.ErrorIs(t, err, s3.ErrEmptyAccessKey)
	})

	t.Run("empty access secret", func(t *testing.T) {
		t.Parallel()

		details := &s3.ClientDetails{
			Host:         s3URL,
			AccessKey:    s3User,
			AccessSecret: "",
			BucketName:   bucketName,
			Secure:       false,
		}

		_, err := s3.NewClient(details)
		require.ErrorIs(t, err, s3.ErrEmptyAccessSecret)
	})

	t.Run("empty bucket name", func(t *testing.T) {
		t.Parallel()

		details := &s3.ClientDetails{
			Host:         s3URL,
			AccessKey:    s3User,
			AccessSecret: s3Pwd,
			BucketName:   "",
			Secure:       false,
		}

		_, err := s3.NewClient(details)
		require.ErrorIs(t, err, s3.ErrEmptyBucketName)
	})

	t.Run("is online", func(t *testing.T) {
		t.Parallel()

		details := &s3.ClientDetails{
			Host:         s3URL,
			AccessKey:    s3User,
			AccessSecret: s3Pwd,
			BucketName:   bucketName,
			Secure:       false,
		}

		client, err := s3.NewClient(details, s3.WithHealthCheck(time.Second))
		require.NoError(t, err)

		require.True(t, client.IsOnline())

		client.Close()
	})
}

func Test_File(t *testing.T) {
	t.Parallel()

	const folder = "test-file"

	s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false))

	uploaded := uploadTestFile(t, folder, testFile1Name)

	t.Run("file bytes", func(t *testing.T) {
		file, err := s3Client.GetFile(context.Background(), uploaded.filePath)
		if err != nil {
			t.Fatal(err)
		}

		result, err := file.Bytes()
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(uploaded.content, result) {
			t.Fatal("wrong file content")
		}
	})
}

func Test_UploadFile(t *testing.T) {
	t.Parallel()

	s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false))

	testFile, err := testData.ReadFile("testdata" + "/" + testFile1Name)
	require.NoError(t, err)

	lenTestFile := int64(len(testFile))
	contentType := "text/plain"
	folder := "test-upload-file"

	t.Run("upload file", func(t *testing.T) {
		t.Parallel()

		fileName := uuid.NewString()
		filePath := folder + "/" + fileName

		metaData := map[string]string{headerFileName: testFile1Name}

		upload := s3.NewUpload(bytes.NewReader(testFile), &lenTestFile, filePath, contentType, metaData)

		info, err := s3Client.UploadFile(context.Background(), upload)
		require.NoError(t, err)

		require.Equal(t, lenTestFile, info.Size)
	})

	t.Run("upload file with options", func(t *testing.T) {
		t.Parallel()

		fileName := uuid.NewString()
		filePath := folder + "/" + fileName

		metaData := map[string]string{headerFileName: testFile1Name}

		upload := s3.NewUpload(bytes.NewReader(testFile), &lenTestFile, filePath, contentType, metaData)

		info, err := s3Client.UploadFile(context.Background(), upload, s3.WithClientUploadOptions(s3.ClientUploadOptions{}))
		require.NoError(t, err)

		require.Equal(t, lenTestFile, info.Size)
	})
}

func Test_GetFile(t *testing.T) {
	t.Parallel()

	const folder = "test-get-file"

	s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false))

	uploaded := uploadTestFile(t, folder, testFile1Name)

	t.Run("get file", func(t *testing.T) {
		t.Parallel()

		file, err := s3Client.GetFile(context.Background(), uploaded.filePath)
		require.NoError(t, err)

		fileContent, err := file.Bytes()
		require.NoError(t, err)

		fileInfo := file.Info()

		require.Equal(t, uploaded.content, fileContent)
		require.Equal(t, uploaded.lenTestFile, fileInfo.Size)
		require.Equal(t, uploaded.contentType, fileInfo.ContentType)
		require.Equal(t, uploaded.fileName, fileInfo.Name)
		require.Equal(t, uploaded.metaData, fileInfo.MetaData)
		require.False(t, fileInfo.ModifiedDate.IsZero())
	})

	t.Run("get file with options", func(t *testing.T) {
		t.Parallel()

		file, err := s3Client.GetFile(context.Background(), uploaded.filePath, s3.WithClientGetOptions(s3.ClientGetOptions{}))
		require.NoError(t, err)

		fileContent, err := file.Bytes()
		require.NoError(t, err)

		fileInfo := file.Info()

		require.Equal(t, uploaded.content, fileContent)
		require.Equal(t, uploaded.lenTestFile, fileInfo.Size)
		require.Equal(t, uploaded.contentType, fileInfo.ContentType)
		require.Equal(t, uploaded.fileName, fileInfo.Name)
		require.Equal(t, uploaded.metaData, fileInfo.MetaData)
		require.False(t, fileInfo.ModifiedDate.IsZero())
	})
}

func Test_GetFileInfo(t *testing.T) {
	t.Parallel()

	const folder = "test-get-file-info"

	s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false))

	uploaded := uploadTestFile(t, folder, testFile1Name)

	fileInfo, err := s3Client.GetFileInfo(context.Background(), uploaded.filePath)
	require.NoError(t, err)

	require.Equal(t, uploaded.lenTestFile, fileInfo.Size)
	require.Equal(t, uploaded.contentType, fileInfo.ContentType)
	require.Equal(t, uploaded.fileName, fileInfo.Name)
	require.Equal(t, uploaded.metaData, fileInfo.MetaData)
	require.False(t, fileInfo.ModifiedDate.IsZero())
}

func Test_GetDirectory(t *testing.T) {
	t.Parallel()

	const folder = "test-get-directory"

	uploaded1 := uploadTestFile(t, folder, testFile1Name)
	uploaded2 := uploadTestFile(t, folder, testFile2Name)

	s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false))

	t.Run("get directory", func(t *testing.T) {
		t.Parallel()

		files, err := s3Client.GetDirectory(context.Background(), folder)
		require.NoError(t, err)

		require.Len(t, files, 2)

		for i := range files {
			file := files[i]

			fileContent, err := file.Bytes()
			require.NoError(t, err)

			fileInfo := file.Info()

			require.False(t, fileInfo.ModifiedDate.IsZero())

			if fileInfo.Name == uploaded1.fileName {
				require.Equal(t, uploaded1.content, fileContent)
				require.Equal(t, uploaded1.lenTestFile, fileInfo.Size)
				require.Equal(t, uploaded1.contentType, fileInfo.ContentType)
				require.Equal(t, uploaded1.metaData, fileInfo.MetaData)
			}

			if fileInfo.Name == uploaded2.fileName {
				require.Equal(t, uploaded2.content, fileContent)
				require.Equal(t, uploaded2.lenTestFile, fileInfo.Size)
				require.Equal(t, uploaded2.contentType, fileInfo.ContentType)
				require.Equal(t, uploaded2.metaData, fileInfo.MetaData)
			}
		}
	})

	t.Run("get directory with options", func(t *testing.T) {
		t.Parallel()

		files, err := s3Client.GetDirectory(context.Background(), folder, s3.WithGetDirectoryClientGetOptions(s3.ClientGetOptions{}))
		require.NoError(t, err)

		require.Len(t, files, 2)

		for i := range files {
			file := files[i]

			fileContent, err := file.Bytes()
			require.NoError(t, err)

			fileInfo := file.Info()

			require.False(t, fileInfo.ModifiedDate.IsZero())

			if fileInfo.Name == uploaded1.fileName {
				require.Equal(t, uploaded1.content, fileContent)
				require.Equal(t, uploaded1.lenTestFile, fileInfo.Size)
				require.Equal(t, uploaded1.contentType, fileInfo.ContentType)
				require.Equal(t, uploaded1.metaData, fileInfo.MetaData)
			}

			if fileInfo.Name == uploaded2.fileName {
				require.Equal(t, uploaded2.content, fileContent)
				require.Equal(t, uploaded2.lenTestFile, fileInfo.Size)
				require.Equal(t, uploaded2.contentType, fileInfo.ContentType)
				require.Equal(t, uploaded2.metaData, fileInfo.MetaData)
			}
		}
	})
}

func Test_GetDirectoryInfos(t *testing.T) {
	t.Parallel()

	const folder = "-infos"

	s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false))

	uploaded1 := uploadTestFile(t, folder, testFile1Name)
	uploaded2 := uploadTestFile(t, folder, testFile2Name)

	fileInfos, err := s3Client.GetDirectoryInfos(context.Background(), folder)
	require.NoError(t, err)

	require.Len(t, fileInfos, 2)

	for i := range fileInfos {
		fileInfo := fileInfos[i]

		require.False(t, fileInfo.ModifiedDate.IsZero())

		if fileInfo.Name == uploaded1.fileName {
			require.Equal(t, uploaded1.lenTestFile, fileInfo.Size)
			require.Equal(t, uploaded1.contentType, fileInfo.ContentType)
			require.Equal(t, uploaded1.metaData, fileInfo.MetaData)
		}

		if fileInfo.Name == uploaded2.fileName {
			require.Equal(t, uploaded2.lenTestFile, fileInfo.Size)
			require.Equal(t, uploaded2.contentType, fileInfo.ContentType)
			require.Equal(t, uploaded2.metaData, fileInfo.MetaData)
		}
	}
}

func Test_DownloadFile(t *testing.T) {
	t.Parallel()

	const (
		folder      = "test-download-file"
		localFolder = "testdata/temp-download-file"
	)

	s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false))

	t.Cleanup(func() {
		err := os.RemoveAll(localFolder)
		require.NoError(t, err)
	})

	t.Run("download file", func(t *testing.T) {
		t.Parallel()

		uploaded := uploadTestFile(t, folder, testFile1Name)

		localPath := localFolder + "/" + uploaded.fileName

		err := s3Client.DownloadFile(context.Background(), uploaded.filePath, localPath)
		require.NoError(t, err)

		fileBytes, err := os.ReadFile(localPath)
		require.NoError(t, err)

		require.Equal(t, uploaded.content, fileBytes)
	})

	t.Run("download file with options", func(t *testing.T) {
		t.Parallel()

		const folder = "test-download-file"

		uploaded := uploadTestFile(t, folder, testFile1Name)

		localPath := localFolder + "/" + uploaded.fileName

		err := s3Client.DownloadFile(context.Background(), uploaded.filePath, localPath, s3.WithClientDownloadOptions(s3.ClientGetOptions{}))
		require.NoError(t, err)

		fileBytes, err := os.ReadFile(localPath)
		require.NoError(t, err)

		require.Equal(t, uploaded.content, fileBytes)
	})
}

func Test_DownloadDirectory(t *testing.T) {
	t.Parallel()

	const (
		folder      = "test-download-directory"
		localFolder = "testdata/temp-download-directory"
	)

	s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false))

	t.Cleanup(func() {
		err := os.RemoveAll(localFolder)
		require.NoError(t, err)
	})

	t.Run("download file", func(t *testing.T) {
		t.Parallel()

		uploaded1 := uploadTestFile(t, folder, testFile1Name)
		uploaded2 := uploadTestFile(t, folder, testFile2Name)

		err := s3Client.DownloadDirectory(context.Background(), folder, localFolder, true)
		require.NoError(t, err)

		entries, err := os.ReadDir(localFolder)
		require.NoError(t, err)

		for i := range entries {
			entry := entries[i]

			fileName := entry.Name()

			fileBytes, err := os.ReadFile(localFolder + "/" + entry.Name())
			require.NoError(t, err)

			if fileName == uploaded1.fileName {
				require.Equal(t, uploaded1.content, fileBytes)
			}

			if fileName == uploaded2.fileName {
				require.Equal(t, uploaded2.content, fileBytes)
			}
		}
	})
}

func Test_RemoveFile(t *testing.T) {
	t.Parallel()

	const folder = "test-remove-file"

	s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false))

	t.Run("remove file", func(t *testing.T) {
		t.Parallel()

		uploaded := uploadTestFile(t, folder, testFile1Name)

		err := s3Client.RemoveFile(context.Background(), uploaded.filePath)
		require.NoError(t, err)

		info, err := minioClient.GetObjectACL(context.Background(), bucketName, uploaded.filePath)
		require.Error(t, err)
		require.Nil(t, info)
	})

	t.Run("remove file with options", func(t *testing.T) {
		t.Parallel()

		uploaded := uploadTestFile(t, folder, testFile1Name)

		err := s3Client.RemoveFile(context.Background(), uploaded.filePath, s3.WithClientRemoveOptions(s3.ClientRemoveOptions{}))
		require.NoError(t, err)

		info, err := minioClient.GetObjectACL(context.Background(), bucketName, uploaded.filePath)
		require.Error(t, err)
		require.Nil(t, info)
	})
}

func Test_CreateFileLink(t *testing.T) {
	t.Parallel()

	const (
		expiration = time.Second * 2
		folder     = "test-create-file-link"
	)

	s3Client := getS3Client(t, s3.WithCRC32CIntegritySupport(false))

	uploaded := uploadTestFile(t, folder, testFile1Name)

	link, err := s3Client.CreateFileLink(context.Background(), uploaded.filePath, expiration)
	require.NoError(t, err)

	require.Equal(t, "/"+bucketName+"/"+uploaded.filePath, link.Path)

	req, err := http.NewRequest(http.MethodGet, link.String(), nil)
	require.NoError(t, err)

	httpClient := &http.Client{}

	resp, err := httpClient.Do(req)
	require.NoError(t, err)

	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	err = resp.Body.Close()
	require.NoError(t, err)

	require.Equal(t, uploaded.content, bodyBytes)

	time.Sleep(expiration + time.Second)

	resp, err = httpClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

type uploaded struct {
	content     []byte
	lenTestFile int64
	fileName    string
	filePath    string
	contentType string
	metaData    map[string]string
}

func uploadTestFile(t *testing.T, s3Folder, testFileName string) *uploaded {
	t.Helper()

	content, err := testData.ReadFile(testDataFolder + "/" + testFileName)
	require.NoError(t, err)

	lenTestFile := int64(len(content))

	fileName := uuid.NewString()

	filePath := s3Folder + "/" + fileName

	metaData := map[string]string{headerFileName: testFileName}

	opt := minio.PutObjectOptions{
		ContentType:  contentType,
		UserMetadata: metaData,
	}

	_, err = minioClient.PutObject(context.Background(), bucketName, filePath, bytes.NewReader(content), lenTestFile, opt)
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

type testEnvironment struct {
	s3URL       string
	s3User      string
	s3Pwd       string
	container   testcontainers.Container
	minioClient *minio.Client
}

func setupTestEnvironment(ctx context.Context, bucketName string) (*testEnvironment, error) {
	container, err := testutils.NewContainer(ctx, testutils.DefaultImage)
	if err != nil {
		return nil, err
	}

	cs, err := container.ConnectionString(ctx)
	if err != nil {
		return nil, err
	}

	minioClient, err := minio.New(cs, &minio.Options{
		Secure: false,
		Creds:  credentials.NewStaticV4(container.Username, container.Password, ""),
	})
	if err != nil {
		return nil, err
	}

	err = minioClient.MakeBucket(context.Background(), bucketName, minio.MakeBucketOptions{})
	if err != nil {
		return nil, err
	}

	result := testEnvironment{
		s3URL:       cs,
		s3User:      container.Username,
		s3Pwd:       container.Password,
		container:   container,
		minioClient: minioClient,
	}

	return &result, nil
}

func getS3Client(t *testing.T, options ...s3.ClientOption) s3.Client {
	t.Helper()

	clientDetails := &s3.ClientDetails{
		Host:         s3URL,
		AccessKey:    s3User,
		AccessSecret: s3Pwd,
		BucketName:   bucketName,
		Secure:       false,
	}

	s3Client, err := s3.NewClient(clientDetails, options...)
	require.NoError(t, err)

	t.Cleanup(s3Client.Close)

	return s3Client
}
