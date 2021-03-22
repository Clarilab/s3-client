package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ExtractingFileName(t *testing.T) {
	paths := []string{
		"env/tenant/test/testfile_1",
		"env/tenant/test/testfile_2",
		"env/tenant/testfile_3",
		"testfile_4",
	}

	for i := range paths {
		fileName := extractFilenameFromPath(paths[i])

		assert.NotEmpty(t, fileName)
		assert.Contains(t, fileName, "testfile_")
	}

	fileName := extractFilenameFromPath("")
	assert.Empty(t, fileName)
}
