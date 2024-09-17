package s3

import (
	"crypto/md5" //nolint:gosec // intended to use MD5 for hashing
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	keyCR32CChecksum = "Checksum-Cr32c"
	keyMD5Checksum   = "Checksum-Md5"
)

type integritySettings struct {
	useIntegrityCRC32C bool
	useIntegrityMD5    bool
}

// Integrity contains checksums for file integrity.
type Integrity struct {
	ChecksumCRC32C string // When CRC32C integrity support is disabled, ChecksumCRC32C will be empty if no explicit integrity check was requested via option
	ChecksumMD5    string // When MD5 integrity support is disabled, ChecksumMD5 will be empty if no explicit integrity check was requested via option
}

// GenerateCheckSumCRC32C returns a CRC32C checksum of the given data.
func GenerateCheckSumCRC32C(data io.Reader) (string, error) {
	const errMessage = "failed to get CRC32C checksum: %w"

	hash := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	if _, err := io.Copy(hash, data); err != nil {
		return "", fmt.Errorf(errMessage, err)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// GenerateCheckSumMD5 returns a MD5 checksum of the given data.
func GenerateCheckSumMD5(data io.Reader) (string, error) {
	const errMessage = "failed to get MD5 checksum: %w"

	hash := md5.New() //nolint:gosec // intended to use MD5 for hashing

	if _, err := io.Copy(hash, data); err != nil {
		return "", fmt.Errorf(errMessage, err)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

type checksum string

func (c checksum) compareChecksum(expected string) error {
	if expected != "" && expected != c.hex() {
		return ErrChecksumMismatch
	}

	return nil
}

func (c checksum) hex() string {
	return string(c)
}

func getCheckSumCRC32C(obj io.ReadSeeker) (checksum, error) {
	const errMessage = "failed to get checksum: %w"

	startPos, err := obj.Seek(0, io.SeekStart)
	if err != nil {
		return "", fmt.Errorf(errMessage, err)
	}

	sum, err := GenerateCheckSumCRC32C(obj)
	if err != nil {
		return "", fmt.Errorf(errMessage, err)
	}

	if _, err := obj.Seek(startPos, io.SeekStart); err != nil {
		return "", fmt.Errorf(errMessage, err)
	}

	return checksum(sum), nil
}

func getCheckSumMD5(obj io.ReadSeeker) (checksum, error) {
	const errMessage = "failed to get checksum: %w"

	startPos, err := obj.Seek(0, io.SeekStart)
	if err != nil {
		return "", fmt.Errorf(errMessage, err)
	}

	sum, err := GenerateCheckSumMD5(obj)
	if err != nil {
		return "", fmt.Errorf(errMessage, err)
	}

	if _, err := obj.Seek(startPos, io.SeekStart); err != nil {
		return "", fmt.Errorf(errMessage, err)
	}

	return checksum(sum), nil
}

func (c *client) handleIntegrity(obj io.ReadSeeker, info *FileInfo, getOptions *getOptions) error {
	const errMessage = "failed to handle integrity: %w"

	params := &handleIntegrityParams{
		content:    obj,
		info:       info,
		getOptions: getOptions,
		crc32c:     checksum(info.MetaData[keyCR32CChecksum]),
		md5:        checksum(info.MetaData[keyMD5Checksum]),
	}

	if info.MetaData != nil {
		delete(info.MetaData, keyCR32CChecksum)
		delete(info.MetaData, keyMD5Checksum)
	}

	if err := c.handleGetFileIntegritySettings(params); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	if getOptions == nil {
		return nil // No integrity check options provided, skip integrity options handling
	}

	if err := handleGetFileIntegrityOptions(params); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

type handleIntegrityParams struct {
	content    io.ReadSeeker
	getOptions *getOptions
	info       *FileInfo
	crc32c     checksum
	md5        checksum
}

func (c *client) handleGetFileIntegritySettings(params *handleIntegrityParams) error {
	const errMessage = "failed to handle integrity settings: %w"

	var err error

	if c.useIntegrityCRC32C {
		if params.crc32c == "" && params.content != nil {
			params.crc32c, err = getCheckSumCRC32C(params.content)
			if err != nil {
				return fmt.Errorf(errMessage, err)
			}
		}

		params.info.ChecksumCRC32C = params.crc32c.hex()
	}

	if c.useIntegrityMD5 {
		if params.md5 == "" && params.content != nil {
			params.md5, err = getCheckSumMD5(params.content)
			if err != nil {
				return fmt.Errorf(errMessage, err)
			}
		}

		params.info.ChecksumMD5 = params.md5.hex()
	}

	return nil
}

func handleGetFileIntegrityOptions(params *handleIntegrityParams) error {
	const errMessage = "failed to handle integrity options: %w"

	var err error

	if params.getOptions.ChecksumCRC32C != "" {
		err = handleGetFileIntegrityCheckOptionsCRC32C(params)
	}

	if params.getOptions.ChecksumMD5 != "" {
		err = handleGetFileIntegrityCheckOptionsMD5(params)
	}

	if err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

func handleGetFileIntegrityCheckOptionsCRC32C(params *handleIntegrityParams) error {
	const errMessage = "failed to handle cr32c integrity check options: %w"

	var err error

	if params.crc32c == "" {
		params.crc32c, err = getCheckSumCRC32C(params.content)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	if err := params.crc32c.compareChecksum(params.getOptions.ChecksumCRC32C); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	params.info.ChecksumCRC32C = params.crc32c.hex()

	return nil
}

func handleGetFileIntegrityCheckOptionsMD5(params *handleIntegrityParams) error {
	const errMessage = "failed to handle md5 integrity options: %w"

	var err error

	if params.md5 == "" {
		params.md5, err = getCheckSumMD5(params.content)
		if err != nil {
			return fmt.Errorf(errMessage, err)
		}
	}

	if err := params.md5.compareChecksum(params.getOptions.ChecksumMD5); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	params.info.ChecksumMD5 = params.md5.hex()

	return nil
}

func (c *client) handleGetFileInfoIntegrity(info *FileInfo) {
	if c.useIntegrityCRC32C {
		info.ChecksumCRC32C = info.MetaData[keyCR32CChecksum]
	}

	if c.useIntegrityMD5 {
		info.ChecksumMD5 = info.MetaData[keyMD5Checksum]
	}

	if info.MetaData != nil {
		delete(info.MetaData, keyCR32CChecksum)
		delete(info.MetaData, keyMD5Checksum)
	}
}
