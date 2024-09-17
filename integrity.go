package s3

// Integrity contains checksums for file integrity.
type Integrity struct {
	ChecksumCRC32C string // When CRC32C integrity support is disabled, ChecksumCRC32C will be empty if no explicit integrity check was requested via option
	ChecksumMD5    string // When MD5 integrity support is disabled, ChecksumMD5 will be empty if no explicit integrity check was requested via option
}
