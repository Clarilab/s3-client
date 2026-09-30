package s3

// ClientDetails is a struct for all required connection details.
type ClientDetails struct {
	Host         string
	AccessKey    string
	AccessSecret string
	BucketName   string
	Secure       bool
}

func (d *ClientDetails) validate() error {
	switch {
	case d.Host == "":
		return ErrEmptyHost
	case d.AccessKey == "":
		return ErrEmptyAccessKey
	case d.AccessSecret == "":
		return ErrEmptyAccessSecret
	case d.BucketName == "":
		return ErrEmptyBucketName
	default:
		return nil
	}
}
