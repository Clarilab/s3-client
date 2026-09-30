package testutils

import (
	"github.com/Clarilab/s3-client/v4"
)

type containerOptions struct {
	image     string
	s3Options []s3.ClientOption
	username  string
	password  string
}

type Option func(*containerOptions)

// WithImage sets the container image to use.
func WithImage(image string) Option {
	return func(o *containerOptions) {
		o.image = image
	}
}

// WithS3Options adds s3.ClientOption to be used when creating the s3.Client.
func WithS3Options(opts ...s3.ClientOption) Option {
	return func(o *containerOptions) {
		o.s3Options = append(o.s3Options, opts...)
	}
}

// WithAuthentication configures the container to use authentication with the given username and password.
func WithAuthentication(username, password string) Option {
	return func(o *containerOptions) {
		o.username = username
		o.password = password
	}
}
