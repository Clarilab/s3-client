package testutils

import (
	"context"
	"errors"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// SeaweedFSImage is the default SeaweedFS container image.
	SeaweedFSImage = "chrislusf/seaweedfs:latest"

	seaweedFSPort = "8333"

	envSeaweedFSAccessKey = "AWS_ACCESS_KEY_ID"
	envSeaweedFSSecretKey = "AWS_SECRET_ACCESS_KEY" //nolint:gosec // environment variable name, not a credential

	defaultSeaweedFSUsername = "seaweedadmin"
	defaultSeaweedFSPassword = "seaweedadmin"
)

// ErrMissingCredentials occurs when the SeaweedFS container is configured without username or password.
var ErrMissingCredentials = errors.New("username or password has not been set")

// SeaweedFSContainer represents a running SeaweedFS container with an S3 gateway.
type SeaweedFSContainer struct {
	testcontainers.Container
	Username string
	Password string
}

// WithSeaweedFSCredentials sets the S3 access key and secret key of the SeaweedFS container.
func WithSeaweedFSCredentials(username, password string) testcontainers.CustomizeRequestOption {
	return testcontainers.WithEnv(map[string]string{
		envSeaweedFSAccessKey: username,
		envSeaweedFSSecretKey: password,
	})
}

// NewSeaweedFSContainer runs a container with a running SeaweedFS instance (`weed mini`).
//
// Notes: Only meant to be used for testing purposes. Host MUST have a docker engine running.
func NewSeaweedFSContainer(
	ctx context.Context,
	image string,
	customizers ...testcontainers.ContainerCustomizer,
) (*SeaweedFSContainer, error) {
	const errMessage = "failed to run seaweedfs container: %w"

	var username, password string

	opts := make([]testcontainers.ContainerCustomizer, 0, len(customizers)+5) //nolint:mnd // module options below

	opts = append(opts,
		testcontainers.WithExposedPorts(seaweedFSPort+"/tcp"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/healthz").WithPort(seaweedFSPort)),
		WithSeaweedFSCredentials(defaultSeaweedFSUsername, defaultSeaweedFSPassword),
		testcontainers.WithCmd("mini", "-dir=/data"),
	)

	opts = append(opts, customizers...)

	opts = append(opts, testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
		username = req.Env[envSeaweedFSAccessKey]
		password = req.Env[envSeaweedFSSecretKey]

		if username == "" || password == "" {
			return ErrMissingCredentials
		}

		return nil
	}))

	ctr, err := testcontainers.Run(ctx, image, opts...)

	var container *SeaweedFSContainer
	if ctr != nil {
		container = &SeaweedFSContainer{
			Container: ctr,
			Username:  username,
			Password:  password,
		}
	}

	if err != nil {
		return container, fmt.Errorf(errMessage, err)
	}

	return container, nil
}

// ConnectionString returns the host and port of the S3 gateway.
func (c *SeaweedFSContainer) ConnectionString(ctx context.Context) (string, error) {
	return c.PortEndpoint(ctx, seaweedFSPort+"/tcp", "")
}
