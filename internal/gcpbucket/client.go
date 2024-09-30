package gcpbucket

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/bennyscetbun/test_horizon/internal/gcp"
	"google.golang.org/api/option"
)

func NewClient(ctx context.Context) (*storage.Client, error) {
	return storage.NewClient(ctx, option.WithCredentialsFile(gcp.CredentialFileName.Value()))
}
