package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/bennyscetbun/test_horizon/internal/gcp"
	"github.com/dogmatiq/ferrite"
	"github.com/ztrue/tracerr"
	"google.golang.org/api/option"
)

var datasetName = ferrite.String("BIGQUERY_DATASET_NAME", "name of the bigquery dataset").Required()

type Client struct {
	client  *bigquery.Client
	dataset *bigquery.Dataset

	transactionsPerDaysTable *bigquery.Table
}

func (c *Client) Close() error {
	return c.client.Close()
}

func NewClientWithDataset(ctx context.Context) (*Client, error) {
	client, err := bigquery.NewClient(ctx, gcp.ProjectID.Value(), option.WithCredentialsFile(gcp.CredentialFileName.Value()))
	if err != nil {
		return nil, tracerr.Wrap(err)
	}
	dataset := client.DatasetInProject(gcp.ProjectID.Value(), datasetName.Value())
	// early error check if the dataset exist
	if _, err := dataset.Metadata(ctx); err != nil {
		return nil, tracerr.Wrap(err)
	}
	return &Client{
		client:  client,
		dataset: dataset,
	}, nil
}
