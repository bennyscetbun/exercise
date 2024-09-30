package bigquery

import (
	"context"
	"net/http"

	"cloud.google.com/go/bigquery"
	"github.com/ztrue/tracerr"
	"google.golang.org/api/googleapi"
)

func (c *Client) InitTables(ctx context.Context) error {
	schema, err := bigquery.InferSchema(TransactionsPerDay{})
	if err != nil {
		return tracerr.Wrap(err)
	}
	c.transactionsPerDaysTable = c.dataset.Table("transactions_per_days")
	if err := c.transactionsPerDaysTable.Create(ctx, &bigquery.TableMetadata{Schema: schema,
		TableConstraints: &bigquery.TableConstraints{
			PrimaryKey: &bigquery.PrimaryKey{
				Columns: []string{"date", "project_id", "event_type"},
			},
		}}); err != nil {
		switch e := err.(type) {
		case *googleapi.Error:
			if e.Code != http.StatusConflict {
				return tracerr.Wrap(err)
			} else {
				// TODO: add some migration system
				if _, err := c.transactionsPerDaysTable.Update(ctx, bigquery.TableMetadataToUpdate{Schema: schema}, ""); err != nil {
					return tracerr.Wrap(err)
				}
			}
		default:
			return tracerr.Wrap(err)
		}
	}
	return nil
}
