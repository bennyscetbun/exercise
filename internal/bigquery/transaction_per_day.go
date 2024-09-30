package bigquery

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/bennyscetbun/test_horizon/internal/uploader"
	"github.com/ztrue/tracerr"
)

type TransactionsPerDay struct {
	Date      string `bigquery:"date"`
	ProjectID string `bigquery:"project_id"`
	EventType string `bigquery:"event_type"`

	NumberOfTransaction int64   `bigquery:"number_of_transactions"`
	VolumeUSD           float64 `bigquery:"volume_usd"`
}

func dataMapToArray(datamap *uploader.DataMap) []*TransactionsPerDay {
	var ret []*TransactionsPerDay
	for day, dayMap := range *datamap {
		for projectID, projectMap := range dayMap {
			for eventType, eventDatum := range projectMap {
				ret = append(ret, &TransactionsPerDay{
					Date:                day,
					ProjectID:           projectID,
					EventType:           eventType,
					NumberOfTransaction: eventDatum.NumberOfTransaction,
					VolumeUSD:           eventDatum.VolumeUSD,
				})
			}
		}
	}
	return ret
}

func (c *Client) InsertTransactionsPerDayFromDataMap(ctx context.Context, datamap *uploader.DataMap) error {
	inserter := c.transactionsPerDaysTable.Inserter()
	inserter.TableTemplateSuffix = strconv.FormatInt(time.Now().Unix(), 10)
	if err := inserter.Put(ctx, dataMapToArray(datamap)); err != nil {
		return tracerr.Wrap(err)
	}

	q := c.client.Query(fmt.Sprintf("MERGE `%s.transactions_per_days` d USING `%s.transactions_per_days%s` s "+
		`ON d.date = s.date AND d.project_id = s.project_id and d.event_type = s.event_type
	WHEN NOT MATCHED THEN INSERT (date, project_id, event_type, number_of_transactions, volume_usd) VALUES(date, project_id, event_type, number_of_transactions, volume_usd)
	WHEN MATCHED THEN UPDATE SET d.number_of_transactions = s.number_of_transactions, d.volume_usd = s.volume_usd;
	`+"DROP TABLE `%s.transactions_per_days%s`", c.dataset.DatasetID, c.dataset.DatasetID, inserter.TableTemplateSuffix, c.dataset.DatasetID, inserter.TableTemplateSuffix))

	if _, err := q.Read(ctx); err != nil {
		return tracerr.Wrap(err)
	}
	return nil
}
