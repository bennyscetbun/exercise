package uploader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/bennyscetbun/test_horizon/internal/coingeckoprice"
	"github.com/bennyscetbun/test_horizon/internal/gcpbucket"
	"github.com/bennyscetbun/test_horizon/internal/tools"
	"github.com/dogmatiq/ferrite"
	"github.com/ztrue/tracerr"
)

var numberOfWorker = ferrite.Unsigned[uint]("WORKER_NUM", "number of workers").WithDefault(10).Required()
var hideP2PMarketplace = ferrite.Bool("HIDE_P2P_MARKETPLACE", "do not use P2P marketplace transaction").WithDefault(true).Required()

type csvRow struct {
	Columns    []string
	LineNumber int
}

func prepareWorkersAndAggregator(ctx context.Context, workerNumber uint, stopWorkers chan struct{}) (chan<- csvRow, <-chan DataMap) {
	var wg sync.WaitGroup
	workersDone := make(chan struct{})

	recordsChannel := make(chan csvRow)
	resultChannel := make(chan *datum, workerNumber)
	finalResultChannel := make(chan DataMap)
	go func() {
		for i := uint(0); i < workerNumber; i++ {
			wg.Add(1)
			go worker(ctx, &wg, stopWorkers, recordsChannel, resultChannel)
		}
		wg.Wait()
		close(workersDone)
	}()
	go aggregator(ctx, workersDone, resultChannel, finalResultChannel)
	return recordsChannel, finalResultChannel
}

func workerDoOneRecord(ctx context.Context, columns []string) (*datum, error) {
	if len(columns) != 16 {
		return nil, tracerr.Errorf("Bad number of column detected: %d", len(columns))
	}
	t, err := time.Parse(tools.TimeFileLayout, columns[1])
	if err != nil {
		return nil, tracerr.Wrap(err)
	}
	date := t.Format(tools.DateLayout)
	p := &props{}
	if err := json.Unmarshal([]byte(columns[14]), p); err != nil {
		return nil, tracerr.Wrap(err)
	}
	n := &nums{}
	if err := json.Unmarshal([]byte(columns[15]), n); err != nil {
		return nil, tracerr.Wrap(err)
	}
	num, err := strconv.ParseFloat(n.CurrencyValueDecimal, 64)
	if err != nil {
		return nil, tracerr.Wrap(err)
	}
	if hideP2PMarketplace.Value() && p.MarketplaceType == "p2p" {
		return nil, nil
	}
	value, err := coingeckoprice.GetUSDPrice(ctx, p.ChainID, p.CurrencyAddress, date)
	if err != nil {
		return nil, tracerr.Wrap(err)
	}
	return &datum{
		Date:      date,
		ProjectID: columns[3],
		EventType: columns[2],
		VolumeUSD: num * value,
	}, nil
}

func worker(ctx context.Context, wg *sync.WaitGroup, stopWorkers chan struct{}, records <-chan csvRow, results chan<- *datum) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		select {
		case r := <-records:
			d, err := workerDoOneRecord(ctx, r.Columns)
			if err != nil {
				d = &datum{
					lineNumber: r.LineNumber,
					err:        err,
				}
			}
			select {
			case results <- d:
			case <-ctx.Done():
				return
			}
		case <-stopWorkers:
			return
		case <-ctx.Done():
			return
		}
	}
}

func aggregator(ctx context.Context, workersDone chan struct{}, results <-chan *datum, finalReturn chan<- DataMap) {
	dataMap := make(DataMap)
main_loop:
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		select {
		case r := <-results:
			if r == nil {
				continue
			}
			if r.err != nil {
				fmt.Println(r.lineNumber, tracerr.Sprint(r.err))
				continue
			}
			dataMap.Insert(r)
		case <-workersDone:
			break main_loop
		case <-ctx.Done():
			return
		}
	}
	// keep reading until no more data to ingest
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		select {
		case <-ctx.Done():
			return
		case r := <-results:
			if r == nil {
				continue
			}
			if r.err != nil {
				fmt.Println(r.lineNumber, tracerr.Sprint(r.err))
				continue
			}
			dataMap.Insert(r)
		default:
			select {
			case finalReturn <- dataMap:
			case <-ctx.Done():
			}
			return
		}
	}
}

func PrepareDataFromCSVFile(ctx context.Context, csvFile gcpbucket.CSVFile) (*DataMap, error) {
	stopWorkers := make(chan struct{})
	recordChannel, resultChannel := prepareWorkersAndAggregator(ctx, numberOfWorker.Value(), stopWorkers)

	// remove the first line which is the header
	if _, err := csvFile.Read(); err != nil {
		return nil, tracerr.Wrap(err)
	}

	lineNumber := 1
main_loop:
	for {
		select {
		case <-ctx.Done():
			break main_loop
		default:
		}
		columns, err := csvFile.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println(err)
			break
		}
		select {
		case recordChannel <- csvRow{Columns: columns, LineNumber: lineNumber}:
		case <-ctx.Done():
			break main_loop
		}
		lineNumber++
	}
	close(stopWorkers)
	select {
	case finalResult := <-resultChannel:
		return &finalResult, nil
	case <-ctx.Done():
		return nil, nil
	}
}
