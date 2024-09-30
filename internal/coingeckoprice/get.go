package coingeckoprice

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/JulianToledano/goingecko"
	"github.com/JulianToledano/goingecko/types"
	"github.com/bennyscetbun/test_horizon/internal/gcpbucket"
	"github.com/bennyscetbun/test_horizon/internal/tools"
	"github.com/dogmatiq/ferrite"
	"github.com/ztrue/tracerr"
)

var forcePriceUpdate = ferrite.Bool("COINGECKO_FORCE_PRICE_UPDATE", "do not use gcp files for caching, but update them").WithDefault(false).Required()

var gcpCachePath = ferrite.String("COINGECKO_GCP_CACHE_PATH", "path for gcp cache files").WithDefault("coingecko_cache").Required()

var rateLimitTime = ferrite.Duration("COINGECKO_RATE_LIMIT_TIME", "rate limit to COINGECKO_RATE_LIMIT_COUNT request per COINGECKO_RATE_LIMIT_TIME").WithDefault(time.Minute).Required()
var rateLimitCount = ferrite.Unsigned[uint]("COINGECKO_RATE_LIMIT_COUNT", "rate limit to COINGECKO_RATE_LIMIT_COUNT request per COINGECKO_RATE_LIMIT_TIME").WithDefault(60).Required()

var apiKey = ferrite.String("COINGECKO_KEY", "the coingecko api key").Required()

type mapKey struct {
	date            string
	chainID         string
	currencyAddress string
}

type chainData struct {
	id           string
	nativeCoinID string
}

var chainIDToName map[string]*chainData
var priceCache = make(map[mapKey]float64)
var priceCacheMut sync.Mutex

var httpClient = tools.NewRateLimitedHttpClient(rateLimitTime.Value(), int(rateLimitCount.Value()))

func getChainData(chainID string) (*chainData, error) {
	if chainIDToName == nil {
		cgClient := goingecko.NewClient(httpClient, apiKey.Value())
		defer cgClient.Close()
		assetPlatforms, err := cgClient.AssetPlatforms("nft")
		if err != nil {
			return nil, tracerr.Wrap(err)
		}
		chainIDToName = make(map[string]*chainData)
		for _, a := range *assetPlatforms {
			chainIDToName[strconv.FormatInt(a.ChainIdentifier, 10)] = &chainData{
				id:           a.ID,
				nativeCoinID: a.NativeCoinId,
			}
		}
	}
	cd, ok := chainIDToName[chainID]
	if !ok {
		return nil, tracerr.New(fmt.Sprintln(chainID, "chainid not found"))
	}
	return cd, nil
}

func retrieveUSDPriceFromGeckoApi(ctx context.Context, date, chainID, currencyAddress string, client *storage.Client) (float64, error) {
	begin, err := time.Parse(tools.DateLayout, date)
	if err != nil {
		return 0, tracerr.Wrap(err)
	}
	end := begin.AddDate(0, 0, 1)
	chainData, err := getChainData(chainID)
	if err != nil {
		return 0, tracerr.Wrap(err)
	}
	cgClient := goingecko.NewClient(httpClient, apiKey.Value())
	defer cgClient.Close()
	var marketChart *types.MarketChart
	if tools.IsNullAddress(currencyAddress) {
		marketChart, err = cgClient.CoinsIdMarketChartRange(chainData.nativeCoinID, "usd", strconv.FormatInt(begin.Unix(), 10), strconv.FormatInt(end.Unix(), 10))
	} else {
		marketChart, err = cgClient.ContractMarketChartRange(chainData.id, currencyAddress, "usd", strconv.FormatInt(begin.Unix(), 10), strconv.FormatInt(end.Unix(), 10))
	}
	if err != nil {
		return 0, tracerr.Wrap(err)
	}
	total := 0.0
	for _, m := range marketChart.Prices {
		total += m[1]
	}
	value := total / float64(len(marketChart.Prices))
	if err := gcpbucket.WriteToFile(ctx, &value, getObjectName(date, chainID, currencyAddress), client); err != nil {
		tracerr.Print(err)
	}
	return value, nil
}

func getObjectName(date, chainID, currencyAddress string) string {
	gcpPath := gcpCachePath.Value()
	if gcpPath == "" {
		return fmt.Sprintf("%s_%s_%s.cache", chainID, currencyAddress, date)
	} else {
		return fmt.Sprintf("%s/%s_%s_%s.cache", gcpPath, chainID, currencyAddress, date)
	}
}

func retrieveUSDPrice(ctx context.Context, date, chainID, currencyAddress string) (float64, error) {
	client, err := gcpbucket.NewClient(ctx)
	if err != nil {
		return 0, tracerr.Wrap(err)
	}
	defer client.Close()
	if forcePriceUpdate.Value() {
		return retrieveUSDPriceFromGeckoApi(ctx, date, chainID, currencyAddress, client)
	}

	objectName := getObjectName(date, chainID, currencyAddress)
	file, err := gcpbucket.GetFile(ctx, objectName, client)
	if tracerr.Unwrap(err) == storage.ErrObjectNotExist {
		return retrieveUSDPriceFromGeckoApi(ctx, date, chainID, currencyAddress, client)
	} else if err != nil {
		return 0, err
	}

	var value float64
	if err := file.BinaryRead(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func GetUSDPrice(ctx context.Context, chainID, currencyAddress string, date string) (float64, error) {
	priceCacheMut.Lock()
	defer priceCacheMut.Unlock()
	key := mapKey{
		date:            date,
		chainID:         chainID,
		currencyAddress: currencyAddress,
	}
	value, ok := priceCache[key]
	if !ok {
		value, err := retrieveUSDPrice(ctx, date, chainID, currencyAddress)
		if err != nil {
			return 0, err
		}
		priceCache[key] = value
	}
	return value, nil
}
