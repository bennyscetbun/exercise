# Prerequisites

To use this tool, you will need:

* A Google Cloud Platform (GCP) project
* A GCP storage bucket
* A CoinGecko API key
* A BigQuery dataset
* A JSON file with credentials to access GCP storage and BigQuery for reading and writing.

# How to use

- Set all the mandatory environment variable.
- Put the csv file in the gcp storage bucket
- run with:

        go run ./cmd/uploader name_of_the_csv_file_in_the_bucket

# Environment Variables

This document describes the environment variables used by `main`.


| Name                             | Usage                         | Description                                                                    |
| -------------------------------- | ----------------------------- | ------------------------------------------------------------------------------ |
| [`BIGQUERY_DATASET_NAME`]        | required                      | name of the bigquery dataset                                                   |
| [`COINGECKO_FORCE_PRICE_UPDATE`] | defaults to `false`           | do not use gcp files for caching, but update them                              |
| [`COINGECKO_GCP_CACHE_PATH`]     | defaults to `coingecko_cache` | path for gcp cache files                                                       |
| [`COINGECKO_KEY`]                | required                      | the coingecko api key                                                          |
| [`COINGECKO_RATE_LIMIT_COUNT`]   | defaults to `60`              | rate limit to COINGECKO_RATE_LIMIT_COUNT request per COINGECKO_RATE_LIMIT_TIME |
| [`COINGECKO_RATE_LIMIT_TIME`]    | defaults to `1m`              | rate limit to COINGECKO_RATE_LIMIT_COUNT request per COINGECKO_RATE_LIMIT_TIME |
| [`GCP_BUCKETNAME`]               | required                      | gcp bucket name                                                                |
| [`GCP_CREDENTIAL_FILENAME`]      | required                      | gcp credential filename                                                        |
| [`GCP_PROJECT_ID`]               | required                      | gcp project id                                                                 |
| [`HIDE_P2P_MARKETPLACE`]         | defaults to `true`            | do not use P2P marketplace transaction                                         |
| [`WORKER_NUM`]                   | defaults to `10`              | number of workers                                                              |

# Big query
## Schema
Schema is code based in `./internal/bigquery`

## Queries

You can get the number of transaction and the volume per day and project with the following query
```
SELECT `date`, project_id, SUM(number_of_transactions), SUM(volume_usd)
FROM `carbide-sensor-436519-q4.youpi_banane.transactions_per_days`
GROUP BY `date`, project_id`
```

