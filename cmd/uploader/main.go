package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bennyscetbun/test_horizon/internal/bigquery"
	"github.com/bennyscetbun/test_horizon/internal/gcpbucket"
	"github.com/bennyscetbun/test_horizon/internal/uploader"
	"github.com/dogmatiq/ferrite"
	"github.com/ztrue/tracerr"
)

var gracefullyQuitTimeout = ferrite.Duration("GRACEFULLY_QUIT_TIMEOUT", "timeout duration for gracefully quitting").WithDefault(10 * time.Second).Required()

func Usage() {
	fmt.Println("Usage: ", os.Args[0], "csv_file_path_in_bucket")
}

func main() {
	ferrite.Init()
	if len(os.Args) < 2 {
		Usage()
		os.Exit(1)
	}
	csvBucketFileName := os.Args[1]

	ctx, ctxCancelFct := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		fmt.Println("Signal catch: gracefully quitting in", gracefullyQuitTimeout.Value())
		ctxCancelFct()
		time.Sleep(gracefullyQuitTimeout.Value())
		fmt.Println("Killing!")
		os.Exit(1)
	}()

	gcpBucketclient, err := gcpbucket.NewClient(ctx)
	if err != nil {
		log.Fatalln(tracerr.Sprint(err))
	}
	defer gcpBucketclient.Close()

	bigqueryClient, err := bigquery.NewClientWithDataset(ctx)
	if err != nil {
		log.Fatalln(tracerr.Sprint(err))
	}
	defer bigqueryClient.Close()

	if err := bigqueryClient.InitTables(ctx); err != nil {
		log.Fatalln(tracerr.Sprint(err))
	}

	csvFile, err := gcpbucket.GetCSVFile(ctx, csvBucketFileName, gcpBucketclient)
	if err != nil {
		log.Fatalln(tracerr.Sprint(err))
	}
	defer csvFile.Close()

	dataMap, err := uploader.PrepareDataFromCSVFile(ctx, *csvFile)
	if err != nil {
		log.Fatalln(tracerr.Sprint(err))
	}
	// context cancelled
	if dataMap == nil {
		return
	}

	if err := bigqueryClient.InsertTransactionsPerDayFromDataMap(ctx, dataMap); err != nil {
		log.Fatalln(tracerr.Sprint(err))
	}
}
