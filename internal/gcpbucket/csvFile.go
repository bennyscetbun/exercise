package gcpbucket

import (
	"context"
	"encoding/csv"

	"cloud.google.com/go/storage"
	"github.com/dogmatiq/ferrite"
)

var bucketName = ferrite.String("GCP_BUCKETNAME", "gcp bucket name").Required()

type CSVFile struct {
	storageReader *storage.Reader
	csvReader     *csv.Reader
}

func (b *CSVFile) Close() error {
	return b.storageReader.Close()
}

func (b *CSVFile) Read() ([]string, error) {
	return b.csvReader.Read()
}

func GetCSVFile(ctx context.Context, bucketFileName string, client *storage.Client) (*CSVFile, error) {
	// Get an object handle
	object := client.Bucket(bucketName.Value()).Object(bucketFileName)

	// Download the object
	reader, err := object.NewReader(ctx)
	if err != nil {
		return nil, err
	}

	return &CSVFile{
		storageReader: reader,
		csvReader:     csv.NewReader(reader),
	}, nil
}
