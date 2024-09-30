package gcpbucket

import (
	"context"
	"encoding/binary"
	"io"

	"cloud.google.com/go/storage"
	"github.com/ztrue/tracerr"
)

type File struct {
	storageReader *storage.Reader
}

func (f *File) Close() error {
	return f.storageReader.Close()
}

func (f *File) ReadAll() ([]byte, error) {
	return io.ReadAll(f.storageReader)
}

func (f *File) BinaryRead(data interface{}) error {
	err := binary.Read(f.storageReader, binary.LittleEndian, data)
	if err != nil {
		return tracerr.Wrap(err)
	}
	return err
}

func GetFile(ctx context.Context, objectName string, client *storage.Client) (*File, error) {
	// Get an object handle
	object := client.Bucket(bucketName.Value()).Object(objectName)

	// Download the object
	reader, err := object.NewReader(ctx)
	if err != nil {
		return nil, tracerr.Wrap(err)
	}
	return &File{
		storageReader: reader,
	}, nil
}

func WriteToFile(ctx context.Context, data interface{}, objectName string, client *storage.Client) error {
	object := client.Bucket(bucketName.Value()).Object(objectName)
	wc := object.NewWriter(ctx)
	if err := binary.Write(wc, binary.LittleEndian, data); err != nil {
		wc.Close()
		return tracerr.Wrap(err)
	}
	return wc.Close()
}
