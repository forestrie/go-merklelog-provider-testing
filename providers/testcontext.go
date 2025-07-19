package providers

import (
	"testing"

	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-testing/mmrtesting"
)

type TestContext interface {
	GetTestCfg() mmrtesting.TestOptions
	GetT() *testing.T
	// GetG() *mmrtesting.TestGenerator
	// StoragePrefix(logID []byte) string
	// DeleteBlobsByPrefix(blobPrefixPath string)
	// GetLog() logger.Logger
	// GetStorer() *azblob.Storer
	// NewStorer() *azblob.Storer
	NewMassifCommitter(opts storage.Options) (storage.MassifCommitter, error)
	NewMassifContextReader(opts storage.Options) (storage.MassifContextReader, error)

	// Generation methods
	PadWithNumberedLeaves(data []byte, first, n int) []byte
	GenerateNumberedLeafBatch(logID storage.LogID, startIndex uint64, count uint64) []mmrtesting.AddLeafArgs
	// GetMassifContext(massifIndex uint
}
