package providers

import (
	"testing"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
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
	NewMassifCommitter(opts massifs.StorageOptions) (massifs.MassifCommitter, error)
	NewMassifContextReader(opts massifs.StorageOptions) (massifs.MassifContextReader, error)

	// Generation methods
	PadWithNumberedLeaves(data []byte, first, n int) []byte
	GenerateNumberedLeafBatch(logID storage.LogID, startIndex uint64, count uint64) []mmrtesting.AddLeafArgs
	// GetMassifContext(massifIndex uint
}

// TestContextV2 is the generic interface for unified provider tests
type TestContextV2[T storage.StorageMetadata] interface {
	GetTestCfg() mmrtesting.TestOptions
	GetT() *testing.T
	NewMassifCommitterV2(opts massifs.StorageOptions) (massifs.MassifCommitterV2[T], error)
	NewMassifContextReader(opts massifs.StorageOptions) (massifs.MassifContextReader, error)

	// Generation methods
	PadWithNumberedLeaves(data []byte, first, n int) []byte
	GenerateNumberedLeafBatch(logID storage.LogID, startIndex uint64, count uint64) []mmrtesting.AddLeafArgs
}
