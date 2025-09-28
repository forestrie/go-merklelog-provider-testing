package providers

import (
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
)

type BuilderFactory func(opts massifs.StorageOptions) mmrtesting.LogBuilder
type ReplicationSourceFactory func(opts massifs.StorageOptions) massifs.ReplicationSource
type VerifyingExtenderFactory func(opts massifs.StorageOptions) massifs.VerifyingExtender
