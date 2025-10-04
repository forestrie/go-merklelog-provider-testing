package providers

import (
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
)

type BuilderFactory func() mmrtesting.LogBuilder
