package providers

import (
	"github.com/forestrie/go-merklelog-provider-testing/mmrtesting"
)

type BuilderFactory func() mmrtesting.LogBuilder
