package datatrails

import (
	dttesting "github.com/forestrie/go-merklelog-datatrails/testing"
	"github.com/forestrie/go-merklelog-provider-testing/mmrtesting"
	"github.com/forestrie/go-merklelog/massifs/storage"
	"github.com/stretchr/testify/require"
)

// NewLeafGenerator returns an mmrtesting.LeafGenerator that produces
// DataTrails-style leaves using the shared TestGenerator.
func NewLeafGenerator(g *mmrtesting.TestGenerator) mmrtesting.LeafGenerator {
	return mmrtesting.LeafGenerator{
		Generator: func(logID storage.LogID, base, i uint64) any {
			return GenerateLeafContent(g, logID, base, i)
		},
		Encoder: func(a any) mmrtesting.AddLeafArgs {
			return EncodeLeafForAddition(g, a)
		},
	}
}

// GenerateLeafContent produces a DataTrails event suitable for use as massif
// leaf content.
func GenerateLeafContent(g *mmrtesting.TestGenerator, logID storage.LogID, base, i uint64) any {
	dtg := dttesting.TestContext{
		T:         g.T,
		Rand:      g.Rand,
		LastTime:  g.LastTime,
		EventRate: g.EventRate,
	}
	return dttesting.DataTrailsGenerateLeafContent(&dtg, logID, base, i)
}

// EncodeLeafForAddition encodes the provided DataTrails event into AddLeafArgs
// using the DataTrails simplehash-based encoding.
func EncodeLeafForAddition(g *mmrtesting.TestGenerator, a any) mmrtesting.AddLeafArgs {
	id, err := g.NextID()
	require.NoError(g.T, err)

	args := dttesting.DataTrailsEncodeLeafForAddition(g.T, id, a)
	return mmrtesting.AddLeafArgs{
		ID:    args.ID,
		AppID: args.AppID,
		Value: args.Value,
		LogID: args.LogID,
	}
}
