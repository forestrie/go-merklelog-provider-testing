package mmrtesting

import (
	dttesting "github.com/forestrie/go-merklelog-datatrails/testing"
	"github.com/forestrie/go-merklelog/massifs/storage"
	"github.com/stretchr/testify/require"
)

const (
	// resourceChangedProperty            = "resource_changed"
	// resourceChangeMerkleLogStoredEvent = "assetsv2merklelogeventstored"
	names               = 2
	assetAttributeWords = 4
	eventAttributeWords = 6
	// DefaultCheckpointIssuer = "https://github.com/robinbryce/testing/checkpoint-issuer/default"
)

func NewDataTrailsLeafGenerator(g *TestGenerator) LeafGenerator {
	leafGenerator := LeafGenerator{
		Generator: func(logID storage.LogID, base, i uint64) any {
			return g.DataTrailsGenerateLeafContent(logID, base, i)
		},
		Encoder: func(a any) AddLeafArgs {
			return g.DataTrailsEncodeLeafForAddition(a)
		},
	}
	return leafGenerator
}

func (g *TestGenerator) DataTrailsGenerateLeafContent(logID storage.LogID, base, i uint64) any {

	dtg := dttesting.TestContext{
		T:         g.T,
		Rand:      g.Rand,
		LastTime:  g.LastTime,
		EventRate: g.EventRate,
	}
	return dttesting.DataTrailsGenerateLeafContent(&dtg, logID, base, i)
}

func (g *TestGenerator) DataTrailsEncodeLeafForAddition(a any) AddLeafArgs {
	// ID TIMSTAMP COMMITMENT
	id, err := g.NextID()
	require.NoError(g.T, err)

	args := dttesting.DataTrailsEncodeLeafForAddition(g.T, id, a)
	return AddLeafArgs{
		ID:    args.ID,
		AppID: args.AppID,
		Value: args.Value,
		LogID: args.LogID,
	}
}
