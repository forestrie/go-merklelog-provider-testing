package mmrtesting

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/stretchr/testify/require"
)

// PadWithLeafEntries pads the given mmr (data) with the given number of leaves (n).
//
//	Each leaf is a hash of a deterministically generated event.
func (c *TestContext[E, F]) PadWithLeafEntries(data []byte, n int) []byte {
	if n == 0 {
		return data
	}

	batch := c.GenerateEventBatch(n)
	for _, ev := range batch {
		leafArgs := c.G.EncodeLeafForAddition(ev)
		data = append(data, leafArgs.Value...)
	}
	return data
}

func (c *TestContext[E, F]) GenerateLeaf(
	base, i uint64) AddLeafArgs {
	content := c.Factory.GenerateLeafContent(c.Cfg.LogID)
	return c.Factory.EncodeLeafForAddition(content)
}

func (c *TestContext[E, F]) GenerateEventBatch(count int) []any {
	events := make([]any, 0, count)
	for range count {
		events = append(events, c.Factory.GenerateLeafContent(c.Cfg.LogID))
	}
	return events
}

type GeneratedLeaves struct {
	Encoded []any
	Args    []AddLeafArgs
	Indices []uint64
}

func (tc *TestContext[E, F]) GenerateTenantLog(leafCount int) GeneratedLeaves {
	// Set the current log ID for the path provider
	tc.PathProvider.SelectLog(context.TODO(), tc.Cfg.LogID)

	c, err := tc.Factory.NewMassifCommitter(
		massifs.StorageOptions{
			LogID:           tc.Cfg.LogID,
			MassifHeight:    tc.Cfg.MassifHeight,
			CommitmentEpoch: tc.Cfg.CommitmentEpoch,
		},
	)

	require.Nil(tc.T, err)
	t := tc.T

	mc, err := c.GetAppendContext(context.Background())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	batch := tc.GenerateEventBatch(leafCount)

	var encoded []any
	var leafArgs []AddLeafArgs
	var indices []uint64
	for _, a := range batch {

		addArgs := tc.Factory.EncodeLeafForAddition(a)
		encoded = append(encoded, a)
		leafArgs = append(leafArgs, addArgs)

		// TODO: decide if we need to support post-encoding decoration per datatrails
		// mmrIndex is equal to the count of all nodes
		indices = append(indices, mc.RangeCount())

		// add the generated event to the mmr
		_, err1 := mc.AddHashedLeaf(
			sha256.New(),
			addArgs.ID, nil, addArgs.LogID, addArgs.AppID, addArgs.Value,
		)
		if err1 != nil {
			if errors.Is(err1, massifs.ErrMassifFull) {
				var err2 error
				err2 = c.CommitContext(context.Background(), mc)
				require.Nil(t, err2)

				// We've filled the current massif. GetAppendContext handles creating new massifs.
				mc, err2 = c.GetAppendContext(context.Background())
				if err2 != nil {
					tc.T.Fatalf("unexpected err: %v", err)
				}

				_, err1 = mc.AddHashedLeaf(
					sha256.New(),
					addArgs.ID, nil, addArgs.LogID, addArgs.AppID, addArgs.Value,
				)

			}

			require.Nil(tc.T, err1)
		}
	}

	err = c.CommitContext(context.Background(), mc)
	require.Nil(t, err)

	return GeneratedLeaves{
		Encoded: encoded,
		Args:    leafArgs,
		Indices: indices,
	}
}
