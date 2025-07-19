package providers

import (
	"context"
	"crypto/sha256"
	"errors"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/stretchr/testify/require"
)

func CommitLeaves(
	ctx context.Context, tc TestContext,
	committer storage.MassifCommitter,
	count uint64,
) error {
	if count <= 0 {
		return nil
	}
	t := tc.GetT()
	mc, err := committer.GetAppendContext(ctx)
	require.NoError(t, err)
	batch := tc.GenerateNumberedLeafBatch(tc.GetTestCfg().LogID, 0, count)

	for _, args := range batch {

		_, err = mc.AddHashedLeaf(
			sha256.New(), args.ID, args.LogID, args.AppID, nil, args.Value)
		if errors.Is(err, massifs.ErrMassifFull) {
			err = committer.CommitContext(ctx, mc)
			if err != nil {
				return err
			}
			mc, err = committer.GetAppendContext(ctx)
			if err != nil {
				return err
			}

			// Remember to add the leaf we failed to add above
			_, err = mc.AddHashedLeaf(
				sha256.New(), args.ID, args.LogID, args.AppID, nil, args.Value)
			if err != nil {
				return err
			}

			err = nil
		}
		if err != nil {
			return err
		}
	}
	err = committer.CommitContext(ctx, mc)
	if err != nil {
		return err
	}

	return nil
}
