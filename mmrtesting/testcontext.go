package mmrtesting

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/datatrails/go-datatrails-merklelog/mmr"
	"github.com/robinbryce/go-merklelog-azure/datatrails"
	"github.com/stretchr/testify/require"

	// TODO: phase this out

	"github.com/datatrails/go-datatrails-common/logger"
)

const (
	DefaultCheckpointIssuer = "https://github.com/robinbryce/testing/checkpoint-issuer/default"
)

type TestContextFactory interface {
	NewMassifGetter(opts massifs.StorageOptions) (massifs.MassifContextGetter, error)
	NewMassifCommitter(opts massifs.StorageOptions) (*massifs.MassifCommitter[massifs.HeadReplacer], error)
	NewMassifCommitterStore(opts massifs.StorageOptions) (*massifs.MassifCommitter[massifs.CommitterStore], error)
	NewCommitterStore(opts massifs.StorageOptions) (massifs.CommitterStore, error)

	GenerateLeafContent(logID storage.LogID) any
	EncodeLeafForAddition(leaf any) AddLeafArgs
}

// ProviderTestContext is satisfied by storage specific provider implementations
// Doing so allows them to be tested using the common provider tests.
type ProviderTestContext interface {
	TestContextFactory

	GetTestCfg() TestOptions
	GetT() *testing.T

	// Generation methods
	PadWithNumberedLeaves(data []byte, first, n int) []byte

	CommitLeaves(
		ctx context.Context,
		committer *massifs.MassifCommitter[massifs.HeadReplacer],
		count uint64,
	) error

	// EncodeLeafContent(leaf any) []byte
	// GenerateNumberedLeafBatch(logID storage.LogID, startIndex uint64, count uint64) []mmrtesting.AddLeafArgs
}

// MassifSealing provides the minimal storage interface required to implement log sealing for tests
type MassifSealing interface {
	GetStorageOptions() massifs.StorageOptions
	HeadIndex(ctx context.Context, otype storage.ObjectType) (uint32, error)
	GetMassifContext(ctx context.Context, massifIndex uint32) (*massifs.MassifContext, error)
	//GetStart(ctx context.Context, massifIndex uint32) (*massifs.MassifStart, error)
	GetCheckpoint(ctx context.Context, massifIndex uint32) (*massifs.Checkpoint, error)
	Put(ctx context.Context, massifIndex uint32, ty storage.ObjectType, data []byte, failIfExists bool) error
}

type MassifStorageEmulator interface {
	// Init(t *testing.T, cfg *TestOptions)
	DeleteLog(logID storage.LogID)
	DeleteByStoragePrefix(storagePrefixPath string)
}

type TestContext[E MassifStorageEmulator, F TestContextFactory] struct {
	Cfg            *TestOptions
	T              *testing.T
	Emulator       E
	Factory        F
	PathProvider   storage.PathProvider
	PrefixProvider storage.PrefixProvider
	G              TestGenerator
}

func NewTestContext[E MassifStorageEmulator, F TestContextFactory](
	t *testing.T, emulator E, factory F, opts *TestOptions) *TestContext[E, F] {

	c := TestContext[E, F]{
		T:        t,
		Emulator: emulator,
		Factory:  factory,
	}
	c.Init(t, opts)

	return &c
}

func (c *TestContext[E, F]) DeleteLog(logID storage.LogID) {
	if logID == nil {
		return
	}
	prefix, err := c.PrefixProvider.Prefix(logID, storage.ObjectMassifData)
	require.NoError(c.T, err)
	c.Emulator.DeleteByStoragePrefix(prefix)
	prefix, err = c.PrefixProvider.Prefix(logID, storage.ObjectCheckpoint)
	require.NoError(c.T, err)
	c.Emulator.DeleteByStoragePrefix(prefix)
}

func (c *TestContext[E, F]) Init(t *testing.T, opts *TestOptions) {

	opts.EnsureDefaults(t)

	// require.NotNil(t, opts.FSDir, "we must have a FSDir if we are using filesystem storage")

	logger.New(opts.LogLevel)
	c.T = t

	c.G.Init(t, opts)
	c.Cfg = opts
	c.PrefixProvider = c.Cfg.PrefixProvider
	c.PathProvider = c.Cfg.PathProvider
	c.PathProvider.SelectLog(context.Background(), c.Cfg.LogID)

	c.Emulator.DeleteByStoragePrefix(datatrails.StoragePrefixPath(c.Cfg.LogID))

	require.True(t, opts.DisableSigning || opts.Signer != nil, "unless signing is disabled we must have a putter")

	c.Cfg = opts
}

func (tc *TestContext[E, F]) CommitLeaves(
	ctx context.Context,
	committer *massifs.MassifCommitter[massifs.HeadReplacer],
	count uint64,
) error {
	if count <= 0 {
		return nil
	}
	t := tc.T
	mc, err := committer.GetAppendContext(ctx)
	require.NoError(t, err)
	batch := tc.G.GenerateNumberedLeafBatch(tc.Cfg.LogID, 0, count)

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

func (tc *TestContext[E, F]) CreateLog(
	ctx context.Context, logID storage.LogID, massifHeight uint8, massifCount uint32, opts ...massifs.Option,
) error {

	options := tc.Cfg.StorageOptions()

	for _, opt := range opts {
		opt(&options)
	}

	tc.DeleteLog(logID)

	tc.PathProvider.SelectLog(ctx, logID)
	committer, err := tc.Factory.NewMassifCommitterStore(options)
	require.NoError(tc.T, err)

	leavesPerMassif := mmr.HeightIndexLeafCount(uint64(massifHeight) - 1)
	count := leavesPerMassif * uint64(massifCount)

	err = tc.AddLeaves(ctx, tc.Cfg.LeafGenerator, committer, count)
	require.NoError(tc.T, err)
	return nil
}

func (tc *TestContext[E, F]) AddLeavesToLog(
	ctx context.Context,
	logID storage.LogID, massifHeight uint8, count int,
	opts ...massifs.Option,
) error {

	options := tc.Cfg.StorageOptions()

	for _, opt := range opts {
		opt(&options)
	}
	committer, err := tc.Factory.NewMassifCommitterStore(options)
	require.NoError(tc.T, err)

	tc.PathProvider.SelectLog(ctx, logID)
	return tc.AddLeaves(ctx, tc.Cfg.LeafGenerator, committer, uint64(count))
}

func (tc *TestContext[E, F]) AddLeaves(
	ctx context.Context,
	leafGenerator LeafGenerator,
	committer *massifs.MassifCommitter[massifs.CommitterStore],
	count uint64,
) error {
	if count <= 0 {
		return nil
	}
	t := tc.T
	mc, err := committer.GetAppendContext(ctx)
	require.NoError(t, err)
	batch := tc.G.GenerateLeafBatch(0, count, leafGenerator)

	for _, args := range batch {

		_, err = mc.AddHashedLeaf(
			sha256.New(), args.ID, args.LogID, args.AppID, nil, args.Value)
		if errors.Is(err, massifs.ErrMassifFull) {
			err = committer.CommitContext(ctx, mc)
			if err != nil {
				return err
			}
			if !tc.Cfg.DisableSigning {
				_, err = tc.SealHead(ctx, committer.Provider)
				require.NoError(tc.T, err)
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
	if !tc.Cfg.DisableSigning {
		_, err = tc.SealHead(ctx, committer.Provider)
		require.NoError(tc.T, err)
	}

	return nil
}

func (tc *TestContext[E, F]) SealHead(
	ctx context.Context, store MassifSealing) (*massifs.Checkpoint, error) {

	massifIndex, err := store.HeadIndex(ctx, storage.ObjectMassifData)
	require.NoError(tc.T, err)
	mc, err := store.GetMassifContext(ctx, massifIndex)
	require.NoError(tc.T, err)
	err = mc.CreatePeakStackMap()
	require.NoError(tc.T, err)

	var chk *massifs.Checkpoint

	// Note: we seal the head massif, if there are higher seals we ignore them.
	chkIndex, err := store.HeadIndex(ctx, storage.ObjectCheckpoint)
	if err != nil {
		if errors.Is(err, storage.ErrDoesNotExist) || errors.Is(err, storage.ErrLogEmpty) {
			chk = nil
			err = nil
		}
	} else {
		// we  require that the caller is building and signing a log incrementally and so the head seal is always
		// the current massif or the one before it.
		require.GreaterOrEqual(
			tc.T, chkIndex+1, massifIndex,
			"you must build and seal the log incrementally. the seal cannot be more than one massif behind")

		chk, err = store.GetCheckpoint(ctx, chkIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to get checkpoint: %w", err)
		}
	}

	require.NoError(tc.T, err)
	chk, err = tc.SealContext(ctx, store, mc, chk)
	require.NoError(tc.T, err)
	return chk, nil
}

func (tc *TestContext[E, F]) SealContext(ctx context.Context, store MassifSealing, mc *massifs.MassifContext, chk *massifs.Checkpoint) (*massifs.Checkpoint, error) {

	var err error
	var peaksB [][]byte

	mmrSizeNew := mc.RangeCount()

	if false && chk != nil {
		var ok bool
		mmrFromIndex := uint64(0)
		mmrFromIndex = chk.MMRState.MMRSize - 1
		cp, err := mmr.IndexConsistencyProof(mc, mmrFromIndex, mmrSizeNew-1)
		if err != nil {
			require.NoError(tc.T, err)
		}

		// a := massifs.TreeCount(mc.Start.MassifHeight)
		// b := massifs.MassifIndexFromMMRIndex(mc.Start.MassifHeight, mmrFromIndex)
		// c := massifs.MassifIndexFromMMRIndex(mc.Start.MassifHeight, mmrSizeNew-1)
		// fmt.Printf("a %d, b %d, c %d\n", a, b, c)

		chkPeaks, err := mmr.PeakHashes(mc, chk.MMRState.MMRSize-1)
		require.NoError(tc.T, err)

		ok, peaksB, err = mmr.CheckConsistency(
			mc, sha256.New(),
			cp.MMRSizeA, cp.MMRSizeB, chkPeaks)

		require.NoError(tc.T, err)
		require.True(tc.T, ok, "consistency check failed: verify failed")
	} else {
		// Just sign the peaks as is. Clearly this is unsafe unless the caller knows what they are doing.
		peaksB, err = mmr.PeakHashes(mc, mmrSizeNew-1)
		require.NoError(tc.T, err)
	}
	lastIDTimestamp := mc.GetLastIDTimestamp()

	state := massifs.MMRState{
		Version:         int(massifs.MMRStateVersionCurrent),
		MMRSize:         mmrSizeNew,
		Peaks:           peaksB,
		Timestamp:       time.Now().UnixMilli(),
		CommitmentEpoch: mc.Start.CommitmentEpoch,
		IDTimestamp:     lastIDTimestamp,
	}
	cborCodec, err := massifs.NewRootSignerCodec()
	require.NoError(tc.T, err)

	rootSigner := massifs.NewRootSigner("https://github.com/robinbryce/veracity", cborCodec)

	data, err := rootSigner.Sign1(
		tc.Cfg.Signer,
		DefaultCheckpointIssuer,
		tc.Cfg.PubKey,
		"test-log-checkpoint",
		state, nil,
	)
	require.NoError(tc.T, err)

	// Write checkpoint directly using Azure blob storage
	// storagePath, err := tc.PathProvider.GetStoragePath(mc.Start.MassifIndex, storage.ObjectCheckpoint)
	// require.NoError(tc.T, err)
	err = store.Put(ctx, mc.Start.MassifIndex, storage.ObjectCheckpoint, data, true)
	require.NoError(tc.T, err)

	// make the checkpoint for the next call to base itself off of

	msg, state2, err := massifs.DecodeSignedRoot(cborCodec, data)
	require.NoError(tc.T, err)
	return &massifs.Checkpoint{
		Sign1Message: *msg,
		MMRState:     state2,
	}, nil
}
